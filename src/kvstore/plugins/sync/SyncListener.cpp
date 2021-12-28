/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/plugins/sync/SyncListener.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/base/Status.h"
#include "common/fs/FileUtils.h"
#include "common/time/WallClock.h"
#include "common/utils/LogStrListIterator.h"
#include "common/utils/NebulaKeyUtils.h"
#include "common/utils/Utils.h"
#include "kvstore/LogEncoder.h"

DEFINE_int32(request_to_drainer_retry_times, 3, "Retry times if drainer request failed");
DEFINE_int32(send_to_drainer_timeout_ms, 60000, "Rpc timeout for sending to drainer");
DEFINE_uint32(sync_listener_commit_batch_size,
              128,
              "Max batch size when listener commit to drainer");

DECLARE_int32(wal_ttl);
DECLARE_int64(wal_file_size);
DECLARE_int32(wal_buffer_size);
DECLARE_bool(wal_sync);

namespace nebula {
namespace kvstore {

void SyncListener::init() {
  // For the master cluster, it don't need judge whether the drainer is alive or not.
  // For the listener, if the drainer does not provide services,
  // the listener fails to send data to the drainer.
  // meta listener have schemaMan_
  if (!isMetaListener_) {
    auto vRet = schemaMan_->getSpaceVidLen(spaceId_);
    if (!vRet.ok()) {
      LOG(FATAL) << "Get vid length error in space id " << spaceId_;
    }
    vIdLen_ = vRet.value();

    auto vIdType = schemaMan_->getSpaceVidType(spaceId_);
    if (!vIdType.ok()) {
      LOG(FATAL) << "Get vid type error in space id " << spaceId_;
    }
    vIdType_ = vIdType.value();

    auto partNumRet = schemaMan_->getPartsNum(spaceId_);
    if (!partNumRet.ok()) {
      LOG(FATAL) << "Get partNum error in space id " << spaceId_;
    }
    partNum_ = partNumRet.value();

    // Get the specified drainer client of spaceId partId
    auto cRet = serviceMan_->getDrainerClient(spaceId_, partId_);
    if (!cRet.ok()) {
      LOG(FATAL) << "Get sync drainer client error in space id " << spaceId_ << " part id "
                 << partId_;
    }

    auto drainerClientInfo = std::move(cRet.value());
    drainerClient_ = drainerClientInfo.get_host();
    toSpaceName_ = drainerClientInfo.get_space_name();

    auto sRet = schemaMan_->toGraphSpaceName(spaceId_);
    if (!sRet.ok()) {
      LOG(FATAL) << "Get space name  error in space id " << spaceId_;
    }
    spaceName_ = sRet.value();
  }
}

void SyncListener::stop() {
  Listener::stop();
  if (lastAppendLogIdFd_ > 0) {
    close(lastAppendLogIdFd_);
  }
  lastAppendLogIdFd_ = -1;
}

void SyncListener::processLogs() {
  // sync listener is sending data to drainer
  bool expected = false;
  if (!requestOnGoing_.compare_exchange_strong(expected, true)) {
    return;
  }

  // For storage sync listener, in order to maintain the continuity of logId,
  // data contains heartbeat information.
  if (!isMetaListener_) {
    std::unique_ptr<LogIterator> iter;
    LogID currentCommittedLogId;
    TermID currentTerm;
    LogID lastApplyLogId;
    {
      std::lock_guard<std::mutex> guard(raftLock_);
      if (lastApplyLogId_ >= committedLogId_) {
        requestOnGoing_.store(false);
        return;
      }

      currentCommittedLogId = committedLogId_;
      currentTerm = term_;
      lastApplyLogId = lastApplyLogId_;
      // sync listener send wal log to drainer
      iter = wal_->iterator(lastApplyLogId + 1, currentCommittedLogId);
    }

    LogID logIdToSend = -1;
    TermID logTermToSend = -1;

    // Like the wal logs between replicas, the heartbeat is included here
    // for the logId is continuous.
    std::vector<nebula::cpp2::LogEntry> logs;

    // When iter is invalid, maybe reset() and use snapshot
    // Wal log format: LastLogID(int64_t)  LastLogTermID(int64_t) MsgLen(head, int32_t)
    // ClusterID logMsg MsgLen(foot, int32_t)
    if (iter->valid()) {
      VLOG(2) << "Prepare the list of wal log entries to send to drainer";
      // Send data of the same term
      logTermToSend = iter->logTerm();

      for (size_t cnt = 0; iter->valid() && iter->logTerm() == logTermToSend &&
                           cnt < FLAGS_sync_listener_commit_batch_size;
           ++(*iter), ++cnt) {
        // logMsg format in wal log: Timestamp(int64_t）+ LogType(1 char)
        // + sizeof(uint32_t val count)
        nebula::cpp2::LogEntry le;
        le.set_cluster(iter->logSource());
        le.set_log_str(iter->logMsg().toString());
        logs.emplace_back(std::move(le));
        logIdToSend = iter->logId();
      }
    }

    // When a certain amount of data is reached, data is sent to drainer
    if (logs.size() != 0) {
      // apply to state machine
      if (apply(logs, logIdToSend, logTermToSend, lastApplyLogId)) {
        std::lock_guard<std::mutex> guard(raftLock_);
        persist(currentCommittedLogId, currentTerm, lastApplyLogId_);
        VLOG(1) << idStr_ << "Listener succeeded apply log to " << lastApplyLogId_;
        lastApplyTime_ = time::WallClock::fastNowInMilliSec();
        VLOG(1) << folly::sformat(
            "Commit log to drainer : committedLogId={},"
            "committedLogTerm={}, lastApplyLogId={}",
            currentCommittedLogId,
            currentTerm,
            lastApplyLogId_);
      }
    }
  } else {
    // meta sync listener
    // 1) split wal
    std::unique_ptr<LogIterator> iter;
    {
      std::lock_guard<std::mutex> guard(raftLock_);
      if (lastApplyLogId_ >= committedLogId_) {
        requestOnGoing_.store(false);
        return;
      }
      iter = wal_->iterator(lastApplyLogId_ + 1, committedLogId_);
    }

    // Like the wal logs between replicas, the heartbeat is not included here,
    // and the logId is not continuous.

    // When iter is invalid, maybe reset() and use snapshot
    // Wal log format: LastLogID(int64_t)  LastLogTermID(int64_t) MsgLen(head, int32_t)
    // ClusterID logMsg MsgLen(foot, int32_t)

    /*
     * key                                             value
     * tag data
     * __index_EntryType_spaceId__tagName              tagId
     * _tags__spaceId__tagid_version                   length(tagName)+tagName+schema
     *
     * edge data
     * __index__EntryType_spaceId__edgeName            edgeType
     * __edges__spaceId__edgeType_version              length(edgeName)+edgeName+schema
     *
     * index data
     * _index__EntryType_spaceId__indexName            indexID
     * _indexes_spaceId_indexID                        IndexItem
     */

    std::unordered_map<GraphSpaceID, std::vector<nebula::cpp2::LogEntry>> logs;
    LogID logIdToSend = -1;

    VLOG(2) << "Split the list of wal log entries to space directory";
    while (iter->valid()) {
      logIdToSend = iter->logId();
      auto clusterId = iter->logSource();

      // LogMsg format in wal log:
      // Timestamp(int64_t）+ LogType(1 char) + sizeof(uint32_t val count)
      auto log = iter->logMsg();
      // Skip the heartbeat
      if (log.empty()) {
        ++(*iter);
        continue;
      }
      DCHECK_GE(log.size(), sizeof(int64_t) + 1 + sizeof(uint32_t));
      switch (log[sizeof(int64_t)]) {
        case OP_PUT: {
          auto pieces = decodeMultiValues(log);
          DCHECK_EQ(2, pieces.size());
          auto key = pieces[0];
          auto ret = getSpaceIdInKey(key);
          if (!ret.ok()) {
            break;
          }
          auto spaceId = ret.value();
          nebula::cpp2::LogEntry entry;
          entry.set_cluster(clusterId);
          entry.set_log_str(log);
          logs[spaceId].emplace_back(std::move(entry));
          break;
        }
        case OP_MULTI_PUT: {
          auto kvs = decodeMultiValues(log);
          // Make the number of values are an even number
          DCHECK_EQ((kvs.size() + 1) / 2, kvs.size() / 2);
          std::vector<std::pair<std::string, std::string>> retKvs;
          for (size_t i = 0; i < kvs.size(); i += 2) {
            auto key = kvs[i];
            auto ret = getSpaceIdInKey(key);
            if (!ret.ok()) {
              continue;
            }
            auto spaceId = ret.value();
            auto newlog = encodeMultiValues(OP_PUT, std::move(key), kvs[i + 1]);
            nebula::cpp2::LogEntry entry;
            entry.set_cluster(clusterId);
            entry.set_log_str(newlog);
            logs[spaceId].emplace_back(std::move(entry));
          }
          break;
        }
        case OP_REMOVE: {
          auto key = decodeSingleValue(log);
          auto ret = getSpaceIdInKey(key);
          if (!ret.ok()) {
            break;
          }
          auto spaceId = ret.value();
          nebula::cpp2::LogEntry entry;
          entry.set_cluster(clusterId);
          entry.set_log_str(log);
          logs[spaceId].emplace_back(std::move(entry));
          break;
        }
        case OP_MULTI_REMOVE: {
          auto keys = decodeMultiValues(log);
          std::vector<std::string> newKeys;
          for (auto key : keys) {
            auto ret = getSpaceIdInKey(key);
            if (!ret.ok()) {
              continue;
            }
            auto spaceId = ret.value();
            auto newlog = encodeSingleValue(OP_REMOVE, key);
            nebula::cpp2::LogEntry entry;
            entry.set_cluster(clusterId);
            entry.set_log_str(newlog);
            logs[spaceId].emplace_back(std::move(entry));
          }
          break;
        }
        // Currently only used for part cleaning up
        case OP_REMOVE_RANGE: {
          // auto range = kvstore::decodeMultiValues(log);
          // DCHECK_EQ(2, range.size());
          // Can only ignore for now
          LOG(INFO) << "A remove range operation has occurred in the master cluster.";
          break;
        }
        case OP_BATCH_WRITE: {
          auto batchdata = decodeBatchValue(log);
          for (auto& op : batchdata) {
            if (op.first == BatchLogType::OP_BATCH_PUT) {
              auto key = op.second.first;
              auto ret = getSpaceIdInKey(key);
              if (!ret.ok()) {
                continue;
              }
              auto spaceId = ret.value();
              auto newlog = encodeMultiValues(OP_PUT, key, op.second.second);
              nebula::cpp2::LogEntry entry;
              entry.set_cluster(clusterId);
              entry.set_log_str(newlog);
              logs[spaceId].emplace_back(std::move(entry));
            } else if (op.first == BatchLogType::OP_BATCH_REMOVE) {
              auto key = op.second.first;
              auto ret = getSpaceIdInKey(key);
              if (!ret.ok()) {
                continue;
              }
              auto spaceId = ret.value();
              auto newlog = encodeSingleValue(OP_REMOVE, key);
              nebula::cpp2::LogEntry entry;
              entry.set_cluster(clusterId);
              entry.set_log_str(newlog);
              logs[spaceId].emplace_back(std::move(entry));
            } else if (op.first == BatchLogType::OP_BATCH_REMOVE_RANGE) {
              // Can only ignore for now
              LOG(INFO) << "A remove range of batch operation has occurred in the master cluster.";
            }
          }
          break;
        }

        default: {
          LOG(WARNING) << "meta sync listener "
                       << " unknown operation: " << static_cast<int32_t>(log[0]);
        }
      }
      ++(*iter);
    }

    // 2. write all kinds space wals
    if (!logs.empty() && writeSpaceLog(logs)) {
      std::lock_guard<std::mutex> guard(raftLock_);
      lastApplyLogId_ = logIdToSend;

      // Write the last_apply_log_0 file here
      persist(committedLogId_, term_, lastApplyLogId_);
      VLOG(1) << idStr_ << "Listener succeeded apply log to " << lastApplyLogId_;
      lastApplyTime_ = time::WallClock::fastNowInMilliSec();
      VLOG(1) << folly::sformat(
          "Commit log to drainer : committedLogId={},"
          "committedLogTerm={}, lastApplyLogId={}",
          committedLogId_,
          term_,
          lastApplyLogId_);
    }
    // 3. send some meta listener space data
    sendMetaListenerDataToDrainer();
  }
  requestOnGoing_.store(false);
}

void SyncListener::sendMetaListenerDataToDrainer() {
  // Get the latest meta listener information before each sending
  auto host = Utils::getStoreAddrFromRaftAddr(addr_);
  CHECK(!!schemaMan_);
  CHECK(!!serviceMan_);

  auto mLRet = serviceMan_->getMetaListenerInfo(host);
  if (!mLRet.ok()) {
    // Get failed, wait
    VLOG(3) << "Get meta sync listener failed, " << mLRet.status().toString();
    return;
  }

  // vector<fromspaceId, tospace name>
  auto metListenerInfos = std::move(mLRet.value());
  for (auto& syncMetaSpace : metListenerInfos) {
    auto syncSpaceId = syncMetaSpace.first;
    auto toSpaceName = syncMetaSpace.second;
    // Get the specified drainer client of spaceId partId
    auto cRet = serviceMan_->getMetaListenerDrainerClient(syncSpaceId);
    if (!cRet.ok()) {
      // Get failed, wait
      VLOG(3) << "Get meta sync drainer client failed, " << cRet.status().toString();
      return;
    }
    auto drainerClientInfo = std::move(cRet.value());
    auto drainerClient = drainerClientInfo.get_host();
    if (drainerClientInfo.get_space_name() != toSpaceName) {
      LOG(FATAL) << "Get meta sync drainer client illegal";
    }

    auto partIter = partNums_.find(syncSpaceId);
    if (partIter == partNums_.end()) {
      auto partNumRet = schemaMan_->getPartsNum(syncSpaceId);
      if (!partNumRet.ok()) {
        LOG(ERROR) << "Get partNum error from space " << syncSpaceId;
        continue;
      }
      auto partNum = partNumRet.value();
      partNums_.emplace(syncSpaceId, partNum);
    }

    // read space last_apply_log
    auto path = folly::stringPrintf("%s/sync/%d/last_apply_log", walPath_.c_str(), syncSpaceId);
    auto retLogId = spacelLastApplyLogId(path);
    if (!retLogId.ok()) {
      LOG(INFO) << "space " << syncSpaceId << " last_apply_log file read failed.";
      continue;
    }
    auto lastApplyLogId = retLogId.value();
    auto spaceIter = wals_.find(syncSpaceId);

    std::shared_ptr<wal::FileBasedWal> wal;
    if (spaceIter == wals_.end()) {
      VLOG(3) << "space " << syncSpaceId << " wal file pointer not exist.";
      wal::FileBasedWalInfo info;
      info.idStr_ = folly::stringPrintf("[Space: %d, Part: %d] ", 0, 0);
      info.spaceId_ = 0;
      info.partId_ = 0;
      wal::FileBasedWalPolicy policy;
      policy.fileSize = FLAGS_wal_file_size;
      policy.bufferSize = FLAGS_wal_buffer_size;
      policy.sync = FLAGS_wal_sync;

      auto syncWalPath = folly::stringPrintf("%s/sync/%d/wal", walPath_.c_str(), syncSpaceId);
      wals_[syncSpaceId] = wal::FileBasedWal::getWal(
          syncWalPath,
          std::move(info),
          std::move(policy),
          [this](LogID logId, TermID logTermId, ClusterID logClusterId, const std::string& log) {
            return this->preProcessLog(logId, logTermId, logClusterId, log);
          },
          nullptr,
          true);
      wal = wals_[syncSpaceId];
    } else {
      wal = spaceIter->second;
    }
    auto lastLogId = wal->lastLogId();
    if (lastLogId <= lastApplyLogId) {
      VLOG(3) << "Process meta data that can be left, space  " << syncSpaceId;
      continue;
    }
    // sync listener send wal log to drainer
    auto iter = wal->iterator(lastApplyLogId + 1, lastLogId);
    TermID logTermToSend = -1;
    LogID logIdToSend = -1;
    std::vector<nebula::cpp2::LogEntry> logs;
    if (iter->valid()) {
      VLOG(2) << "Prepare the list of wal log entries to send to drainer";
      // Send data of the same term
      logTermToSend = iter->logTerm();

      for (size_t cnt = 0; iter->valid() && iter->logTerm() == logTermToSend &&
                           cnt < FLAGS_sync_listener_commit_batch_size;
           ++(*iter), ++cnt) {
        // Skip the heartbeat
        // logMsg format in wal log: Timestamp(int64_t）+ LogType(1 char)
        // + sizeof(uint32_t val count)
        nebula::cpp2::LogEntry le;
        le.set_cluster(iter->logSource());
        le.set_log_str(iter->logMsg().toString());
        logs.emplace_back(std::move(le));
        logIdToSend = iter->logId();
      }
    }

    // When a certain amount of data is reached, data is sent to drainer
    if (logs.size() != 0) {
      // apply to drainer
      LogID nextApplyLogId = lastApplyLogId;
      if (metaApply(logs,
                    syncSpaceId,
                    logIdToSend,
                    logTermToSend,
                    lastApplyLogId,
                    drainerClient,
                    toSpaceName,
                    nextApplyLogId)) {
        writespacelLastApplyLogId(path, logIdToSend);
      } else {
        // send failed
        if (nextApplyLogId != lastApplyLogId) {
          writespacelLastApplyLogId(path, nextApplyLogId);
        }
      }
    }
  }
}

// Send wal log format data
bool SyncListener::metaApply(const std::vector<nebula::cpp2::LogEntry>& data,
                             GraphSpaceID spaceId,
                             LogID logIdToSend,
                             TermID logTermToSend,
                             LogID lastApplyLogId,
                             HostAddr& drainerClient,
                             std::string tospaceName,
                             LogID& nextApplyLogId) {
  auto retryCnt = FLAGS_request_to_drainer_retry_times;

  while (retryCnt-- > 0) {
    auto f = send(spaceId,
                  0,
                  logIdToSend,
                  logTermToSend,
                  lastApplyLogId,
                  data,
                  drainerClient,
                  tospaceName,
                  false);
    try {
      auto resp = std::move(f).get();
      if (resp.get_error_code() == cpp2::ErrorCode::SUCCEEDED) {
        VLOG(1) << idStr_ << "sync listener has sended to drainer " << drainerClient
                << " succeeded, total logs is " << data.size();
        return true;
      } else {
        LOG(INFO) << idStr_ << "sync listener has sended to drainer " << drainerClient
                  << " failed, The error code is "
                  << apache::thrift::util::enumNameSafe(resp.get_error_code());
        // If there is a gap in the log, need to reset lastApplyLogId_
        if (resp.get_error_code() == nebula::cpp2::ErrorCode::E_LOG_GAP) {
          nextApplyLogId = resp.get_last_log_id();
        }
        return false;
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << idStr_ << "sync listener has sended to drainer " << drainerClient
                 << " failed, exception " << e.what() << ", retry " << retryCnt << " times";
      continue;
    }
  }

  LOG(WARNING) << idStr_ << "sync listener has sended to drainer " << drainerClient << " failed!";
  return false;
}

// lastApplyLogId
nebula::StatusOr<LogID> SyncListener::spacelLastApplyLogId(std::string& path) {
  if (!fs::FileUtils::exist(path)) {
    VLOG(3) << "Non-existent file : " << path;
    return 0;
  }
  int32_t fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(FATAL) << "Failed to open the file \"" << path << "\" (" << errno
               << "): " << strerror(errno);
  }
  // read last applied logId from listener wal file.
  LogID logId;
  CHECK_EQ(pread(fd, &logId, sizeof(LogID), 0), static_cast<ssize_t>(sizeof(LogID)));

  close(fd);
  return logId;
}

bool SyncListener::writespacelLastApplyLogId(std::string& path, LogID lastApplyLogId) {
  int32_t fd = open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    VLOG(3) << "Failed to open file " << path << " (errno: " << errno << "): " << strerror(errno);
    return false;
  }

  std::string val;
  val.reserve(sizeof(LogID) + sizeof(TermID));
  val.append(reinterpret_cast<const char*>(&lastApplyLogId), sizeof(LogID));

  ssize_t written = write(fd, val.c_str(), val.size());
  if (written != (ssize_t)val.size()) {
    VLOG(3) << "Written:" << path << "failed, error:" << strerror(errno);
    close(fd);
    return false;
  }
  close(fd);
  return true;
}

StatusOr<GraphSpaceID> SyncListener::getSpaceIdInKey(const folly::StringPiece& rawKey) {
  auto key = rawKey.toString();
  if (MetaKeyUtils::isIndexTagKey(key) || MetaKeyUtils::isIndexEdgeKey(key) ||
      MetaKeyUtils::isIndexIndexKey(key)) {
    // _index_EntryType_spaceId__tagName
    // __index__EntryType_spaceId__edgeName
    // _index__EntryType_spaceId__indexName
    return MetaKeyUtils::getSpaceIdFromTagEdgeIndexIndexKey(key);
  } else if (MetaKeyUtils::isSchemaTagKey(key)) {
    // _tags__spaceId__tagid_version
    return MetaKeyUtils::parseTagsKeySpaceID(key);
  } else if (MetaKeyUtils::isSchemaEdgeKey(key)) {
    // __edges__spaceId__edgeType_version
    return MetaKeyUtils::parseEdgesKeySpaceID(key);
  } else if (MetaKeyUtils::isIndexKey(key)) {
    // _indexes_spaceId_indexID
    return MetaKeyUtils::parseIndexesKeySpaceID(key);
  }
  return nebula::Status::Error("Not tag/edge/index schema data");
}

std::pair<int64_t, int64_t> SyncListener::commitSnapshot(const std::vector<std::string>& rows,
                                                         LogID committedLogId,
                                                         TermID committedLogTerm,
                                                         bool finished) {
  VLOG(1) << idStr_ << "sync Listener is committing snapshot to drainer.";
  // sync listener is sending data to drainer
  bool expected = false;
  if (!requestOnGoing_.compare_exchange_strong(expected, true)) {
    LOG(ERROR) << idStr_ << "Failed to apply data to drainer while committing snapshot.";
    return std::make_pair(0, 0);
  }

  // storage sync listener
  if (!isMetaListener_) {
    int64_t count = 0;
    int64_t size = 0;
    std::vector<KV> data;
    data.reserve(rows.size());
    for (const auto& row : rows) {
      count++;
      size += row.size();
      auto kv = decodeKV(row);
      data.emplace_back(kv.first, kv.second);
    }
    if (!apply(data)) {
      LOG(ERROR) << idStr_ << "Failed to apply data to draier while committing snapshot.";
      requestOnGoing_.store(false);
      return std::make_pair(0, 0);
    }
    if (finished) {
      CHECK(!raftLock_.try_lock());
      leaderCommitId_ = committedLogId;
      lastApplyLogId_ = committedLogId;
      persist(committedLogId, committedLogTerm, lastApplyLogId_);
      LOG(INFO) << idStr_ << "Sync listener succeeded apply log to drainer " << lastApplyLogId_;
      lastApplyTime_ = time::WallClock::fastNowInMilliSec();
      VLOG(3) << folly::sformat(
          "Commit snapshot to : committedLogId={},"
          "committedLogTerm={}, lastApplyLogId={}",
          committedLogId,
          committedLogTerm,
          lastApplyLogId_);
    }
    requestOnGoing_.store(false);
    return std::make_pair(count, size);
  } else {
    // meta sync listener, Meta listener writes wal files of each space level
    // Even if a snapshot occurs, it is appended
    if (!firstInSnapshot_.load()) {
      firstInSnapshot_.store(true);

      // Delete the corresponding wal directory files of each space
      wals_.clear();
      auto path = folly::stringPrintf("%s/sync", walPath_.c_str());
      fs::FileUtils::remove(path.c_str(), true);
    }

    int64_t count = 0;
    int64_t size = 0;
    std::vector<KV> data;
    data.reserve(rows.size());
    std::unordered_map<GraphSpaceID, std::vector<nebula::cpp2::LogEntry>> logs;
    for (const auto& row : rows) {
      count++;
      size += row.size();
      auto kv = decodeKV(row);

      auto ret = getSpaceIdInKey(kv.first);
      if (!ret.ok()) {
        continue;
      }
      auto spaceId = ret.value();
      auto newlog = encodeMultiValues(OP_PUT, kv.first, kv.second);
      nebula::cpp2::LogEntry entry;
      entry.set_cluster(clusterId_);
      entry.set_log_str(newlog);
      logs[spaceId].emplace_back(std::move(entry));
    }

    // write space failed
    if (!writeSpaceLog(logs)) {
      LOG(ERROR) << idStr_ << "Failed to apply data to draier while committing snapshot.";
      requestOnGoing_.store(false);
      return std::make_pair(0, 0);
    }

    if (finished) {
      CHECK(!raftLock_.try_lock());
      leaderCommitId_ = committedLogId;
      lastApplyLogId_ = committedLogId;
      persist(committedLogId, committedLogTerm, lastApplyLogId_);
      LOG(INFO) << idStr_ << "Sync listener succeeded apply log to drainer " << lastApplyLogId_;
      lastApplyTime_ = time::WallClock::fastNowInMilliSec();
      VLOG(3) << folly::sformat(
          "Commit snapshot to : committedLogId={},"
          "committedLogTerm={}, lastApplyLogId={}",
          committedLogId,
          committedLogTerm,
          lastApplyLogId_);
      firstInSnapshot_.store(false);
      // No data is sent here, and the doApply later will be sent together.
    }
    requestOnGoing_.store(false);
    return std::make_pair(count, size);
  }
}

bool SyncListener::writeSpaceLog(
    std::unordered_map<GraphSpaceID, std::vector<nebula::cpp2::LogEntry>>& logs) {
  // The directory structure of the meta listener is as follows:
  /* |--listenerPath_/spaceId/partId/wal
   * |------walxx
   * |------last_apply_log_0
   * |------sync
   * |--------spaceId
   * |----------wal
   * |------------walxx
   * |----------last_apply_log
   */
  bool allWrited = true;
  for (auto& spaceLog : logs) {
    if (!allWrited) {
      break;
    }
    auto spaceId = spaceLog.first;
    auto entries = spaceLog.second;
    std::shared_ptr<wal::FileBasedWal> wal;
    auto spaceIter = wals_.find(spaceId);
    if (spaceIter != wals_.end()) {
      wal = spaceIter->second;
    }

    if (!wal) {
      wal::FileBasedWalInfo info;
      info.idStr_ = folly::stringPrintf("[Space: %d, Part: %d] ", 0, 0);
      info.spaceId_ = 0;
      info.partId_ = 0;
      wal::FileBasedWalPolicy policy;
      policy.fileSize = FLAGS_wal_file_size;
      policy.bufferSize = FLAGS_wal_buffer_size;
      policy.sync = FLAGS_wal_sync;

      auto path = folly::stringPrintf("%s/sync/%d/wal", walPath_.c_str(), spaceId);
      wals_[spaceId] = wal::FileBasedWal::getWal(
          path,
          std::move(info),
          std::move(policy),
          [this](LogID logId, TermID logTermId, ClusterID logClusterId, const std::string& log) {
            return this->preProcessLog(logId, logTermId, logClusterId, log);
          },
          nullptr,
          true);
      wal = wals_[spaceId];
    }

    size_t numLogs = entries.size();
    auto firstId = wal->lastLogId() + 1;
    // term  is always 0
    auto term = wal->lastLogTerm();
    nebula::LogStrListIterator logIter(firstId, term, entries);
    if (wal->appendLogs(logIter)) {
      if (numLogs != 0) {
        CHECK_EQ(firstId + numLogs - 1, wal->lastLogId()) << "First Id is " << firstId;
      }
    } else {
      allWrited = false;
    }
  }
  return allWrited;
}

// Use this interface when sending snapshots, snapshot batch size is snapshot_batch_size
bool SyncListener::apply(const std::vector<KV>& data) {
  CHECK(!raftLock_.try_lock());
  std::string log = encodeMultiValues(OP_MULTI_PUT, data);

  std::vector<nebula::cpp2::LogEntry> logs;
  nebula::cpp2::LogEntry le;
  le.set_cluster(clusterId_);
  le.set_log_str(std::move(log));
  logs.emplace_back(std::move(le));

  // Only the first time the snapshot sends data, then cleanup data
  bool cleanupData = lastTotalCount_ == 0;
  return apply(logs, 0, 0, 0, cleanupData);
}

// Send wal log format data
bool SyncListener::apply(const std::vector<nebula::cpp2::LogEntry>& data,
                         LogID logIdToSend,
                         TermID logTermToSend,
                         LogID lastApplyLogId,
                         bool cleanupData) {
  auto retryCnt = FLAGS_request_to_drainer_retry_times;

  while (retryCnt-- > 0) {
    auto f = send(spaceId_,
                  partId_,
                  logIdToSend,
                  logTermToSend,
                  lastApplyLogId,
                  data,
                  drainerClient_,
                  toSpaceName_,
                  cleanupData);
    try {
      auto resp = std::move(f).get();
      if (resp.get_error_code() == cpp2::ErrorCode::SUCCEEDED) {
        VLOG(1) << idStr_ << "sync listener has sent to drainer " << drainerClient_
                << " succeeded, total logs is " << data.size();
        bool needToUnlock = raftLock_.try_lock();
        lastApplyLogId_ = resp.get_last_log_id();
        if (needToUnlock) {
          raftLock_.unlock();
        }
        return true;
      } else {
        LOG(INFO) << idStr_ << "sync listener has sent to drainer " << drainerClient_
                  << " failed, The error code is "
                  << apache::thrift::util::enumNameSafe(resp.get_error_code());
        // If there is a gap in the log, need to reset lastApplyLogId_
        if (resp.get_error_code() == nebula::cpp2::ErrorCode::E_LOG_GAP) {
          bool needToUnlock = raftLock_.try_lock();
          lastApplyLogId_ = resp.get_last_log_id();
          if (needToUnlock) {
            raftLock_.unlock();
          }
        }
        return false;
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << idStr_ << "sync listener has sent to drainer " << drainerClient_
                 << " failed, exception " << e.what() << ", the remaining retries " << retryCnt
                 << " times";
      continue;
    }
  }

  LOG(WARNING) << idStr_ << "sync listener has sent to drainer " << drainerClient_ << " failed!";
  return false;
}

folly::Future<nebula::drainer::cpp2::AppendLogResponse> SyncListener::send(
    GraphSpaceID spaceId,
    PartitionID partId,
    LogID lastLogIdToSend,
    TermID lastLogTermToSend,
    LogID lastLogIdSent,
    const std::vector<nebula::cpp2::LogEntry>& data,
    HostAddr& drainerClient,
    std::string& tospaceName,
    bool cleanupData) {
  VLOG(2) << idStr_ << "send append log request to drainer " << drainerClient_ << ", space id "
          << spaceId << ", part id " << partId << ", lastLogIdToSend " << lastLogIdToSend
          << ", lastLogTermToSend " << lastLogTermToSend << ", lastLogIdSent" << lastLogIdSent
          << ", cleanupData " << cleanupData;

  nebula::drainer::cpp2::AppendLogRequest req;
  req.set_clusterId(clusterId_);

  if (partId == 0) {
    req.set_sync_meta(true);
    req.set_space(spaceId);
    auto partIter = partNums_.find(spaceId);
    if (partIter == partNums_.end()) {
      LOG(FATAL) << "Shouldn't be here, space " << spaceId;
    }
    req.set_part_num(partIter->second);
  } else {
    req.set_sync_meta(false);
    req.set_space(spaceId);
    req.set_part_num(partNum_);
    req.set_space_vid_type(vIdType_);
    req.set_space_vid_len(vIdLen_);
  }

  req.set_part(partId);
  req.set_last_log_id_to_send(lastLogIdToSend);
  req.set_last_log_id_sent(lastLogIdSent);
  req.set_log_term(lastLogTermToSend);
  req.set_log_str_list(data);
  req.set_cleanup_data(cleanupData);
  req.set_to_space_name(tospaceName);

  auto* evb = ioThreadPool_->getEventBase();
  return folly::via(evb, [this, evb, drainerClient, req = std::move(req)]() mutable {
    auto client =
        drainerClientMan_->client(drainerClient, evb, false, FLAGS_send_to_drainer_timeout_ms);
    return client->future_appendLog(req);
  });
}

bool SyncListener::persist(LogID committedLogId, TermID currentTerm, LogID lastApplyLogId) {
  if (!writeAppliedId(committedLogId, currentTerm, lastApplyLogId)) {
    LOG(FATAL) << "last apply log id write failed";
  }
  return true;
}

// lastCommittedLogId + lastCommittedTerm + lastApplyLogId
std::pair<LogID, TermID> SyncListener::lastCommittedLogId() {
  if (lastAppendLogIdFd_ < 0) {
    if (!fs::FileUtils::exist(lastApplyLogFile_)) {
      VLOG(3) << "Non-existent file : " << lastApplyLogFile_;
      return {0, 0};
    }
    lastAppendLogIdFd_ = open(lastApplyLogFile_.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
    if (lastAppendLogIdFd_ < 0) {
      LOG(FATAL) << "Failed to open the file " << lastApplyLogFile_ << " (" << errno
                 << "): " << strerror(errno);
    }
  }

  // read last committed logId from log file.
  LogID logId;
  auto ret = pread(lastAppendLogIdFd_, &logId, sizeof(LogID), 0);
  if (ret != static_cast<ssize_t>(sizeof(LogID))) {
    close(lastAppendLogIdFd_);
    lastAppendLogIdFd_ = -1;
    LOG(ERROR) << "Read apply log id file failed";
    return {0, 0};
  }

  // read last committed termId from log file.
  TermID termId;
  ret = pread(lastAppendLogIdFd_, &termId, sizeof(TermID), sizeof(LogID));
  if (ret != static_cast<ssize_t>(sizeof(TermID))) {
    close(lastAppendLogIdFd_);
    lastAppendLogIdFd_ = -1;
    LOG(ERROR) << "Read apply log id file failed";
    return {0, 0};
  }
  return {logId, termId};
}

// lastCommittedLogId + lastCommittedTerm + lastApplyLogId
LogID SyncListener::lastApplyLogId() {
  if (lastAppendLogIdFd_ < 0) {
    if (!fs::FileUtils::exist(lastApplyLogFile_)) {
      VLOG(3) << "Non-existent file : " << lastApplyLogFile_;
      return 0;
    }
    lastAppendLogIdFd_ = open(lastApplyLogFile_.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
    if (lastAppendLogIdFd_ < 0) {
      LOG(FATAL) << "Failed to open the file " << lastApplyLogFile_ << " (" << errno
                 << "): " << strerror(errno);
    }
  }
  // read last applied logId from log  file.
  LogID logId;
  auto offset = sizeof(LogID) + sizeof(TermID);
  auto ret = pread(lastAppendLogIdFd_, &logId, sizeof(LogID), offset);
  if (ret != static_cast<ssize_t>(sizeof(LogID))) {
    close(lastAppendLogIdFd_);
    lastAppendLogIdFd_ = -1;
    LOG(ERROR) << "Read apply log id file failed";
    return 0;
  }
  return logId;
}

bool SyncListener::writeAppliedId(LogID lastId, TermID lastTerm, LogID lastApplyLogId) {
  if (lastAppendLogIdFd_ < 0) {
    lastAppendLogIdFd_ = open(lastApplyLogFile_.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
    if (lastAppendLogIdFd_ < 0) {
      VLOG(3) << idStr_ << "Failed to open file " << lastApplyLogFile_ << " errno(" << errno
              << "): " << strerror(errno);
      return false;
    }
  }

  if (lseek(lastAppendLogIdFd_, 0, SEEK_SET) < 0) {
    VLOG(3) << idStr_ << "Failed to seek the " << lastApplyLogFile_
            << " error: " << strerror(errno);
    close(lastAppendLogIdFd_);
    lastAppendLogIdFd_ = -1;
    return false;
  }

  auto raw = encodeAppliedId(lastId, lastTerm, lastApplyLogId);
  ssize_t written = write(lastAppendLogIdFd_, raw.c_str(), raw.size());
  if (written != (ssize_t)raw.size()) {
    VLOG(3) << idStr_ << "bytesWritten:" << written << ", expected:" << raw.size()
            << ", error:" << strerror(errno);
    close(lastAppendLogIdFd_);
    lastAppendLogIdFd_ = -1;
    return false;
  }
  fsync(lastAppendLogIdFd_);
  return true;
}

std::string SyncListener::encodeAppliedId(LogID lastId,
                                          TermID lastTerm,
                                          LogID lastApplyLogId) const noexcept {
  std::string val;
  val.reserve(sizeof(LogID) * 2 + sizeof(TermID) * 2);
  val.append(reinterpret_cast<const char*>(&lastId), sizeof(LogID))
      .append(reinterpret_cast<const char*>(&lastTerm), sizeof(TermID))
      .append(reinterpret_cast<const char*>(&lastApplyLogId), sizeof(LogID));
  return val;
}

}  // namespace kvstore
}  // namespace nebula
