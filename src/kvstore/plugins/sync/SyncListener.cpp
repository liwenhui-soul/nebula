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
    auto cRet = schemaMan_->getDrainerClient(spaceId_, partId_);
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
        persist(currentCommittedLogId, currentTerm, lastApplyLogId);
        VLOG(1) << idStr_ << "Listener succeeded apply log to " << lastApplyLogId;
        lastApplyTime_ = time::WallClock::fastNowInMilliSec();
        VLOG(1) << folly::sformat(
            "Commit log to drainer : committedLogId={},"
            "committedLogTerm={}, lastApplyLogId={}",
            currentCommittedLogId,
            currentTerm,
            lastApplyLogId);
      }
    }
    requestOnGoing_.store(false);
  } else {
    // meta sync listener
    // TODO Implement it in the next PR.
  }
  requestOnGoing_.store(false);
  // });
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
    // meta syncn listener, implement it in the next PR.
    return std::make_pair(0, 0);
  }
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

  return apply(logs, 0, 0, 0, true);
}

// Send wal log format data
bool SyncListener::apply(const std::vector<nebula::cpp2::LogEntry>& data,
                         LogID logIdToSend,
                         TermID logTermToSend,
                         LogID lastApplyLogId,
                         bool sendsnapshot) {
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
                  sendsnapshot);
    try {
      auto resp = std::move(f).get();
      if (resp.get_error_code() == cpp2::ErrorCode::SUCCEEDED) {
        VLOG(1) << idStr_ << "sync listener has sent to drainer " << drainerClient_
                << " succeeded, total logs is " << data.size();
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
    bool sendsnapshot) {
  VLOG(2) << idStr_ << "send append log request to drainer " << drainerClient_ << ", space id "
          << spaceId << ", part id " << partId << ", lastLogIdToSend " << lastLogIdToSend
          << ", lastLogTermToSend " << lastLogTermToSend << ", lastLogIdSent" << lastLogIdSent
          << ", send snapshot " << sendsnapshot;

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
  req.set_sending_snapshot(sendsnapshot);
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
