/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "drainer/DrainerTask.h"

#include "common/utils/IndexKeyUtils.h"
#include "common/utils/MetaKeyUtils.h"
#include "common/utils/NebulaKeyUtils.h"
#include "drainer/DrainerFlags.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace drainer {

// Using the round roubin method, each round of traversal uses the same schema of the space
nebula::cpp2::ErrorCode DrainerTask::getSchemas(GraphSpaceID spaceId) {
  UNUSED(spaceId);
  /*
  CHECK_NOTNULL(env_->schemaMan_);
  auto tags = env_->schemaMan_->getAllVerTagSchema(spaceId);
  if (!tags.ok()) {
    return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
  }
  auto edges = env_->schemaMan_->getAllVerEdgeSchema(spaceId);
  if (!edges.ok()) {
    return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
  }
  for (auto tag : tags.value()) {
    auto tagId = tag.first;
    auto tagNameRet = env_->schemaMan_->toTagName(spaceId, tagId);
    if (!tagNameRet.ok()) {
      VLOG(1) << "Can't find spaceId " << spaceId << " tagId " << tagId;
      continue;
    }
    tags_.emplace(tagId, std::move(tagNameRet.value()));
  }
  for (auto edge : edges.value()) {
    auto edgeType = edge.first;
    auto edgeNameRet = env_->schemaMan_->toEdgeName(spaceId, std::abs(edgeType));
    if (!edgeNameRet.ok()) {
      VLOG(1) << "Can't find spaceId " << spaceId << " edgeType " << std::abs(edgeType);
      continue;
    }
    edges_.emplace(edgeType, std::move(edgeNameRet.value()));
  }
  */
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<DrainerSubTask>> DrainerTask::genSubTasks() {
  auto iter = env_->wals_.find(spaceId_);
  if (iter == env_->wals_.end()) {
    return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
  }

  partWal_ = iter->second;
  subTaskSize_ = partWal_.size();

  auto retVidLens = env_->schemaMan_->getSpaceVidLen(spaceId_);
  if (!retVidLens.ok()) {
    LOG(ERROR) << retVidLens.status();
    return nebula::cpp2::ErrorCode::E_INVALID_SPACEVIDLEN;
  }
  vIdLen_ = retVidLens.value();

  // TODO(ps) handle schema change
  auto ret = getSchemas(spaceId_);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Space not found, spaceId: " << spaceId_;
    return ret;
  }

  std::vector<DrainerSubTask> subTasks;
  for (const auto& part : partWal_) {
    std::function<nebula::cpp2::ErrorCode()> task =
        std::bind(&DrainerTask::genSubTask, this, spaceId_, part.first, part.second.get());
    subTasks.emplace_back(std::move(task), part.first);
  }
  return subTasks;
}

// Read the wal file of this part, replace it, and then send it through storageclient or meta
// client. Write send.log file after success.
// Send in batches to storage client or meta client
nebula::cpp2::ErrorCode DrainerTask::genSubTask(GraphSpaceID spaceId,
                                                PartitionID part,
                                                nebula::wal::FileBasedWal* wal) {
  auto sendLogFile =
      folly::stringPrintf("%s/nebula/%d/%d/send.log", env_->drainerPath_.c_str(), spaceId, part);

  // Only the part directory exists, sendLogFile must exist
  auto ret = readSendLog(sendLogFile, spaceId, part);
  if (!ret.ok()) {
    LOG(ERROR) << ret.status();
    return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
  }

  auto lastSendLogId = ret.value();
  std::unique_ptr<LogIterator> iter;
  auto lastId = wal->lastLogId();

  // Note that when reading from wal, do not use the lastLogId of wal,
  // should use the last_log_id_recv written in recvLogFile.
  auto recvLogFile =
      folly::stringPrintf("%s/nebula/%d/%d/recv.log", env_->drainerPath_.c_str(), spaceId, part);
  // Only the part directory exists, sendLogFile must exist
  ret = readRecvLog(recvLogFile, spaceId, part);
  if (!ret.ok()) {
    LOG(ERROR) << ret.status();
    return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
  }
  auto lastRecvLogId = ret.value();
  if (lastRecvLogId > lastId) {
    LOG(ERROR) << "recv log id should not be greater than lastId.";
    return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
  }

  if (lastSendLogId == lastRecvLogId) {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  iter = wal->iterator(lastSendLogId + 1, lastRecvLogId);

  LogID logIdToSend = -1;
  // Unlike the replication wal between replicas, the heartbeat is not included here,
  std::vector<std::string> logs;

  // Wal log format: LastLogID(int64_t)  LastLogTermID(int64_t) MsgLen(head, int32_t)
  // ClusterID logMsg MsgLen(foot, int32_t)
  VLOG(2) << "Prepare the list of log entries to send to storage";

  // 1) The data here is only the tag, edge, and index of the space
  //
  // 2) If it is a meta listener, drainer does not perform decode to replace spaceId,
  //    because when the slave meta receives it, it also performs decode operation.
  //
  // 3) When drainer sends data, no need to consider log term.
  while (iter->valid()) {
    logIdToSend = iter->logId();

    // logMsg format in wal log: Timestamp(int64_t）+ LogType(1 char) + sizeof(uint32_t val count)
    auto log = iter->logMsg();
    // Skip the heartbeat
    if (log.empty()) {
      ++(*iter);
      continue;
    }

    // Send specified data
    DCHECK_GE(log.size(), sizeof(int64_t) + 1 + sizeof(uint32_t));
    switch (log[sizeof(int64_t)]) {
      case kvstore::OP_PUT:
      case kvstore::OP_MULTI_PUT:
      case kvstore::OP_REMOVE:
      case kvstore::OP_REMOVE_RANGE:
      case kvstore::OP_MULTI_REMOVE:
      case kvstore::OP_BATCH_WRITE: {
        // Check whether the clusterId of each log is the master clusterId
        if (iter->logSource() != masterClusterId_) {
          LOG(ERROR) << "Cluster Id does not match, expect clusterId " << masterClusterId_
                     << " actual clusterid " << iter->logSource();
          return nebula::cpp2::ErrorCode::E_WRONGCLUSTER;
        }
        logs.emplace_back(log.toString());
        break;
      }

      case kvstore::OP_TRANS_LEADER:
      case kvstore::OP_ADD_LEARNER:
      case kvstore::OP_ADD_PEER:
      case kvstore::OP_REMOVE_PEER: {
        break;
      }
      default: {
        LOG(WARNING) << "Drainer task space " << spaceId << " partId " << part
                     << " unknown operation: " << static_cast<int32_t>(log[0]);
      }
    }

    if (logs.size() == FLAGS_drainer_batch_size) {
      auto resp = processorAndSend(logs, part);
      if (resp.ok()) {
        // Persist to the sendLog file
        auto updateSendLogIdRet = updateSendLog(sendLogFile, spaceId, part, logIdToSend);
        if (!updateSendLogIdRet) {
          LOG(ERROR) << "Update sendLogId failed.";
          return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
        }
        logs.clear();
      } else {
        LOG(ERROR) << resp.toString();
        return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
      }
    }
    ++(*iter);
  }

  if (logs.size() != 0) {
    // When sending data fails, it will keep sending
    auto resp = processorAndSend(logs, part);
    if (resp.ok()) {
      // Persist to the sendLog file
      auto updateSendLogIdRet = updateSendLog(sendLogFile, spaceId, part, logIdToSend);
      if (!updateSendLogIdRet) {
        LOG(ERROR) << "Update sendLogId failed.";
        return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
      }
    } else {
      LOG(ERROR) << resp.toString();
      return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

StatusOr<std::unordered_map<PartitionID, std::vector<std::string>>> DrainerTask::rePartitionData(
    std::vector<std::string>& logs, PartitionID part) {
  std::unordered_map<PartitionID, std::vector<std::string>> result;

  CHECK(!!env_->metaClient_);
  for (auto& log : logs) {
    // logMsg format in wal log:
    // Timestamp(int64_t）+ LogType(1 char) + sizeof(uint32_t val count)
    DCHECK_GE(log.size(), sizeof(int64_t) + 1 + sizeof(uint32_t));

    // Skip the timestamp (type of int64_t)
    switch (log[sizeof(int64_t)]) {
      case kvstore::OP_PUT: {
        auto pieces = kvstore::decodeMultiValues(log);
        DCHECK_EQ(2, pieces.size());
        auto key = pieces[0];
        auto ret = getPartIdAndNewKey(key);
        if (!ret.ok()) {
          LOG(ERROR) << ret.status().toString();
          return ret.status();
        }
        auto partAndKey = ret.value();
        std::string newlog =
            encodeMultiValues(kvstore::OP_PUT, std::move(partAndKey.second), pieces[1]);
        result[partAndKey.first].emplace_back(std::move(newlog));
        break;
      }
      case kvstore::OP_MULTI_PUT: {
        auto kvs = kvstore::decodeMultiValues(log);
        // Make the number of values are an even number
        DCHECK_EQ((kvs.size() + 1) / 2, kvs.size() / 2);
        std::unordered_map<PartitionID, std::vector<std::pair<std::string, std::string>>> rawKeys;
        for (size_t i = 0; i < kvs.size(); i += 2) {
          auto key = kvs[i];
          auto ret = getPartIdAndNewKey(key);
          if (!ret.ok()) {
            LOG(ERROR) << ret.status().toString();
            return ret.status();
          }
          auto partAndKey = ret.value();
          rawKeys[partAndKey.first].emplace_back(
              std::make_pair(partAndKey.second, kvs[i + 1].toString()));
        }
        for (auto& keys : rawKeys) {
          auto newlog = encodeMultiValues(kvstore::OP_MULTI_PUT, keys.second);
          result[keys.first].emplace_back(std::move(newlog));
        }
        break;
      }
      case kvstore::OP_REMOVE: {
        auto key = kvstore::decodeSingleValue(log);
        auto ret = getPartIdAndNewKey(key);
        if (!ret.ok()) {
          LOG(ERROR) << ret.status().toString();
          return ret.status();
        }
        auto partAndKey = ret.value();
        std::string newlog = encodeSingleValue(kvstore::OP_REMOVE, partAndKey.second);
        result[partAndKey.first].emplace_back(newlog);
        break;
      }
      case kvstore::OP_MULTI_REMOVE: {
        auto keys = kvstore::decodeMultiValues(log);
        std::unordered_map<PartitionID, std::vector<std::string>> rawKeys;
        for (auto key : keys) {
          auto ret = getPartIdAndNewKey(key);
          if (!ret.ok()) {
            LOG(ERROR) << ret.status().toString();
            return ret.status();
          }
          auto partAndKey = ret.value();
          rawKeys[partAndKey.first].emplace_back(partAndKey.second);
        }

        // encode
        // A multiremove is changed to a multi remove of multiple parts
        for (auto& newKey : rawKeys) {
          auto newlog = encodeMultiValues(kvstore::OP_MULTI_REMOVE, newKey.second);
          result[newKey.first].emplace_back(std::move(newlog));
        }
        break;
      }

      // Currently only used for part cleaning up
      case kvstore::OP_REMOVE_RANGE: {
        // auto range = kvstore::decodeMultiValues(log);
        // DCHECK_EQ(2, range.size());
        // Can only ignore for now
        LOG(INFO) << "A remove range operation has occurred in the master cluster.";
        break;
      }
      case kvstore::OP_BATCH_WRITE: {
        auto data = kvstore::decodeBatchValue(log);
        std::unordered_map<PartitionID, std::unique_ptr<kvstore::BatchHolder>> batchHolders;
        for (auto& op : data) {
          if (op.first == kvstore::BatchLogType::OP_BATCH_PUT) {
            auto key = op.second.first;
            auto ret = getPartIdAndNewKey(key);
            if (!ret.ok()) {
              LOG(ERROR) << ret.status().toString();
              return ret.status();
            }
            auto partAndKey = ret.value();
            auto partId = partAndKey.first;
            auto newkey = partAndKey.second;
            auto iter = batchHolders.find(partId);
            if (iter == batchHolders.end()) {
              batchHolders[partId] = std::make_unique<kvstore::BatchHolder>();
            }
            batchHolders[partId]->put(std::move(partAndKey.second), op.second.second.toString());
          } else if (op.first == kvstore::BatchLogType::OP_BATCH_REMOVE) {
            auto key = op.second.first;
            auto ret = getPartIdAndNewKey(key);
            if (!ret.ok()) {
              LOG(ERROR) << ret.status().toString();
              return ret.status();
            }
            auto partAndKey = ret.value();
            auto partId = partAndKey.first;
            auto iter = batchHolders.find(partId);
            if (iter == batchHolders.end()) {
              batchHolders[partId] = std::make_unique<kvstore::BatchHolder>();
            }
            batchHolders[partId]->remove(std::move(partAndKey.second));
          } else if (op.first == kvstore::BatchLogType::OP_BATCH_REMOVE_RANGE) {
            // Can only ignore for now
            LOG(INFO) << "A remove range of batch operation has occurred in the master cluster.";
          }
        }

        for (auto& batchHolder : batchHolders) {
          auto elog = encodeBatchValue(batchHolder.second->getBatch());
          result[batchHolder.first].emplace_back(std::move(elog));
        }
        break;
      }
      default: {
        // It has been filtered before. Shouldn't get here.
        LOG(ERROR) << "Unknown operation: " << static_cast<int32_t>(log[0]) << " in space "
                   << spaceId_ << " partId " << part;
        return Status::Error("Unknown operation: %d", static_cast<int32_t>(log[0]));
      }
    }
  }
  return result;
}

StatusOr<std::pair<PartitionID, std::string>> DrainerTask::getPartIdAndNewKey(
    const folly::StringPiece& rawKey) {
  if (nebula::NebulaKeyUtils::isTag(vIdLen_, rawKey)) {
    // type(1byte NebulaKeyType::kTag_)_PartID(3bytes)_VertexID(8bytes)_TagID(4bytes)
    // TODO check schema
    return getVertexPartIdAndNewKey(rawKey);
  } else if (nebula::NebulaKeyUtils::isEdge(vIdLen_, rawKey)) {
    // type(1byte)_PartID(3bytes)_VertexID(8bytes)_EdgeType(4bytes)_Rank(8bytes)_otherVertexId(8bytes)_version(1)
    return getEdgePartIdAndNewKey(rawKey);
    // TODO check schema
  } else if (nebula::IndexKeyUtils::isIndexKey(rawKey)) {
    // tag index key
    // type(NebulaKeyType::kIndex)_PartitionId  IndexId Index binary  nullableBit  VertexId
    // edge index key
    // type(NebulaKeyType::kIndex)_PartitionId IndexId Index binary nullableBit
    // SrcVertexId EdgeRank DstVertexId
    auto indexId = nebula::IndexKeyUtils::getIndexId(rawKey);
    auto eRet = env_->indexMan_->getEdgeIndex(spaceId_, indexId);
    std::string VertexId;
    if (eRet.ok()) {
      // TODO check schema
      VertexId = nebula::IndexKeyUtils::getIndexSrcId(vIdLen_, rawKey).toString();
    } else {
      auto tRet = env_->indexMan_->getTagIndex(spaceId_, indexId);
      if (tRet.ok()) {
        // TODO check schema
        VertexId = nebula::IndexKeyUtils::getIndexVertexID(vIdLen_, rawKey).toString();
      } else {
        return Status::Error("Wal index data Illegal.");
      }
    }
    auto partId = env_->metaClient_->partId(newPartNum_, VertexId);
    auto newKey = IndexKeyUtils::updatePartIdIndexKey(partId, VertexId);
    return std::make_pair(partId, std::move(newKey));
  }
  LOG(ERROR) << "Not tag/edge/index data " << folly::hexDump(rawKey.data(), rawKey.size());
  return Status::Error("Not tag/edge/index data");
}

std::pair<PartitionID, std::string> DrainerTask::getVertexPartIdAndNewKey(
    const folly::StringPiece& rawKey) {
  auto vertexId = nebula::NebulaKeyUtils::getVertexId(vIdLen_, rawKey).toString();
  auto partId = env_->metaClient_->partId(newPartNum_, vertexId);
  auto newKey = nebula::NebulaKeyUtils::updatePartIdTagKey(partId, vertexId);
  return std::make_pair(partId, std::move(newKey));
}

std::pair<PartitionID, std::string> DrainerTask::getEdgePartIdAndNewKey(
    const folly::StringPiece& rawKey) {
  auto vertexId = nebula::NebulaKeyUtils::getSrcId(vIdLen_, rawKey).toString();
  auto partId = env_->metaClient_->partId(newPartNum_, vertexId);
  auto newKey = nebula::NebulaKeyUtils::updatePartIdEdgeKey(partId, vertexId);
  return std::make_pair(partId, std::move(newKey));
}

Status DrainerTask::processorAndSend(std::vector<std::string> logs, PartitionID part) {
  // storage sync listener data from drainer to slave storage
  if (part != 0) {
    auto oldPartIter = env_->spaceOldParts_.find(spaceId_);
    if (oldPartIter == env_->spaceOldParts_.end()) {
      return Status::Error("Do not set old partNum in space %d", spaceId_);
    }
    auto oldPartNum = oldPartIter->second;

    auto newPartIter = env_->spaceNewParts_.find(spaceId_);
    if (newPartIter == env_->spaceNewParts_.end()) {
      return Status::Error("Do not set new partNum in space %d", spaceId_);
    }
    newPartNum_ = newPartIter->second;

    std::unordered_map<PartitionID, std::vector<std::string>> data;
    // If the number of parts in the space on the master-slave cluster is the same,
    // Do not re-partitioned, at this time, the number of parts is 1.
    if (oldPartNum == newPartNum_) {
      data.emplace(part, std::move(logs));
    } else {
      // re-partitioned, the number of parts is unknown at this time.
      auto retLogStats = rePartitionData(logs, part);
      if (!retLogStats.ok()) {
        return retLogStats.status();
      }
      data = retLogStats.value();
    }

    // drainer server to storage. When sending data fails, it will retry.
    auto retryCnt = FLAGS_request_to_sync_retry_times;
    while (retryCnt-- > 0) {
      auto retFuture =
          interClient_->syncData(slaveClusterId_, spaceId_, data)
              .via(ioThreadPool_.get())
              .thenValue([spaceId = spaceId_,
                          part](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
                if (resp.completeness() != 100) {
                  return Status::Error(
                      "drainer send data to storage failed, space %d part %d", spaceId, part);
                }
                return Status::OK();
              });

      try {
        auto ret = std::move(retFuture).get();
        if (ret.ok()) {
          return Status::OK();
        } else {
          LOG(ERROR) << ret.toString() << " retryCnt " << retryCnt;
          if (retryCnt == 0) {
            return Status::Error(
                "drainer send data to storage failed, space %d part %d", spaceId_, part);
          }
        }
      } catch (const std::exception& e) {
        LOG(ERROR) << "Send drainer data to meta/storage failed exception " << e.what();
        if (retryCnt == 0) {
          return Status::Error(
              "drainer send data to storage failed, space %d part %d", spaceId_, part);
        }
      }
    }
  } else {
    return Status::OK();
  }
  return Status::OK();
}

void DrainerTask::subTaskFinish(nebula::cpp2::ErrorCode rc, PartitionID partId) {
  if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(3) << "Drainer task space " << spaceId_ << " part " << partId << " succeeded.";
  } else {
    VLOG(3) << "Drainer task space " << spaceId_ << " part " << partId << " failed.";
  }
  auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;
  rc_.compare_exchange_strong(suc, rc);
}

bool DrainerTask::updateSendLog(const std::string& sendLogFile,
                                GraphSpaceID spaceId,
                                PartitionID part,
                                LogID lastLogIdSend) {
  auto key = std::make_pair(spaceId, part);
  auto sendLogFdIter = env_->sendLogIdFd_.find(key);
  if (sendLogFdIter == env_->sendLogIdFd_.end()) {
    int32_t fd = open(sendLogFile.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_LARGEFILE, 0644);
    if (fd < 0) {
      LOG(ERROR) << "Failed to open file " << sendLogFile << "errno(" << errno
                 << "): " << strerror(errno);
      return false;
    }
    auto ret = env_->sendLogIdFd_.insert(key, fd).second;
    if (!ret) {
      close(fd);
    }
  }

  int32_t currFd;
  try {
    currFd = env_->sendLogIdFd_.at(key);
  } catch (...) {
    LOG(ERROR) << "Failed to read the file " << sendLogFile;
    return false;
  }
  if (lseek(currFd, 0, SEEK_SET) < 0) {
    LOG(ERROR) << "Failed to seek the send.log, space " << spaceId << " part " << part
               << "error: " << strerror(errno);
    close(currFd);
    env_->sendLogIdFd_.erase(key);
    return false;
  }

  std::string val;
  val.reserve(sizeof(LogID));
  val.append(reinterpret_cast<const char*>(&lastLogIdSend), sizeof(LogID));
  ssize_t written = write(currFd, val.c_str(), val.size());
  if (written != (ssize_t)val.size()) {
    LOG(ERROR) << "bytesWritten:" << written << ", expected:" << val.size()
               << ", error:" << strerror(errno);
    close(currFd);
    env_->sendLogIdFd_.erase(key);
    return false;
  }
  fsync(currFd);
  return true;
}

// send.log(last_log_id_sent)
StatusOr<LogID> DrainerTask::readSendLog(const std::string& sendLogFile,
                                         GraphSpaceID spaceId,
                                         PartitionID part) {
  auto key = std::make_pair(spaceId, part);
  auto sendLogFdIter = env_->sendLogIdFd_.find(key);
  if (sendLogFdIter == env_->sendLogIdFd_.end()) {
    // If send log file not exists, return 0
    if (access(sendLogFile.c_str(), 0) != 0) {
      return 0;
    }

    int32_t fd = open(sendLogFile.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_LARGEFILE, 0644);
    if (fd < 0) {
      return Status::Error(
          "Failed to open the file %s. errno: %s", sendLogFile.c_str(), strerror(errno));
    }
    auto ret = env_->sendLogIdFd_.insert(key, fd).second;
    if (!ret) {
      close(fd);
    }
  }

  LogID lastLogIdSend;
  int32_t currFd;
  try {
    currFd = env_->sendLogIdFd_.at(key);
  } catch (...) {
    return Status::Error("Failed to read the file %s", sendLogFile.c_str());
  }
  auto ret = pread(currFd, reinterpret_cast<char*>(&lastLogIdSend), sizeof(LogID), 0);
  if (ret != static_cast<ssize_t>(sizeof(LogID))) {
    close(currFd);
    env_->sendLogIdFd_.erase(key);
    return Status::Error("Failed to read the file %s.", sendLogFile.c_str());
  }
  return lastLogIdSend;
}

// recv.log(last_log_id_recv)
StatusOr<LogID> DrainerTask::readRecvLog(const std::string& path,
                                         GraphSpaceID spaceId,
                                         PartitionID part) {
  // TODO add lock, because apppend log also open this file.
  auto key = std::make_pair(spaceId, part);
  auto recvLogFdIter = env_->recvLogIdFd_.find(key);
  if (recvLogFdIter == env_->recvLogIdFd_.end()) {
    int32_t fd = open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_LARGEFILE, 0644);
    if (fd < 0) {
      return Status::Error("Failed to open the file %s, error: %s", path.c_str(), strerror(errno));
    }
    auto ret = env_->recvLogIdFd_.insert(key, fd).second;
    if (!ret) {
      close(fd);
    }
  }

  int32_t currFd;
  try {
    currFd = env_->recvLogIdFd_.at(key);
  } catch (...) {
    return Status::Error("Failed to read the file %s", path.c_str());
  }
  LogID lastLogIdRecv;
  auto ret = pread(currFd, reinterpret_cast<char*>(&lastLogIdRecv), sizeof(LogID), 0);
  if (ret != static_cast<ssize_t>(sizeof(LogID))) {
    close(currFd);
    env_->recvLogIdFd_.erase(key);
    return Status::Error("Failed to read the file %s.", path.c_str());
  }
  return lastLogIdRecv;
}

std::shared_ptr<DrainerTask> DrainerTaskFactory::createDrainerTask(
    DrainerEnv* env,
    GraphSpaceID space,
    nebula::storage::InternalStorageClient* interClient,
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
    ClusterID masterClusterId,
    ClusterID slaveClusterId) {
  std::shared_ptr<DrainerTask> ret;
  ret = std::make_shared<DrainerTask>(
      env, space, interClient, ioThreadPool, masterClusterId, slaveClusterId);
  return ret;
}

}  // namespace drainer
}  // namespace nebula
