/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "drainer/DrainerTask.h"

#include "codec/RowReaderWrapper.h"
#include "common/utils/IndexKeyUtils.h"
#include "common/utils/MetaKeyUtils.h"
#include "common/utils/NebulaKeyUtils.h"
#include "drainer/DrainerFlags.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace drainer {

ErrorOr<nebula::cpp2::ErrorCode, std::vector<DrainerSubTask>> DrainerTask::genSubTasks() {
  auto iter = env_->wals_.find(spaceId_);
  if (iter == env_->wals_.end()) {
    return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
  }

  // Check whether the drainer is legal under the space of the slave cluster
  auto drainersRet = env_->serviceMan_->getDrainerServer(spaceId_);
  if (!drainersRet.ok()) {
    VLOG(2) << folly::stringPrintf("space id %d", spaceId_) << drainersRet.status();
    return nebula::cpp2::ErrorCode::E_DRAINER_NOT_FOUND;
  }

  auto drainers = drainersRet.value();
  auto localHost = env_->localHost_;
  auto drainerIter = std::find_if(drainers.begin(), drainers.end(), [&localHost](const auto& h) {
    return h.get_host() == localHost;
  });
  if (drainerIter == drainers.end()) {
    VLOG(2) << "Drainer host " << localHost << " not in space " << spaceId_;
    return nebula::cpp2::ErrorCode::E_DRAINER_NOT_FOUND;
  }
  if (drainerIter->get_status() != meta::cpp2::HostStatus::ONLINE) {
    VLOG(2) << "Drainer host " << localHost << " status not online";
    return nebula::cpp2::ErrorCode::E_DRAINER_NOT_FOUND;
  }

  partWal_ = iter->second;
  subTaskSize_ = partWal_.size();

  auto retVidLens = env_->schemaMan_->getSpaceVidLen(spaceId_);
  if (!retVidLens.ok()) {
    VLOG(2) << retVidLens.status();
    return nebula::cpp2::ErrorCode::E_INVALID_SPACEVIDLEN;
  }
  vIdLen_ = retVidLens.value();

  auto oldPartIter = env_->spaceOldParts_.find(spaceId_);
  if (oldPartIter == env_->spaceOldParts_.end()) {
    VLOG(2) << "Do not set old partNum in space: " << spaceId_;
    return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
  }
  auto oldPartNum = oldPartIter->second;

  auto newPartIter = env_->spaceNewParts_.find(spaceId_);
  if (newPartIter == env_->spaceNewParts_.end()) {
    VLOG(2) << "Do not set new partNum in space: " << spaceId_;
    return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
  }
  newPartNum_ = newPartIter->second;

  repart_ = oldPartNum != newPartNum_;

  std::vector<DrainerSubTask> subTasks;
  for (const auto& part : partWal_) {
    std::function<nebula::cpp2::ErrorCode()> task =
        std::bind(&DrainerTask::genSubTask, this, part.first, part.second.get());
    subTasks.emplace_back(std::move(task), part.first);
  }
  return subTasks;
}

nebula::cpp2::ErrorCode DrainerTask::genSubTask(PartitionID part, nebula::wal::FileBasedWal* wal) {
  auto sendLogFile =
      folly::stringPrintf("%s/nebula/%d/%d/send.log", env_->drainerPath_.c_str(), spaceId_, part);

  // Only the part directory exists, sendLogFile must exist
  auto ret = readSendLog(sendLogFile, spaceId_, part);
  if (!ret.ok()) {
    VLOG(2) << "Read send log failed " << ret.status();
    return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
  }

  auto lastSendLogId = ret.value();
  std::unique_ptr<LogIterator> iter;
  auto lastId = wal->lastLogId();

  // Note that when reading from wal, do not use the lastLogId of wal,
  // should use the last_log_id_recv written in recvLogFile.
  auto recvLogFile =
      folly::stringPrintf("%s/nebula/%d/%d/recv.log", env_->drainerPath_.c_str(), spaceId_, part);
  // Only the part directory exists, recvLogFile must exist
  ret = readRecvLog(recvLogFile, spaceId_, part);
  if (!ret.ok()) {
    VLOG(2) << "Read recv log failed " << ret.status();
    return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
  }
  auto lastRecvLogId = ret.value();
  if (lastRecvLogId > lastId) {
    VLOG(2) << "Recv logId should not be greater than lastId.";
    return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
  }

  if (lastSendLogId == lastRecvLogId) {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  iter = wal->iterator(lastSendLogId + 1, lastRecvLogId);

  // The logId of the last real data(not include heartbeat)
  LogID logIdToSend = -1;
  // Unlike the replication wal between replicas, the heartbeat will be ignored.
  std::unordered_map<PartitionID, std::vector<std::string>> logs;

  // Wal log format: LastLogID(int64_t)  LastLogTermID(int64_t) MsgLen(head, int32_t)
  // ClusterID logMsg MsgLen(foot, int32_t)
  VLOG(2) << "Prepare the list of log entries to send";
  bool terminate = false;

  // When drainer sends data, no need to consider log term.
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
          VLOG(2) << "Cluster Id does not match, expect clusterId " << masterClusterId_
                  << " actual clusterid " << iter->logSource();
          return nebula::cpp2::ErrorCode::E_WRONGCLUSTER;
        }

        // check schema and maybe repart for storage data.
        // adjust spaceId for meta data.
        // When repartition, one log may generate multiple logs
        auto partAndLogsRet = ProcessData(part, log.toString());
        if (!partAndLogsRet.ok()) {
          VLOG(2) << "Process data failed in space " << spaceId_ << " part " << part;
          terminate = true;
          // logId is continuous
          logIdToSend--;
          break;
        }

        auto partAndLogs = partAndLogsRet.value();
        for (auto& elem : partAndLogs) {
          logs[elem.first].insert(logs[elem.first].end(), elem.second.begin(), elem.second.end());
        }
        break;
      }

      case kvstore::OP_TRANS_LEADER:
      case kvstore::OP_ADD_LEARNER:
      case kvstore::OP_ADD_PEER:
      case kvstore::OP_REMOVE_PEER: {
        break;
      }
      default: {
        VLOG(2) << "Drainer task space " << spaceId_ << " partId " << part
                << " unknown operation: " << static_cast<int32_t>(log[0]);
      }
    }

    if (terminate) {
      break;
    }

    if (logs.size() == FLAGS_drainer_batch_size) {
      auto resp = sendData(logs, part);
      if (resp.ok()) {
        // Persist to the sendLog file
        auto updateSendLogIdRet = updateSendLog(sendLogFile, spaceId_, part, logIdToSend);
        if (!updateSendLogIdRet) {
          VLOG(2) << "Update sendLogId failed.";
          return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
        }
        logs.clear();
      } else {
        VLOG(2) << "Send data failed " << resp.toString();
        return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
      }
    }
    ++(*iter);
  }

  if (!logs.empty()) {
    // When sending data fails, it will keep sending
    auto resp = sendData(logs, part);
    if (resp.ok()) {
      // Persist to the sendLog file
      auto updateSendLogIdRet = updateSendLog(sendLogFile, spaceId_, part, logIdToSend);
      if (!updateSendLogIdRet) {
        VLOG(2) << "Update sendLogId failed.";
        return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
      }
    } else {
      VLOG(2) << "Send data failed " << resp.toString();
      return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
    }
  } else {
    // all data is heartbeat
    if (logIdToSend != -1) {
      auto updateSendLogIdRet = updateSendLog(sendLogFile, spaceId_, part, logIdToSend);
      if (!updateSendLogIdRet) {
        VLOG(2) << "Update sendLogId failed.";
        return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
      }
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

StatusOr<std::unordered_map<PartitionID, std::vector<std::string>>> DrainerTask::ProcessData(
    PartitionID part, const std::string& log) {
  if (part != 0) {
    return ProcessStorageData(part, log);
  } else {
    return ProcessMetaData(log);
  }
}

StatusOr<std::unordered_map<PartitionID, std::vector<std::string>>> DrainerTask::ProcessStorageData(
    PartitionID part, const std::string& log) {
  std::unordered_map<PartitionID, std::vector<std::string>> result;
  // logMsg format in wal log:
  // Timestamp(int64_t）+ LogType(1 char) + sizeof(uint32_t val count)
  DCHECK_GE(log.size(), sizeof(int64_t) + 1 + sizeof(uint32_t));

  // Skip the timestamp (type of int64_t)
  switch (log[sizeof(int64_t)]) {
    case kvstore::OP_PUT: {
      auto pieces = kvstore::decodeMultiValues(log);
      DCHECK_EQ(2, pieces.size());
      auto& key = pieces[0];
      auto& val = pieces[1];
      // 1) check schema and repart
      auto ret = checkSchemaAndRepartition(part, key, val, true);
      if (!ret.ok()) {
        VLOG(2) << "Check schema and repart failed " << ret.status().toString();
        return ret.status();
      }
      auto partAndKey = ret.value();
      std::string newlog = encodeMultiValues(kvstore::OP_PUT, std::move(partAndKey.second), val);
      result[partAndKey.first].emplace_back(std::move(newlog));
      break;
    }
    case kvstore::OP_MULTI_PUT: {
      auto kvs = kvstore::decodeMultiValues(log);
      // Make the number of values are an even number
      DCHECK_EQ((kvs.size() + 1) / 2, kvs.size() / 2);
      std::unordered_map<PartitionID, std::vector<std::pair<std::string, std::string>>> rawKeys;
      for (size_t i = 0; i < kvs.size(); i += 2) {
        auto& key = kvs[i];
        auto& val = kvs[i + 1];
        auto ret = checkSchemaAndRepartition(part, key, val, true);
        if (!ret.ok()) {
          VLOG(2) << "Check schema and repart failed " << ret.status().toString();
          return ret.status();
        }
        auto partAndKey = ret.value();
        rawKeys[partAndKey.first].emplace_back(std::make_pair(partAndKey.second, val.toString()));
      }
      for (auto& keys : rawKeys) {
        auto newlog = encodeMultiValues(kvstore::OP_MULTI_PUT, keys.second);
        result[keys.first].emplace_back(std::move(newlog));
      }
      break;
    }
    case kvstore::OP_REMOVE: {
      auto key = kvstore::decodeSingleValue(log);
      auto ret = checkSchemaAndRepartition(part, key, "", false);
      if (!ret.ok()) {
        VLOG(2) << "Check schema and repart failed " << ret.status().toString();
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
        auto ret = checkSchemaAndRepartition(part, key, "", false);
        if (!ret.ok()) {
          VLOG(2) << "Check schema and repart failed " << ret.status().toString();
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
      // Ignore for now
      LOG(INFO) << "A remove range operation has occurred in the master cluster.";
      break;
    }
    case kvstore::OP_BATCH_WRITE: {
      auto data = kvstore::decodeBatchValue(log);
      std::unordered_map<PartitionID, std::unique_ptr<kvstore::BatchHolder>> batchHolders;
      for (auto& op : data) {
        if (op.first == kvstore::BatchLogType::OP_BATCH_PUT) {
          auto& key = op.second.first;
          auto& val = op.second.second;
          auto ret = checkSchemaAndRepartition(part, key, val, true);
          if (!ret.ok()) {
            VLOG(2) << "Check schema and repart failed " << ret.status().toString();
            return ret.status();
          }
          auto partAndKey = ret.value();
          auto partId = partAndKey.first;
          auto newkey = partAndKey.second;
          auto iter = batchHolders.find(partId);
          if (iter == batchHolders.end()) {
            batchHolders[partId] = std::make_unique<kvstore::BatchHolder>();
          }
          batchHolders[partId]->put(std::move(partAndKey.second), val.toString());
        } else if (op.first == kvstore::BatchLogType::OP_BATCH_REMOVE) {
          auto key = op.second.first;
          auto ret = checkSchemaAndRepartition(part, key, "", false);
          if (!ret.ok()) {
            VLOG(2) << "Check schema and repart failed " << ret.status().toString();
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
          // Ignore for now
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
      VLOG(2) << "Unknown operation: " << static_cast<int32_t>(log[0]) << " in space " << spaceId_
              << " partId " << part;
      return Status::Error("Unknown operation: %d", static_cast<int32_t>(log[0]));
    }
  }
  return result;
}

StatusOr<std::unordered_map<PartitionID, std::vector<std::string>>> DrainerTask::ProcessMetaData(
    const std::string& log) {
  // replace the spaceId of schema data
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
  std::unordered_map<PartitionID, std::vector<std::string>> result;
  // logMsg format in wal log:
  // Timestamp(int64_t）+ LogType(1 char) + sizeof(uint32_t val count)
  DCHECK_GE(log.size(), sizeof(int64_t) + 1 + sizeof(uint32_t));

  // Skip the timestamp (type of int64_t)
  switch (log[sizeof(int64_t)]) {
    case kvstore::OP_PUT: {
      auto pieces = kvstore::decodeMultiValues(log);
      DCHECK_EQ(2, pieces.size());
      auto& key = pieces[0];
      auto ret = adjustSpaceIdInKey(spaceId_, key);
      if (!ret.ok()) {
        VLOG(2) << "Adjust spaceId in key failed " << ret.status().toString();
        return ret.status();
      }
      auto newKey = ret.value();
      std::string newlog = encodeMultiValues(kvstore::OP_PUT, newKey, pieces[1]);
      result[0].emplace_back(std::move(newlog));
      break;
    }
    case kvstore::OP_MULTI_PUT: {
      auto kvs = kvstore::decodeMultiValues(log);
      // Make the number of values are an even number
      DCHECK_EQ((kvs.size() + 1) / 2, kvs.size() / 2);
      std::vector<std::pair<std::string, std::string>> retKvs;
      for (size_t i = 0; i < kvs.size(); i += 2) {
        auto key = kvs[i];
        auto ret = adjustSpaceIdInKey(spaceId_, key);
        if (!ret.ok()) {
          VLOG(2) << "Adjust spaceId in key failed " << ret.status().toString();
          return ret.status();
        }
        auto newKey = ret.value();
        retKvs.emplace_back(std::make_pair(newKey, kvs[i + 1].toString()));
      }
      std::string newlog = encodeMultiValues(kvstore::OP_MULTI_PUT, retKvs);
      result[0].emplace_back(std::move(newlog));
      break;
    }
    case kvstore::OP_REMOVE: {
      auto key = kvstore::decodeSingleValue(log);
      auto ret = adjustSpaceIdInKey(spaceId_, key);
      if (!ret.ok()) {
        VLOG(2) << "Adjust spaceId in key failed " << ret.status().toString();
        return ret.status();
      }
      auto newKey = ret.value();
      std::string newlog = encodeSingleValue(kvstore::OP_REMOVE, newKey);
      result[0].emplace_back(std::move(newlog));
      break;
    }
    case kvstore::OP_MULTI_REMOVE: {
      auto keys = kvstore::decodeMultiValues(log);
      std::vector<std::string> newKeys;
      for (auto key : keys) {
        auto ret = adjustSpaceIdInKey(spaceId_, key);
        if (!ret.ok()) {
          VLOG(2) << "Adjust spaceId in key failed " << ret.status().toString();
          return ret.status();
        }
        auto newKey = ret.value();
        newKeys.emplace_back(newKey);
      }

      auto newlog = encodeMultiValues(kvstore::OP_MULTI_REMOVE, std::move(newKeys));
      result[0].emplace_back(std::move(newlog));
      break;
    }
    // Currently only used for part cleaning up
    case kvstore::OP_REMOVE_RANGE: {
      // auto range = kvstore::decodeMultiValues(log);
      // DCHECK_EQ(2, range.size());
      // Ignore for now
      LOG(INFO) << "A remove range operation has occurred in the master cluster.";
      break;
    }
    case kvstore::OP_BATCH_WRITE: {
      auto batchdata = kvstore::decodeBatchValue(log);
      auto batchHolder = std::make_unique<kvstore::BatchHolder>();
      for (auto& op : batchdata) {
        if (op.first == kvstore::BatchLogType::OP_BATCH_PUT) {
          auto key = op.second.first;
          auto ret = adjustSpaceIdInKey(spaceId_, key);
          if (!ret.ok()) {
            VLOG(2) << "Adjust spaceId in key failed " << ret.status().toString();
            return ret.status();
          }
          auto newKey = ret.value();
          batchHolder->put(std::move(newKey), op.second.second.toString());
        } else if (op.first == kvstore::BatchLogType::OP_BATCH_REMOVE) {
          auto key = op.second.first;
          auto ret = adjustSpaceIdInKey(spaceId_, key);
          if (!ret.ok()) {
            VLOG(2) << "Adjust spaceId in key failed " << ret.status().toString();
            return ret.status();
          }
          auto newKey = ret.value();
          batchHolder->remove(std::move(newKey));
        } else if (op.first == kvstore::BatchLogType::OP_BATCH_REMOVE_RANGE) {
          // Ignore for now
          LOG(INFO) << "A remove range of batch operation has occurred in the master cluster.";
        }
      }

      auto newlog = encodeBatchValue(batchHolder->getBatch());
      result[0].emplace_back(std::move(newlog));
      break;
    }

    default: {
      VLOG(2) << "Drainer task space " << spaceId_ << " partId 0 "
              << " unknown operation: " << static_cast<int32_t>(log[0]);
    }
  }
  return result;
}

StatusOr<std::pair<PartitionID, std::string>> DrainerTask::checkSchemaAndRepartition(
    PartitionID part,
    const folly::StringPiece& rawKey,
    const folly::StringPiece& rawVal,
    bool checkSchema) {
  if (nebula::NebulaKeyUtils::isVertex(vIdLen_, rawKey)) {
    // type(1byte NebulaKeyType::kVertex)_PartID(3bytes)_VertexID(8bytes)
    return vertexRepart(part, rawKey);
  } else if (nebula::NebulaKeyUtils::isTag(vIdLen_, rawKey)) {
    // type(1byte NebulaKeyType::kTag_)_PartID(3bytes)_VertexID(8bytes)_TagID(4bytes)
    return checkTagSchemaAndRepart(part, rawKey, rawVal, checkSchema);
  } else if (nebula::NebulaKeyUtils::isEdge(vIdLen_, rawKey)) {
    // type(1byte)_PartID(3bytes)_VertexID(8bytes)_EdgeType(4bytes)_Rank(8bytes)_otherVertexId(8bytes)_version(1)
    return checkEdgeSchemaAndRepart(part, rawKey, rawVal, checkSchema);
  } else if (nebula::IndexKeyUtils::isIndexKey(rawKey)) {
    // tag index key
    // type(NebulaKeyType::kIndex)_PartitionId  IndexId Index binary  nullableBit  VertexId
    // edge index key
    // type(NebulaKeyType::kIndex)_PartitionId IndexId Index binary nullableBit
    // SrcVertexId EdgeRank DstVertexId
    return checkIndexAndRepart(part, rawKey);
  }
  // Illegal data info
  constexpr int32_t len = static_cast<int32_t>(sizeof(NebulaKeyType));
  auto type = readInt<uint32_t>(rawKey.data(), len) & kTypeMask;
  VLOG(2) << "Data type " << type << " not tag/edge/index data "
          << folly::hexDump(rawKey.data(), rawKey.size());
  return Status::Error("Not tag/edge/index data");
}

StatusOr<std::pair<PartitionID, std::string>> DrainerTask::vertexRepart(
    PartitionID part, const folly::StringPiece& rawKey) {
  if (repart_) {
    auto vertexId = nebula::NebulaKeyUtils::getVertexIdFromVertexKey(vIdLen_, rawKey).toString();
    auto newPartId = env_->metaClient_->partId(newPartNum_, vertexId);
    auto newKey = nebula::NebulaKeyUtils::updatePartIdVertexKey(newPartId, rawKey.toString());
    return std::make_pair(newPartId, std::move(newKey));
  } else {
    return std::make_pair(part, rawKey.toString());
  }
}

StatusOr<std::pair<PartitionID, std::string>> DrainerTask::checkTagSchemaAndRepart(
    PartitionID part,
    const folly::StringPiece& rawKey,
    const folly::StringPiece& rawVal,
    bool checkSchema) {
  // 1) check tag schema version
  if (checkSchema) {
    SchemaVer schemaVer;
    int32_t readerVer;
    RowReaderWrapper::getVersions(rawVal, schemaVer, readerVer);
    if (schemaVer >= 0) {
      auto tagId = nebula::NebulaKeyUtils::getTagId(vIdLen_, rawKey);
      auto schema = env_->schemaMan_->getTagSchema(spaceId_, tagId, schemaVer);
      if (schema == nullptr) {
        VLOG(2) << "The tag schema in the data is too new and needs to be sent next time "
                << " in space " << spaceId_ << " partId " << part;
        return Status::Error(
            "The tag schema in the data is too new and needs to be sent next time"
            " in space %d  partId %d",
            spaceId_,
            part);
      }
    } else {
      VLOG(2) << "Tag schema illegal in space " << spaceId_ << " partId " << part;
      return Status::Error("Tag schema illegal in space %d  partId %d", spaceId_, part);
    }
  }

  // 2) repartition
  if (repart_) {
    auto vertexId = nebula::NebulaKeyUtils::getVertexId(vIdLen_, rawKey).toString();
    auto newPartId = env_->metaClient_->partId(newPartNum_, vertexId);
    auto newKey = nebula::NebulaKeyUtils::updatePartIdTagKey(newPartId, rawKey.toString());
    return std::make_pair(newPartId, std::move(newKey));
  } else {
    return std::make_pair(part, rawKey.toString());
  }
}

StatusOr<std::pair<PartitionID, std::string>> DrainerTask::checkEdgeSchemaAndRepart(
    PartitionID part,
    const folly::StringPiece& rawKey,
    const folly::StringPiece& rawVal,
    bool checkSchema) {
  // 1) check edge schema version
  if (checkSchema) {
    SchemaVer schemaVer;
    int32_t readerVer;
    RowReaderWrapper::getVersions(rawVal, schemaVer, readerVer);
    if (schemaVer >= 0) {
      auto edgeType = std::abs(nebula::NebulaKeyUtils::getEdgeType(vIdLen_, rawKey));
      auto schema = env_->schemaMan_->getEdgeSchema(spaceId_, edgeType, schemaVer);
      if (schema == nullptr) {
        VLOG(2) << "The edge schema in the data is too new and needs to be sent next time "
                << "in space " << spaceId_ << " partId " << part;
        return Status::Error(
            "The edge schema in the data is too new and needs to be sent next time"
            " in space %d  partId %d",
            spaceId_,
            part);
      }
    } else {
      VLOG(2) << "Edge schema illegal in space " << spaceId_ << " partId " << part;
      return Status::Error("Edge schema illegal in space %d  partId %d", spaceId_, part);
    }
  }

  // 2) repartition
  if (repart_) {
    auto srcId = nebula::NebulaKeyUtils::getSrcId(vIdLen_, rawKey).toString();
    auto newPartId = env_->metaClient_->partId(newPartNum_, srcId);
    auto newKey = nebula::NebulaKeyUtils::updatePartIdEdgeKey(newPartId, rawKey.toString());
    return std::make_pair(newPartId, std::move(newKey));
  } else {
    return std::make_pair(part, rawKey.toString());
  }
}

StatusOr<std::pair<PartitionID, std::string>> DrainerTask::checkIndexAndRepart(
    PartitionID part, const folly::StringPiece& rawKey) {
  // 1) check index schema
  auto indexId = nebula::IndexKeyUtils::getIndexId(rawKey);
  auto eRet = env_->indexMan_->getEdgeIndex(spaceId_, indexId);
  std::string VertexId;
  if (eRet.ok()) {
    VertexId = nebula::IndexKeyUtils::getIndexSrcId(vIdLen_, rawKey).toString();
  } else {
    auto tRet = env_->indexMan_->getTagIndex(spaceId_, indexId);
    if (tRet.ok()) {
      VertexId = nebula::IndexKeyUtils::getIndexVertexID(vIdLen_, rawKey).toString();
    } else {
      VLOG(2) << "Index data illegal in space " << spaceId_ << " partId " << part;
      return Status::Error("Index data illegal in space %d  partId %d", spaceId_, part);
    }
  }

  // 2) reparition index data
  if (repart_) {
    auto partId = env_->metaClient_->partId(newPartNum_, VertexId);
    auto newKey = IndexKeyUtils::updatePartIdIndexKey(partId, rawKey.toString());
    return std::make_pair(partId, std::move(newKey));
  } else {
    return std::make_pair(part, rawKey.toString());
  }
}  // namespace drainer

Status DrainerTask::sendData(std::unordered_map<PartitionID, std::vector<std::string>>& logs,
                             PartitionID part) {
  // storage sync listener data from drainer to slave storage
  if (part != 0) {
    // drainer server to storage. When sending data fails, it will retry.
    auto retryCnt = FLAGS_request_to_sync_retry_times;
    while (retryCnt-- > 0) {
      auto retFuture =
          interClient_->syncData(slaveClusterId_, spaceId_, logs)
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
          VLOG(2) << ret.toString() << " retryCnt " << retryCnt;
          if (retryCnt == 0) {
            return Status::Error(
                "Drainer send data to storage failed, space %d part %d", spaceId_, part);
          }
        }
      } catch (const std::exception& e) {
        LOG(ERROR) << "Send drainer data to storage failed exception " << e.what();
        if (retryCnt == 0) {
          return Status::Error(
              "Drainer send data to storage failed, space %d part %d", spaceId_, part);
        }
      }
    }
  } else {
    // meta sync listener data from drainer to slave meta
    // The meta listener sends only the data of the space.
    // drainer server to metaß. When sending data fails, it will retry.
    auto retryCnt = FLAGS_request_to_sync_retry_times;
    while (retryCnt-- > 0) {
      // Meta listener data sent to space 0 of the slave cluster
      auto retFuture =
          env_->metaClient_->syncData(slaveClusterId_, 0, logs[0])
              .via(ioThreadPool_.get())
              .thenValue([spaceId = spaceId_](StatusOr<bool> resp) {
                if (!resp.ok()) {
                  LOG(ERROR) << resp.status();
                  return Status::Error("Drainer send data to meta failed, space %d", spaceId);
                }
                return Status::OK();
              });

      try {
        auto ret = std::move(retFuture).get();
        if (ret.ok()) {
          return Status::OK();
        } else {
          VLOG(2) << ret.toString() << " retryCnt " << retryCnt;
          if (retryCnt == 0) {
            return Status::Error("Drainer send data to meta failed, space %d", spaceId_);
          }
        }
      } catch (const std::exception& e) {
        LOG(ERROR) << "Send drainer data to meta failed exception " << e.what();
        if (retryCnt == 0) {
          return Status::Error("Drainer send data to meta failed, space %d", spaceId_);
        }
      }
    }
  }
  return Status::OK();
}

StatusOr<std::string> DrainerTask::adjustSpaceIdInKey(GraphSpaceID space,
                                                      const folly::StringPiece& rawKey) {
  auto key = rawKey.toString();
  if (MetaKeyUtils::isIndexTagKey(key) || MetaKeyUtils::isIndexEdgeKey(key) ||
      MetaKeyUtils::isIndexIndexKey(key)) {
    // _index_EntryType_spaceId__tagName
    // __index__EntryType_spaceId__edgeName
    // _index__EntryType_spaceId__indexName
    return MetaKeyUtils::replaceTagEdgeIndexIndexKey(space, key);
  } else if (MetaKeyUtils::isSchemaTagKey(key)) {
    // _tags__spaceId__tagid_version
    return MetaKeyUtils::replaceSchemaTagKey(space, key);
  } else if (MetaKeyUtils::isSchemaEdgeKey(key)) {
    // __edges__spaceId__edgeType_version
    return MetaKeyUtils::replaceSchemaEdgeKey(space, key);
  } else if (MetaKeyUtils::isIndexKey(key)) {
    // _indexes_spaceId_indexID
    return MetaKeyUtils::replaceIndexKey(space, key);
  }

  VLOG(2) << "Not tag/edge/index schema data ";
  return Status::Error("Not tag/edge/index schema data");
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
      VLOG(2) << "Failed to open file " << sendLogFile << "errno(" << errno
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
    VLOG(2) << "Failed to read the file " << sendLogFile;
    return false;
  }
  if (lseek(currFd, 0, SEEK_SET) < 0) {
    VLOG(2) << "Failed to seek the send.log, space " << spaceId << " part " << part
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
    VLOG(2) << "BytesWritten:" << written << ", expected:" << val.size()
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
StatusOr<LogID> DrainerTask::readRecvLog(const std::string& recvLogFile,
                                         GraphSpaceID spaceId,
                                         PartitionID part) {
  auto key = std::make_pair(spaceId, part);
  auto recvLogFdIter = env_->recvLogIdFd_.find(key);
  if (recvLogFdIter == env_->recvLogIdFd_.end()) {
    int32_t fd = open(recvLogFile.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_LARGEFILE, 0644);
    if (fd < 0) {
      return Status::Error(
          "Failed to open the file %s, error: %s", recvLogFile.c_str(), strerror(errno));
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
    return Status::Error("Failed to read the file %s", recvLogFile.c_str());
  }
  LogID lastLogIdRecv;
  auto ret = pread(currFd, reinterpret_cast<char*>(&lastLogIdRecv), sizeof(LogID), 0);
  if (ret != static_cast<ssize_t>(sizeof(LogID))) {
    close(currFd);
    env_->recvLogIdFd_.erase(key);
    return Status::Error("Failed to read the file %s.", recvLogFile.c_str());
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
