/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/kv/SyncDataProcessor.h"

#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/LogEncoder.h"

DECLARE_int32(cluster_id);

namespace nebula {
namespace storage {

ProcessorCounters kSyncDataCounters;

void SyncDataProcessor::process(const cpp2::SyncDataRequest& req) {
  auto clusterId = req.get_cluster();
  spaceId_ = req.get_space_id();
  const auto& partLogEntry = req.get_parts();

  if (clusterId != FLAGS_cluster_id) {
    LOG(ERROR) << "Cluster Id does not match, expect clusterId " << FLAGS_cluster_id
               << " actual clusterid " << clusterId;
    for (auto& part : partLogEntry) {
      pushResultCode(nebula::cpp2::ErrorCode::E_WRONGCLUSTER, part.first);
    }
    onFinished();
    return;
  }

  CHECK_NOTNULL(env_->schemaMan_);
  auto retCode = getSpaceVidLen(spaceId_);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "SpaceId " << spaceId_ << " not found";
    for (auto& part : partLogEntry) {
      pushResultCode(retCode, part.first);
    }
    onFinished();
    return;
  }

  CHECK_NOTNULL(env_->kvstore_);
  callingNum_ = partLogEntry.size();

  // Decode wal logMsg, use batch to decode one part log into a log
  for (auto& part : partLogEntry) {
    auto partId = part.first;
    const auto& LogEntryVec = part.second;
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();

    for (auto& log : LogEntryVec) {
      // logMsg format in wal log:
      // Timestamp(int64_t）+ LogType(1 char) + sizeof(uint32_t val count)
      DCHECK_GE(log.size(), sizeof(int64_t) + 1 + sizeof(uint32_t));
      // Skip the timestamp (type of int64_t)
      switch (log[sizeof(int64_t)]) {
        case kvstore::OP_PUT: {
          auto pieces = kvstore::decodeMultiValues(log);
          DCHECK_EQ(2, pieces.size());
          batchHolder->put(pieces[0].toString(), pieces[1].toString());
          break;
        }
        case kvstore::OP_MULTI_PUT: {
          auto kvs = kvstore::decodeMultiValues(log);
          // Make the number of values are an even number
          DCHECK_EQ((kvs.size() + 1) / 2, kvs.size() / 2);
          for (size_t i = 0; i < kvs.size(); i += 2) {
            batchHolder->put(kvs[i].toString(), kvs[i + 1].toString());
          }
          break;
        }
        case kvstore::OP_REMOVE: {
          auto key = kvstore::decodeSingleValue(log);
          batchHolder->remove(key.toString());
          break;
        }
        case kvstore::OP_MULTI_REMOVE: {
          auto keys = kvstore::decodeMultiValues(log);
          for (auto k : keys) {
            batchHolder->remove(k.toString());
          }
          break;
        }
        case kvstore::OP_REMOVE_RANGE: {
          auto range = kvstore::decodeMultiValues(log);
          DCHECK_EQ(2, range.size());
          batchHolder->rangeRemove(range[0].toString(), range[1].toString());
          break;
        }
        case kvstore::OP_BATCH_WRITE: {
          auto data = kvstore::decodeBatchValue(log);
          for (auto& op : data) {
            if (op.first == kvstore::BatchLogType::OP_BATCH_PUT) {
              batchHolder->put(op.second.first.toString(), op.second.second.toString());
            } else if (op.first == kvstore::BatchLogType::OP_BATCH_REMOVE) {
              batchHolder->remove(op.second.first.toString());
            } else if (op.first == kvstore::BatchLogType::OP_BATCH_REMOVE_RANGE) {
              batchHolder->rangeRemove(op.second.first.toString(), op.second.second.toString());
            }
          }
          break;
        }
        default: {
          LOG(ERROR) << "Unknown operation: " << static_cast<int32_t>(log[0]) << " in space "
                     << spaceId_ << " partId " << partId;
          retCode = nebula::cpp2::ErrorCode::E_DATA_ILLEGAL;
        }
      }  // end switch
    }    // end for

    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      handleAsync(spaceId_, partId, retCode);
      continue;
    }
    auto batch = encodeBatchValue(batchHolder->getBatch());
    DCHECK(!batch.empty());
    if (env_->kvstore_->hasVertexCache()) {
      // write need to acquire the lock from read to avoid cache incoherence
      folly::SharedMutex::WriteHolder wHolder(env_->cacheLock_);
      env_->kvstore_->asyncAppendBatch(
          spaceId_, partId, std::move(batch), [partId, this](nebula::cpp2::ErrorCode code) {
            handleAsync(spaceId_, partId, code);
          });
    } else {
      env_->kvstore_->asyncAppendBatch(
          spaceId_, partId, std::move(batch), [partId, this](nebula::cpp2::ErrorCode code) {
            handleAsync(spaceId_, partId, code);
          });
    }
  }
}

}  // namespace storage
}  // namespace nebula
