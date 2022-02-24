/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/kv/SyncDataProcessor.h"

#include "kvstore/LogEncoder.h"

namespace nebula {
namespace meta {

void SyncDataProcessor::process(const cpp2::SyncDataReq& req) {
  auto clusterId = req.get_cluster();
  GraphSpaceID spaceId = req.get_space_id();
  const auto& LogEntrys = req.get_logs();

  if (clusterId != FLAGS_cluster_id) {
    LOG(INFO) << "Cluster Id does not match, expect clusterId " << FLAGS_cluster_id
              << " actual clusterid " << clusterId;
    handleErrorCode(nebula::cpp2::ErrorCode::E_WRONGCLUSTER);
    onFinished();
    return;
  }

  // Sync meta data here (spaceId is 0), so it is not to check whether space 0 exists.
  if (spaceId != 0) {
    LOG(INFO) << "The meta data synchronized here, spaceId should be 0. "
              << "But space in request is" << spaceId;
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
  auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
  std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
  // Drainer has processed the meta listener data.
  for (auto& log : LogEntrys) {
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
        LOG(INFO) << "Unknown operation: " << static_cast<int32_t>(log[0]) << " in meta";
        code = nebula::cpp2::ErrorCode::E_DATA_ILLEGAL;
      }
    }  // end switch
  }    // end for

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }
  auto batch = encodeBatchValue(batchHolder->getBatch());
  DCHECK(!batch.empty());
  LOG(INFO) << "Append batch data";
  doSyncAppendBatchAndUpdate(batch);
}

}  // namespace meta
}  // namespace nebula
