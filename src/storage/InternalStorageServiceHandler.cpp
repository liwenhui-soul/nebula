/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/InternalStorageServiceHandler.h"

#include "storage/kv/SyncDataProcessor.h"
#include "storage/transaction/ChainAddEdgesProcessorRemote.h"
#include "storage/transaction/ChainUpdateEdgeProcessorRemote.h"

#define RETURN_FUTURE(processor)   \
  auto f = processor->getFuture(); \
  processor->process(req);         \
  return f;

namespace nebula {
namespace storage {

InternalStorageServiceHandler::InternalStorageServiceHandler(StorageEnv* env) : env_(env) {
  kSyncDataCounters.init("sync_data");
}

folly::Future<cpp2::ExecResponse> InternalStorageServiceHandler::future_chainAddEdges(
    const cpp2::ChainAddEdgesRequest& req) {
  auto* processor = ChainAddEdgesProcessorRemote::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::UpdateResponse> InternalStorageServiceHandler::future_chainUpdateEdge(
    const cpp2::ChainUpdateEdgeRequest& req) {
  auto* processor = ChainUpdateEdgeProcessorRemote::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse> InternalStorageServiceHandler::future_syncData(
    const cpp2::SyncDataRequest& req) {
  auto* processor = SyncDataProcessor::instance(env_, &kSyncDataCounters);
  RETURN_FUTURE(processor);
}

}  // namespace storage
}  // namespace nebula
