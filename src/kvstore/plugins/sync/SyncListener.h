/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_PLUGINS_SYNC_SYNCLISTENER_H_
#define KVSTORE_PLUGINS_SYNC_SYNCLISTENER_H_

#include <folly/Function.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>

#include "common/base/Base.h"
#include "common/thrift/ThriftClientManager.h"
#include "interface/gen-cpp2/DrainerServiceAsyncClient.h"
#include "interface/gen-cpp2/drainer_types.h"
#include "kvstore/Listener.h"

namespace nebula {
namespace kvstore {

using DrainerClient = thrift::ThriftClientManager<drainer::cpp2::DrainerServiceAsyncClient>;

class SyncListener : public Listener {
 public:
  SyncListener(GraphSpaceID spaceId,
               PartitionID partId,
               HostAddr localAddr,
               const std::string& walPath,
               std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
               std::shared_ptr<thread::GenericThreadPool> workers,
               std::shared_ptr<folly::Executor> handlers,
               std::shared_ptr<raftex::SnapshotManager> snapshotMan,  // nullptr
               std::shared_ptr<RaftClient> clientMan,                 // nullptr
               std::shared_ptr<DiskManager> diskMan,                  // nullptr
               meta::SchemaManager* schemaMan,
               std::shared_ptr<DrainerClient> drainerClientMan)
      : Listener(spaceId,
                 partId,
                 std::move(localAddr),
                 walPath,
                 ioPool,
                 workers,
                 handlers,
                 snapshotMan,
                 clientMan,
                 diskMan,
                 schemaMan),
        drainerClientMan_(drainerClientMan) {
    CHECK(!!schemaMan);
    CHECK(!!drainerClientMan_);
    lastApplyLogFile_ = std::make_unique<std::string>(
        folly::stringPrintf("%s/last_apply_log_%d", walPath.c_str(), partId));
  }

 protected:
  void init() override;

  bool apply(const std::vector<KV>& data) override;

  std::pair<LogID, TermID> lastCommittedLogId() override;

  LogID lastApplyLogId() override;

  bool persist(LogID, TermID, LogID) override {
    LOG(FATAL) << "Should not reach here";
    return true;
  }

 private:
  // File name, store lastCommittedlogId + lastCommittedtermId + lastApplyLogId + lastApplyLogId
  std::unique_ptr<std::string> lastApplyLogFile_{nullptr};

  std::shared_ptr<DrainerClient> drainerClientMan_;
};

}  // namespace kvstore
}  // namespace nebula

#endif  // KVSTORE_PLUGINS_SYNC_SYNCLISTENER_H_
