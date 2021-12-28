/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_PLUGINS_SYNC_SYNCLISTENER_H_
#define KVSTORE_PLUGINS_SYNC_SYNCLISTENER_H_

#include <folly/Function.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>

#include "common/base/Base.h"
#include "common/thrift/ThriftClientManager.h"
#include "common/utils/MetaKeyUtils.h"
#include "interface/gen-cpp2/DrainerServiceAsyncClient.h"
#include "interface/gen-cpp2/drainer_types.h"
#include "kvstore/Listener.h"
#include "meta/processors/Common.h"

namespace nebula {
namespace kvstore {

using DrainerClient = thrift::ThriftClientManager<drainer::cpp2::DrainerServiceAsyncClient>;

// The directory structure of the storage listener is as follows:
/* |--listenerPath_/spaceId/partId/wal
 * |------walxx
 * |------last_apply_log_partId(committedlogId + committedtermId + lastApplyLogId)
 */

// The directory structure of the meta listener is as follows:
/* |--listenerPath_/spaceId/partId/wal  walPath
 * |------walxx
 * |------last_apply_log_0(committedlogId + committedtermId + lastApplyLogId)
 * |------sync
 * |--------spaceId1
 * |----------wal
 * |------------walxx
 * |----------last_apply_log
 * |--------spaceId2
 * |----------wal
 * |------------walxx
 * |----------last_apply_log
 */
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
               meta::ServiceManager* serviceMan,
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
                 schemaMan,
                 serviceMan),
        drainerClientMan_(drainerClientMan) {
    // Meta listener not check shemanMan
    if (spaceId == nebula::kDefaultSpaceId && partId == nebula::kDefaultPartId) {
      isMetaListener_ = true;
    }
    CHECK(!!schemaMan);
    CHECK(!!serviceMan);
    CHECK(!!drainerClientMan_);
    walPath_ = walPath;
    lastApplyLogFile_ = folly::stringPrintf("%s/last_apply_log_%d", walPath.c_str(), partId);
  }

 protected:
  void init() override;

  // Process logs and then call apply to execute
  void processLogs() override;

  bool apply(const std::vector<KV>& data) override;

  // send data to drainer and handle AppendLogResponse
  bool apply(const std::vector<nebula::cpp2::LogEntry>& data,
             LogID logIdToSend,
             TermID logTermToSend,
             LogID lastApplyLogId,
             bool cleanupData = false);

  bool metaApply(const std::vector<nebula::cpp2::LogEntry>& data,
                 GraphSpaceID spaceId,
                 LogID logIdToSend,
                 TermID logTermToSend,
                 LogID lastApplyLogId,
                 HostAddr& drainerClient,
                 std::string tospaceName,
                 LogID& nextApplyLogId);

  void sendMetaListenerDataToDrainer();

  nebula::StatusOr<LogID> spacelLastApplyLogId(std::string& path);

  bool writespacelLastApplyLogId(std::string& path, LogID lastApplyLogId);

  std::pair<int64_t, int64_t> commitSnapshot(const std::vector<std::string>& rows,
                                             LogID committedLogId,
                                             TermID committedLogTerm,
                                             bool finished) override;

  bool persist(LogID committedLogId, TermID currentTerm, LogID lastApplyLogId) override;

  std::pair<LogID, TermID> lastCommittedLogId() override;

  LogID lastApplyLogId() override;

  void stop() override;

 private:
  // send AppendLogRequest to drainer
  folly::Future<nebula::drainer::cpp2::AppendLogResponse> send(
      GraphSpaceID spaceId,
      PartitionID partId,
      LogID lastLogIdToSend,
      TermID lastLogTermToSend,
      LogID lastLogIdSent,
      const std::vector<nebula::cpp2::LogEntry>& data,
      HostAddr& drainerClient,
      std::string& tospaceName,
      bool cleanupData);

  bool writeAppliedId(LogID lastId, TermID lastTerm, LogID lastApplyLogId);

  std::string encodeAppliedId(LogID lastId, TermID lastTerm, LogID lastApplyLogId) const noexcept;

  StatusOr<GraphSpaceID> getSpaceIdInKey(const folly::StringPiece& rawKey);

  bool writeSpaceLog(std::unordered_map<GraphSpaceID, std::vector<nebula::cpp2::LogEntry>>& logs);

 private:
  std::string walPath_;

  // File name, store lastCommittedlogId + lastCommittedtermId + lastApplyLogId
  std::string lastApplyLogFile_;

  std::string spaceName_;

  HostAddr drainerClient_;

  std::string toSpaceName_;

  int32_t vIdLen_;

  int32_t partNum_;

  nebula::cpp2::PropertyType vIdType_;

  // Used to identify whether the sync listener is sending data to the drainer
  std::atomic<bool> requestOnGoing_{false};

  std::shared_ptr<DrainerClient> drainerClientMan_;

  bool isMetaListener_{false};

  std::unordered_map<GraphSpaceID, std::shared_ptr<nebula::wal::FileBasedWal>> wals_;

  // use for meta listener
  std::unordered_map<GraphSpaceID, int32_t> partNums_;

  // For storage listener
  int32_t lastAppendLogIdFd_ = -1;

  std::atomic<bool> firstInSnapshot_{false};
};

}  // namespace kvstore
}  // namespace nebula

#endif  // KVSTORE_PLUGINS_SYNC_SYNCLISTENER_H_
