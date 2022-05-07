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
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/drainer_types.h"
#include "kvstore/Listener.h"
#include "meta/processors/Common.h"

namespace nebula {
namespace kvstore {

using DrainerClient = thrift::ThriftClientManager<drainer::cpp2::DrainerServiceAsyncClient>;

/**
 * @brief This class is the public class of syncListener. According to different partId,
 * it is divided into MetaSyncListener and StorageSyncListener.
 *
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
               std::shared_ptr<raftex::SnapshotManager> snapshotMan,
               std::shared_ptr<RaftClient> clientMan,
               std::shared_ptr<DiskManager> diskMan,
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
    CHECK(!!schemaMan);
    CHECK(!!serviceMan);
    CHECK(!!drainerClientMan_);
    walPath_ = walPath;
    lastApplyLogFile_ = folly::stringPrintf("%s/last_apply_log_%d", walPath.c_str(), partId);
  }

 protected:
  /**
   * @brief Persist some information to the last_apply_log_partId file.
   * Format: committedLogId_currentTerm_lastApplyLogId
   *
   * @param committedLogId
   * @param currentTerm
   * @param lastApplyLogId
   * @return True if write successful.
   */
  bool persist(LogID committedLogId, TermID currentTerm, LogID lastApplyLogId) override;

  /**
   * @brief Get lastCommittedLogId and lastCommittedTerm from last_apply_log_partId file
   *
   * @return std::pair<LogID, TermID> lastCommittedLogId, lastCommittedTerm
   */
  std::pair<LogID, TermID> lastCommittedLogId() override;

  /**
   * @brief Get lastApplyLogId from last_apply_log_partId file
   *
   * @return LogID
   */
  LogID lastApplyLogId() override;

  /**
   * @brief Stop listener
   */
  void stop() override;

  /**
   * @brief Persist some information to the last_apply_log_partId file.
   * Format: committedLogId_currentTerm_lastApplyLogId
   *
   * @param lastId
   * @param lastTerm
   * @param lastApplyLogId
   * @return True if write successful.
   */
  bool writeAppliedId(LogID lastId, TermID lastTerm, LogID lastApplyLogId);

  /**
   * @brief Encode variable to string
   *
   * @param lastId
   * @param lastTerm
   * @param lastApplyLogId
   * @return std::string
   */
  std::string encodeAppliedId(LogID lastId, TermID lastTerm, LogID lastApplyLogId) const noexcept;

 protected:
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

  std::unordered_map<GraphSpaceID, std::shared_ptr<nebula::wal::FileBasedWal>> wals_;

  int32_t lastAppendLogIdFd_ = -1;

  std::atomic<bool> firstInSnapshot_{false};
};

/**
 * @brief Handle meta information synchronization.
 *
 * The directory structure of the meta listener is as follows:
 * |--listenerPath_/spaceId/partId/wal  walPath
 * |------walxx
 * |------last_apply_log_0(committedlogId + committedtermId + lastApplyLogId)
 * |------sync
 * |--------spaceId1
 * |----------wal
 * |------------walxx
 * |----------last_apply_log(lastApplyLogId)
 * |--------spaceId2
 * |----------wal
 * |------------walxx
 * |----------last_apply_log(lastApplyLogId)
 */
class MetaSyncListener : public SyncListener {
 public:
  MetaSyncListener(GraphSpaceID spaceId,
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
      : SyncListener(spaceId,
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
                     serviceMan,
                     drainerClientMan) {}

 protected:
  // Do nothing for MetaSyncListener
  void init() override{};

  /**
   * @brief Process meta info log, and then send data to drainer. The main work is as follows:
   *
   * 1. Read the wal log of meta
   * 2. Divide the meta logs according to space and write them to different space directories.
   * 3. Send multiple synchronized space meta data in sequence.
   *    After the meta data of each space are sent, update the last_apply_log.
   */
  void processLogs() override;

  // Do nothing for MetaSyncListener
  bool apply(const std::vector<KV>&) override {
    return true;
  };

  /**
   * @brief Apply wal log format meta data of one space.
   *
   * @param data
   * @param spaceId
   * @param logIdToSend
   * @param logTermToSend
   * @param lastApplyLogId
   * @param drainerClient
   * @param tospaceName
   * @param nextApplyLogId
   * @return True if apply succesful.
   */
  bool metaApply(const std::vector<nebula::cpp2::LogEntry>& data,
                 GraphSpaceID spaceId,
                 LogID logIdToSend,
                 TermID logTermToSend,
                 LogID lastApplyLogId,
                 HostAddr& drainerClient,
                 std::string tospaceName,
                 LogID& nextApplyLogId);

  /**
   * @brief Send multiple synchronized space meta data in sequence.
   *
   */
  void sendDataToDrainer();

  /**
   * @brief Get LastApplyLogId under the space level
   *
   * @param path File path
   * @return nebula::StatusOr<LogID>
   */
  nebula::StatusOr<LogID> spacelLastApplyLogId(std::string& path);

  // Write the space-level LastApplyLogId to the corresponding file
  bool writespacelLastApplyLogId(std::string& path, LogID lastApplyLogId);

  /**
   * @brief Handling the situation where the meta replica sends a snapshot.
   *
   * @param rows
   * @param committedLogId
   * @param committedLogTerm
   * @param finished
   * @return std::tuple<cpp2::ErrorCode, int64_t, int64_t>
   */
  std::tuple<cpp2::ErrorCode, int64_t, int64_t> commitSnapshot(const std::vector<std::string>& rows,
                                                               LogID committedLogId,
                                                               TermID committedLogTerm,
                                                               bool finished) override;

  /**
   * @brief Send AppendLogRequest to drainer through meta client to synchronize meta data.
   *
   * @param spaceId
   * @param partId
   * @param lastLogIdToSend
   * @param lastLogTermToSend
   * @param lastLogIdSent
   * @param data
   * @param drainerClient
   * @param tospaceName
   * @param cleanupData
   * @return folly::Future<nebula::drainer::cpp2::AppendLogResponse>
   */
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

 private:
  /**
   * @brief Get the Space Id from key.
   * For meta, only the following 3 kinds of data are processed.
   *
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
   *
   * @param rawKey
   * @return StatusOr<GraphSpaceID>
   */
  StatusOr<GraphSpaceID> getSpaceIdInKey(const folly::StringPiece& rawKey);

  /**
   * @brief Write the wal logs of different spaces to the directory under the corresponding space.
   *
   * @param logs
   * @return True if write all logs successful.
   */
  bool writeSpaceLog(std::unordered_map<GraphSpaceID, std::vector<nebula::cpp2::LogEntry>>& logs);

 private:
  std::unordered_map<GraphSpaceID, int32_t> partNums_;
};

/**
 * @brief Handle data information synchronization under one space.
 *
 * The directory structure of the storage listener is as follows:
 * |--listenerPath_/spaceId/partId/wal
 * |------walxx
 * |------last_apply_log_partId(committedlogId + committedtermId + lastApplyLogId)
 */
class StorageSyncListener : public SyncListener {
 public:
  StorageSyncListener(GraphSpaceID spaceId,
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
      : SyncListener(spaceId,
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
                     serviceMan,
                     drainerClientMan) {}

 protected:
  /**
   * @brief initialize some variables
   */
  void init() override;

  // Process logs and then call apply to execute
  /**
   * @brief Process storage info log, and then send data to drainer. The main work is as follows:
   *
   * 1. Read the wal log of storage, include heartbeat info
   * 2. Apply the logs to drainer
   */
  void processLogs() override;

  // Do nothing
  bool apply(const std::vector<KV>&) override {
    return true;
  };

  /**
   * @brief Apply the logs to drainer, when sending snapshots.
   * when sending snapshots, snapshot batch size is snapshot_batch_size.
   *
   * @param data
   * @param finished
   * @param committedLogId
   * @return True if apply succesfully.
   */
  bool apply(const std::vector<KV>& data, bool finished, LogID committedLogId);

  /**
   * @brief Send wal log format data to drainer and handle AppendLogResponse.
   *
   * @param data
   * @param logIdToSend
   * @param logTermToSend
   * @param lastApplyLogId
   * @param cleanupData
   * @param snapshotData
   * @param finished
   * @param committedLogId
   * @return True if apply succesfully.
   */
  bool apply(const std::vector<nebula::cpp2::LogEntry>& data,
             LogID logIdToSend,
             TermID logTermToSend,
             LogID lastApplyLogId,
             bool cleanupData = false,
             bool snapshotData = false,
             bool finished = false,
             LogID committedLogId = 0);

  /**
   * @brief Handling the situation where the storage replica sends a snapshot.
   *
   * @param rows
   * @param committedLogId
   * @param committedLogTerm
   * @param finished
   * @return std::tuple<cpp2::ErrorCode, int64_t, int64_t>
   */
  std::tuple<cpp2::ErrorCode, int64_t, int64_t> commitSnapshot(const std::vector<std::string>& rows,
                                                               LogID committedLogId,
                                                               TermID committedLogTerm,
                                                               bool finished) override;

  /**
   * @brief Send AppendLogRequest to drainer
   *
   * @param spaceId
   * @param partId
   * @param lastLogIdToSend
   * @param lastLogTermToSend
   * @param lastLogIdSent
   * @param data
   * @param drainerClient
   * @param tospaceName
   * @param cleanupData
   * @return folly::Future<nebula::drainer::cpp2::AppendLogResponse>
   */
  folly::Future<nebula::drainer::cpp2::AppendLogResponse> send(
      GraphSpaceID spaceId,
      PartitionID partId,
      LogID lastLogIdToSend,
      TermID lastLogTermToSend,
      LogID lastLogIdSent,
      const std::vector<nebula::cpp2::LogEntry>& data,
      HostAddr& drainerClient,
      std::string& tospaceName,
      bool cleanupData,
      bool snapshotData,
      bool finished,
      LogID committedLogId);
};

}  // namespace kvstore
}  // namespace nebula

#endif  // KVSTORE_PLUGINS_SYNC_SYNCLISTENER_H_
