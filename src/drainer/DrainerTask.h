/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef DRAINER_DRAINERTASK_H_
#define DRAINER_DRAINERTASK_H_

#include <folly/executors/task_queue/UnboundedBlockingQueue.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "clients/storage/InternalStorageClient.h"
#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"
#include "drainer/CommonUtils.h"

namespace nebula {
namespace drainer {

// One part and one DrainerSubTask
class DrainerSubTask {
 public:
  DrainerSubTask() = default;

  explicit DrainerSubTask(std::function<nebula::cpp2::ErrorCode()> f, PartitionID part)
      : run_(f), partId_(part) {}

  nebula::cpp2::ErrorCode invoke() { return run_(); }

  // master cluster partId
  PartitionID part() { return partId_; }

 private:
  std::function<nebula::cpp2::ErrorCode()> run_;
  PartitionID partId_;
};

// Receive wal log data sent from the sync listener of the master cluster.
// drainer use tospace spaceId and master cluster partId as directory name.
// This class only processes files related to receiving data.
// The directory structure of the data is as follows:
/* |--data/drainer
 * |----nebula
 * |------toSpaceId
 * |--------cluster_space_id (clusterId_spaceId_parts from master and clusterId from slave)
 * |--------partId1(from master space)
 * |----------wal
 * |----------recv.log(last_log_id_recv)
 * |----------send.log(last_log_id_sent) // when the drainer has sent data, the send.log exists.
 * |--------partId2
 */
// A drainer task processes all parts under the space
class DrainerTask {
  using SubTaskQueue = folly::UnboundedBlockingQueue<DrainerSubTask>;

 public:
  DrainerTask() = default;

  explicit DrainerTask(DrainerEnv* env,
                       GraphSpaceID spaceId,
                       nebula::storage::InternalStorageClient* interClient,
                       std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                       ClusterID masterClusterId,
                       ClusterID slaveClusterId)
      : env_(env),
        spaceId_(spaceId),
        interClient_(interClient),
        ioThreadPool_(ioThreadPool),
        masterClusterId_(masterClusterId),
        slaveClusterId_(slaveClusterId) {}

  virtual ~DrainerTask() { VLOG(3) << "Release Drainer Task"; }

  // Process all parts under the space, one part generates a DrainerSubTask.
  virtual ErrorOr<nebula::cpp2::ErrorCode, std::vector<DrainerSubTask>> genSubTasks();

  // Read the wal file of this part, process it, and then send it through storageclient or meta
  // client. Write send.log file after success.
  // Send in batches to storage client or meta client.
  nebula::cpp2::ErrorCode genSubTask(PartitionID part, nebula::wal::FileBasedWal* wal);

  // Unified interface for processing data.
  StatusOr<std::unordered_map<PartitionID, std::vector<std::string>>> ProcessData(
      PartitionID part, const std::string& log);

  // For storage data
  // Check schema and repart operation. One log, decode once.
  StatusOr<std::unordered_map<PartitionID, std::vector<std::string>>> ProcessStorageData(
      PartitionID part, const std::string& log);

  // For meta data
  // replace spaceId. One log, decode once.
  StatusOr<std::unordered_map<PartitionID, std::vector<std::string>>> ProcessMetaData(
      const std::string& log);

  // For storage data, check tag, edge, index information and repart
  StatusOr<std::pair<PartitionID, std::string>> checkSchemaAndRepartition(
      PartitionID part,
      const folly::StringPiece& rawKey,
      const folly::StringPiece& rawVal,
      bool checkSchema);

  StatusOr<std::pair<PartitionID, std::string>> checkTagSchemaAndRepart(
      PartitionID part,
      const folly::StringPiece& rawKey,
      const folly::StringPiece& rawVal,
      bool checkSchema);

  StatusOr<std::pair<PartitionID, std::string>> vertexRepart(PartitionID part,
                                                             const folly::StringPiece& rawKey);

  StatusOr<std::pair<PartitionID, std::string>> checkEdgeSchemaAndRepart(
      PartitionID part,
      const folly::StringPiece& rawKey,
      const folly::StringPiece& rawVal,
      bool checkSchema);

  StatusOr<std::pair<PartitionID, std::string>> checkIndexAndRepart(
      PartitionID part, const folly::StringPiece& rawKey);

  // Send data to meta client or storage client according to part.
  virtual Status sendData(std::unordered_map<PartitionID, std::vector<std::string>>& logs,
                          PartitionID part);

  // For meta data, perform replacement spaceid.
  StatusOr<std::string> adjustSpaceIdInKey(GraphSpaceID space, const folly::StringPiece& rawKey);
  // update send.log(last_log_id_sent)
  bool updateSendLog(const std::string& sendLogFile,
                     GraphSpaceID spaceId,
                     PartitionID part,
                     LogID lastLogIdSend);

  // read send.log(last_log_id_sent)
  // If send log file not exists, return 0.
  StatusOr<LogID> readSendLog(const std::string& sendLogFile,
                              GraphSpaceID spaceId,
                              PartitionID part);

  // read recv.log(last_log_id_recv)
  StatusOr<LogID> readRecvLog(const std::string& path, GraphSpaceID spaceId, PartitionID part);

  int getSpaceId() { return spaceId_; }

  void finish() { finish(rc_); }

  void finish(nebula::cpp2::ErrorCode rc) {
    if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Drainer task space " << spaceId_ << " succeeded";
    } else {
      LOG(INFO) << "Drainer task space " << spaceId_ << " failed, "
                << apache::thrift::util::enumNameSafe(rc);
    }
    auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;
    rc_.compare_exchange_strong(suc, rc);
  }

  nebula::cpp2::ErrorCode status() const { return rc_; }

  void subTaskFinish(nebula::cpp2::ErrorCode rc, PartitionID partId);

  void cancel() {
    FLOG_INFO("task(%d) cancelled", spaceId_);
    auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;
    rc_.compare_exchange_strong(suc, nebula::cpp2::ErrorCode::E_USER_CANCEL);
  }

 public:
  std::atomic<size_t> unFinishedSubTask_;
  SubTaskQueue subtasks_;

  std::atomic<nebula::cpp2::ErrorCode> rc_{nebula::cpp2::ErrorCode::SUCCEEDED};

 protected:
  DrainerEnv* env_;

  // The spaceId of the slave cluster
  GraphSpaceID spaceId_;

  // The number of subtasks equals to the number of parts in DrainerEnv.readWal_
  size_t subTaskSize_{0};

  // Contain meta listener data, partId is 0
  std::unordered_map<PartitionID, std::shared_ptr<nebula::wal::FileBasedWal>> partWal_;

  nebula::storage::InternalStorageClient* interClient_;

  std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;

  ClusterID masterClusterId_;
  ClusterID slaveClusterId_;

  // The number of this space part of the slave cluster
  int32_t newPartNum_;

  // The number of this space part of the master cluster
  bool repart_ = false;

  size_t vIdLen_;
};

class DrainerTaskFactory {
 public:
  static std::shared_ptr<DrainerTask> createDrainerTask(
      DrainerEnv* env,
      GraphSpaceID space,
      nebula::storage::InternalStorageClient* interClient,
      std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
      ClusterID masterClusterId,
      ClusterID slaveClusterId);
};

}  // namespace drainer
}  // namespace nebula

#endif  // DRAINER_DRAINERTASK_H_
