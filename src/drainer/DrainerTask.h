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

  virtual ~DrainerTask() { LOG(INFO) << "Release Drainer Task"; }

  nebula::cpp2::ErrorCode getSchemas(GraphSpaceID spaceId);

  // Process all parts under the space, one part generates a DrainerSubTask.
  virtual ErrorOr<nebula::cpp2::ErrorCode, std::vector<DrainerSubTask>> genSubTasks();

  nebula::cpp2::ErrorCode genSubTask(GraphSpaceID space,
                                     PartitionID part,
                                     nebula::wal::FileBasedWal* wal);

  // update send.log(last_log_id_sent)
  bool updateSendLog(const std::string& sendLogFile,
                     GraphSpaceID spaceId,
                     PartitionID part,
                     LogID lastLogIdSend);

  // read send.log(last_log_id_sent)
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

  // The drainer server regroups the data according to the partNum of tospace
  virtual Status processorAndSend(std::vector<std::string> logs, PartitionID part);

  StatusOr<std::unordered_map<PartitionID, std::vector<std::string>>> rePartitionData(
      std::vector<std::string>& logs, PartitionID part);

  // Get the new partId, and replace the partId in the key
  StatusOr<std::pair<PartitionID, std::string>> getPartIdAndNewKey(
      const folly::StringPiece& rawKey);

  std::pair<PartitionID, std::string> getVertexPartIdAndNewKey(const folly::StringPiece& rawKey);

  std::pair<PartitionID, std::string> getEdgePartIdAndNewKey(const folly::StringPiece& rawKey);

  // Replace the spaceId of schema data
  StatusOr<std::vector<std::string>> adjustSpaceId(GraphSpaceID space,
                                                   std::vector<std::string> data);

  StatusOr<std::string> adjustSpaceIdInKey(GraphSpaceID space, const folly::StringPiece& rawKey);

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
