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

/**
 * @brief One part corresponds to one DrainerSubTask
 */
class DrainerSubTask {
 public:
  DrainerSubTask() = default;

  explicit DrainerSubTask(std::function<nebula::cpp2::ErrorCode()> f, PartitionID part)
      : run_(f), partId_(part) {}

  /**
   * @brief Execute DrainerSubTask
   *
   * @return nebula::cpp2::ErrorCodeh The result of executing DrainerSubTask
   */
  nebula::cpp2::ErrorCode invoke() {
    return run_();
  }

  /**
   * @brief Get the partId of drainerSubTask
   *
   * @return PartitionID
   */
  PartitionID part() {
    return partId_;
  }

 private:
  std::function<nebula::cpp2::ErrorCode()> run_;
  // Note: This partId is the partId of the master cluster, not the partId of the slave cluster.
  PartitionID partId_;
};

/**
 * @brief Drainer service uses spaceId of toSpace and partId of master cluster as directory name to
 * receive wal log data from the sync listener of the master cluster. This class only processes
 * files related to send data.
 *
 * The directory structure of the data is as follows:
 * |--data/drainer
 * |----nebula
 * |------toSpaceId
 * |--------cluster_space_id (clusterId_spaceId_parts from master and clusterId from slave)
 * |--------partId1(from master cluster space)
 * |----------wal
 * |----------recv.log(last_log_id_recv)
 * |----------send.log(last_log_id_sent) // when the drainer has sent data, the send.log exists.
 * |--------partId2
 */
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

  virtual ~DrainerTask() {
    VLOG(3) << "Release Drainer Task";
  }

  /**
   * @brief Process all parts under the space, one part generates a DrainerSubTask.
   *
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<DrainerSubTask>>
   * std::vector<DrainerSubTask> stores all generated drainerSubTasks
   */
  virtual ErrorOr<nebula::cpp2::ErrorCode, std::vector<DrainerSubTask>> genSubTasks();

  /**
   * @brief Generate a drainerSubTask. The drainerSubTask performs the following work:
   * Read the wal file of this part, process data, and then send data through storage client or meta
   * client. Write send.log file if successful. Send data in batch to storage client or meta client.
   *
   * @param part The partId of drainerSubTask
   * @param wal The current wal of drainerSubTask
   * @return nebula::cpp2::ErrorCode Whether succeed
   */
  nebula::cpp2::ErrorCode genSubTask(PartitionID part, nebula::wal::FileBasedWal* wal);

  /**
   * @brief Unified interface for processing storage data or meta data
   *
   * @param part The PartId to process
   * @param log LogMsg in wal log
   * @return StatusOr<std::unordered_map<PartitionID, std::vector<std::string>>>
   */
  StatusOr<std::unordered_map<PartitionID, std::vector<std::string>>> ProcessData(
      PartitionID part, const std::string& log);

  /**
   * @brief Update send.log(last_log_id_sent)
   *
   * @param sendLogFile Filename where lastLogIdsend is stored
   * @param spaceId The space to process
   * @param part The part to process
   * @param lastLogIdSend The latest lastLogIdSend
   * @return True if the update is successful
   */
  bool updateSendLog(const std::string& sendLogFile,
                     GraphSpaceID spaceId,
                     PartitionID part,
                     LogID lastLogIdSend);

  /**
   * @brief Send data to meta client or storage client according to part Id.
   * If part Id is 0, send data through meta client，otherwise send data through storage client.
   *
   * @param logs The log data to send
   * @param part The part to process, the partId is from master cluster.
   * @return Status Whether the data was sent successfully
   */
  virtual Status sendData(std::unordered_map<PartitionID, std::vector<std::string>>& logs,
                          PartitionID part);

  /**
   * @brief Read send.log(last_log_id_sent)
   * If send.log file not exists, return 0.
   *
   * @param sendLogFile Filename where lastLogIdsend is stored
   * @param spaceId The spaceId to process
   * @param part The partId to process, the partId is from master cluster.
   * @return StatusOr<LogID> Return the latest lastLogIdSend if successful.
   */
  StatusOr<LogID> readSendLog(const std::string& sendLogFile,
                              GraphSpaceID spaceId,
                              PartitionID part);

  /**
   * @brief Read recv.log(last_log_id_recv)
   *
   * @param recvLogFile  recv log filename
   * @param spaceId the spaceId to process
   * @param part the partId to process, the partId is from master cluster.
   * @return StatusOr<LogID> Return the latest lastLogIdRecv
   */
  StatusOr<LogID> readRecvLog(const std::string& recvLogFile,
                              GraphSpaceID spaceId,
                              PartitionID part);

  /**
   * @brief Get the Space Id
   *
   * @return int SpaceId
   */
  int getSpaceId() {
    return spaceId_;
  }

  /**
   * @brief Processing when the Drainer task finishes
   */
  void finish() {
    finish(rc_);
  }

  /**
   * @brief Set the state of drainer task
   *
   * @param rc nebula::cpp2::ErrorCode
   */
  void finish(nebula::cpp2::ErrorCode rc) {
    if (rc == nebula::cpp2::ErrorCode::SUCCEEDED) {
      VLOG(2) << "Drainer task space " << spaceId_ << " succeeded";
    } else {
      LOG(INFO) << "Drainer task space " << spaceId_ << " failed, "
                << apache::thrift::util::enumNameSafe(rc);
    }
    auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;
    rc_.compare_exchange_strong(suc, rc);
  }

  /**
   * @brief Return Status of drainer task
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode status() const {
    return rc_;
  }

  /**
   * @brief Processing when SubDrainer task finishes
   *
   * @param rc status
   * @param partId  The partId of drainerSubTask
   */
  void subTaskFinish(nebula::cpp2::ErrorCode rc, PartitionID partId);

  /**
   * @brief Cancel the drainer task and set the corresponding state
   */
  void cancel() {
    FLOG_INFO("task(%d) cancelled", spaceId_);
    auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;
    rc_.compare_exchange_strong(suc, nebula::cpp2::ErrorCode::E_USER_CANCEL);
  }

  ///////////////////////////////////////////////////////////
  //
  //  Process storage data
  //
  ///////////////////////////////////////////////////////////
  /**
   * @brief Check schema and repart operation. One log, decode once.
   * If the schema version does not match, return an error.
   * If repart is required, calculate the new partId and generate a new key.
   * If repart, one log may generate multiple logs.
   *
   * @param part the partId to process, the partId is from master cluster.
   * @param log  logMsg format in wal log
   * @return StatusOr<std::unordered_map<PartitionID, std::vector<std::string>>>
   * If repart is required, return the new partId and new key，otherwise return the original partId
   * and key
   */
  StatusOr<std::unordered_map<PartitionID, std::vector<std::string>>> ProcessStorageData(
      PartitionID part, const std::string& log);

  /**
   * @brief Check tag, edge, index information of one log and repart if needed
   * If repart, one log may generate multiple logs.
   *
   * @param part The partId to process, the partId is from the master cluster.
   * @param rawKey Old key
   * @param rawVal Old value
   * @param checkSchema Whether to check the schema
   * @return StatusOr<std::pair<PartitionID, std::string>>
   * If repart is required, return the new partId and new key，Otherwise return the original partId
   * and key
   */
  StatusOr<std::pair<PartitionID, std::string>> checkSchemaAndRepartition(
      PartitionID part,
      const folly::StringPiece& rawKey,
      const folly::StringPiece& rawVal,
      bool checkSchema);

  /**
   * @brief If rapart is required, calculate the new partId and return the new key
   * If repart, one log may generate multiple logs.
   *
   * @param part The partId to process, the partId is from the master cluster.
   * @param rawKey Old key
   * @return StatusOr<std::pair<PartitionID, std::string>>
   * If repart is required, return the new partId and new key，Otherwise return the original partId
   * and key
   */
  StatusOr<std::pair<PartitionID, std::string>> vertexRepart(PartitionID part,
                                                             const folly::StringPiece& rawKey);

  /**
   * @brief Check the tag schema of the tag data, if rapart is required, calculate the new partId
   * and return the new key
   * If repart, one log may generate multiple logs.
   *
   * @param part The partId to process, the partId is from the master cluster.
   * @param rawKey Old key
   * @param rawVal Old value
   * @param checkSchema Whether to check the schema
   * @return StatusOr<std::pair<PartitionID, std::string>>
   * If repart is required, return the new partId and new key，Otherwise return the original partId
   * and key
   */
  StatusOr<std::pair<PartitionID, std::string>> checkTagSchemaAndRepart(
      PartitionID part,
      const folly::StringPiece& rawKey,
      const folly::StringPiece& rawVal,
      bool checkSchema);

  /**
   * @brief Check the edge schema of the tag data, if rapart is required, calculate the new partId
   * and return the new key
   * If repart, one log may generate multiple logs.
   *
   * @param part The partId to process, the partId is from the master cluster.
   * @param rawKey Old key
   * @param rawVal Old value
   * @param checkSchema Whether to check the schema
   * @return StatusOr<std::pair<PartitionID, std::string>>
   * If repart is required, return the new partId and new key，Otherwise return the original partId
   * and key
   */
  StatusOr<std::pair<PartitionID, std::string>> checkEdgeSchemaAndRepart(
      PartitionID part,
      const folly::StringPiece& rawKey,
      const folly::StringPiece& rawVal,
      bool checkSchema);

  /**
   * @brief  Check the tag or edge index, if rapart is required, calculate the new partId and return
   * the new key.
   * If repart, one log may generate multiple logs.
   *
   * @param part The partId to process, the partId is from the master cluster.
   * @param rawKey Old key
   * @return StatusOr<std::pair<PartitionID, std::string>>
   * If repart is required, return the new partId and new key，Otherwise return the original partId
   * and key
   */
  StatusOr<std::pair<PartitionID, std::string>> checkIndexAndRepart(
      PartitionID part, const folly::StringPiece& rawKey);

  /////////////////////// end storge data ///////////////////

  ///////////////////////////////////////////////////////////
  //
  //  Process meta data
  //
  ///////////////////////////////////////////////////////////
  /**
   * @brief Replace spaceId. so that one log decode one operation
   *
   * @param log LogMsg in wal log
   * @return StatusOr<std::unordered_map<PartitionID, std::vector<std::string>>>
   * partId is always 0
   * std::vector<std::string> are the new keys of replacing the spaceid
   */
  StatusOr<std::unordered_map<PartitionID, std::vector<std::string>>> ProcessMetaData(
      const std::string& log);

  /**
   * @brief Replace the spaceId in the key with the spaceId from the slave cluster.
   *
   * @param space The spaceId is from the slave cluster
   * @param rawKey Old key
   * @return StatusOr<std::string> Return the new key if successful
   */
  StatusOr<std::string> adjustSpaceIdInKey(GraphSpaceID space, const folly::StringPiece& rawKey);

  /////////////////////// end meta data ///////////////////

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

/**
 * @brief Generate a DrainerTask shared_ptr
 */
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
