/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "drainer/CommonUtils.h"
#include "drainer/DrainerFlags.h"
#include "drainer/DrainerTask.h"
#include "drainer/DrainerTaskManager.h"
#include "drainer/processor/AppendLogProcessor.h"
#include "drainer/test/TestUtils.h"
#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/wal/FileBasedWal.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"

DECLARE_int32(wal_ttl);
DECLARE_int64(wal_file_size);
DECLARE_int32(wal_buffer_size);
DECLARE_bool(wal_sync);
DECLARE_int32(cluster_id);
DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace drainer {

using ErrOrSubTasks = ErrorOr<nebula::cpp2::ErrorCode, std::vector<DrainerSubTask>>;

struct HookableTask : public DrainerTask {
  HookableTask() {
    fGenSubTasks = [&]() { return subTasks; };
  }

  ErrOrSubTasks genSubTasks() override {
    LOG(INFO) << "HookableTask::genSubTasks() subTasks.size()=" << subTasks.size();
    return fGenSubTasks();
  }

  // For the continuity of logId, do nothing
  bool preProcessLog(LogID logId, TermID termId, ClusterID clusterId, const std::string& log) {
    UNUSED(logId);
    UNUSED(termId);
    UNUSED(clusterId);
    UNUSED(log);
    return true;
  }

  StatusOr<std::tuple<ClusterID, int32_t, ClusterID>> getClusterIdPartsFormClusterSpaceId(
      std::string& path) {
    int32_t fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
      return Status::Error("Failed to open the file %s", path.c_str());
    }
    std::tuple<ClusterID, int32_t, ClusterID> ret;
    // read fromCluster, fromSpaceId, partNum, toCluster, toSpaceId
    ClusterID fromClusterId;
    CHECK_EQ(pread(fd, reinterpret_cast<char*>(&fromClusterId), sizeof(ClusterID), 0),
             static_cast<ssize_t>(sizeof(ClusterID)));

    GraphSpaceID fromSpaceId;
    CHECK_EQ(
        pread(fd, reinterpret_cast<char*>(&fromSpaceId), sizeof(GraphSpaceID), sizeof(ClusterID)),
        static_cast<ssize_t>(sizeof(GraphSpaceID)));

    int32_t parts;
    auto offset = sizeof(ClusterID) + sizeof(GraphSpaceID);
    CHECK_EQ(pread(fd, reinterpret_cast<char*>(&parts), sizeof(int32_t), offset),
             static_cast<ssize_t>(sizeof(parts)));

    ClusterID toClusterId;
    offset = sizeof(ClusterID) + sizeof(GraphSpaceID) + sizeof(int32_t);
    CHECK_EQ(pread(fd, reinterpret_cast<char*>(&toClusterId), sizeof(ClusterID), offset),
             static_cast<ssize_t>(sizeof(ClusterID)));
    close(fd);

    std::get<0>(ret) = fromClusterId;
    std::get<1>(ret) = parts;
    std::get<2>(ret) = toClusterId;
    return ret;
  }

  Status checkClusterAndParts(DrainerEnv* env, std::string& spaceDir, GraphSpaceID spaceId) {
    auto iter = env->spaceClusters_.find(spaceId);
    auto oldIter = env->spaceOldParts_.find(spaceId);

    ClusterID fromClusterId;
    ClusterID toClusterId;
    int32_t partNum;
    if (iter == env->spaceClusters_.end() || oldIter == env->spaceOldParts_.end()) {
      // read cluster_space_id
      auto spaceFile = folly::stringPrintf("%s/cluster_space_id", spaceDir.c_str());
      auto partsRet = getClusterIdPartsFormClusterSpaceId(spaceFile);
      if (!partsRet.ok()) {
        LOG(ERROR) << "Get space partition number failed from cluster_space_id";
        return partsRet.status();
      }
      std::tie(fromClusterId, partNum, toClusterId) = partsRet.value();
    }

    if (iter == env->spaceClusters_.end()) {
      env->spaceClusters_[spaceId] = std::make_pair(fromClusterId, toClusterId);
    }

    if (oldIter == env->spaceOldParts_.end()) {
      env->spaceOldParts_.emplace(spaceId, partNum);
    }

    auto newIter = env->spaceNewParts_.find(spaceId);
    if (newIter == env->spaceNewParts_.end()) {
      // Get new parts from schema
      auto partRet = env->schemaMan_->getPartsNum(spaceId);
      if (!partRet.ok()) {
        LOG(ERROR) << "Get space partition number failed from schema";
        return partRet.status();
      }
      auto parts = partRet.value();
      env->spaceNewParts_.emplace(spaceId, parts);
    }
    return Status::OK();
  }

  nebula::cpp2::ErrorCode addTaskForPart(DrainerEnv* env,
                                         GraphSpaceID space,
                                         std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                                         PartitionID part) {
    // Initialize the drainerTask environment
    env_ = env;
    spaceId_ = space;
    ioThreadPool_ = ioThreadPool;

    auto spacepath = env->drainerPath_ + "/nebula";
    if (!fs::FileUtils::exist(spacepath)) {
      LOG(ERROR) << folly::stringPrintf("drainer space path '%s' not exists.", spacepath.c_str());
      return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    auto spaceDirs = fs::FileUtils::listAllDirsInDir(spacepath.c_str());

    bool findSpace = false;
    for (auto& sdir : spaceDirs) {
      LOG(INFO) << "Scan path \"" << spacepath << "/" << sdir << "\"";
      try {
        GraphSpaceID spaceId;
        auto spaceDir = folly::stringPrintf("%s/%s", spacepath.c_str(), sdir.c_str());
        try {
          spaceId = folly::to<GraphSpaceID>(sdir);
        } catch (const std::exception& ex) {
          LOG(ERROR) << "Data path " << spaceDir << " invalid: " << ex.what();
          continue;
        }
        if (spaceId != space) {
          continue;
        }
        findSpace = true;
        // check space exists
        auto spaceNameRet = env->schemaMan_->toGraphSpaceName(spaceId);
        if (!spaceNameRet.ok()) {
          LOG(ERROR) << folly::stringPrintf("space '%d' not exists.", space);
          return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
        }
        // check space oldparts and newParts
        auto ret = checkClusterAndParts(env, spaceDir, spaceId);
        if (!ret.ok()) {
          LOG(ERROR) << "Get clusterId and partNum failed.";
          return nebula::cpp2::ErrorCode::E_PART_MISMATCH;
        }

        // list all part dirs
        auto partDirs = fs::FileUtils::listAllDirsInDir(spaceDir.c_str());
        bool findPart = false;
        for (auto& pdir : partDirs) {
          PartitionID partId;
          auto partDir = folly::stringPrintf("%s/%s", spaceDir.c_str(), pdir.c_str());
          try {
            partId = folly::to<PartitionID>(pdir);
          } catch (const std::exception& ex) {
            LOG(ERROR) << folly::stringPrintf(
                "Data path %s invalid %s", partDir.c_str(), ex.what());
            return nebula::cpp2::ErrorCode::E_PART_NOT_FOUND;
          }
          if (partId != part) {
            continue;
          }
          findPart = true;

          // insert into DrainerEnv.wals_
          auto spaceIter = env->wals_.find(spaceId);
          if (spaceIter != env->wals_.end()) {
            auto partIter = spaceIter->second.find(partId);
            if (partIter != spaceIter->second.end()) {
              break;
            }
          }

          wal::FileBasedWalInfo info;
          info.idStr_ = folly::stringPrintf("[Space: %d, Part: %d] ", spaceId, partId);
          info.spaceId_ = spaceId;
          info.partId_ = partId;
          wal::FileBasedWalPolicy policy;
          policy.fileSize = FLAGS_wal_file_size;
          policy.bufferSize = FLAGS_wal_buffer_size;
          policy.sync = FLAGS_wal_sync;

          // Assuming that the wal logId from sync listener to drainer is continuous
          // That is, the wal log contains heartbeat information
          auto walPath = folly::stringPrintf("%s/wal", partDir.c_str());
          env->wals_[spaceId][partId] = wal::FileBasedWal::getWal(
              walPath,
              std::move(info),
              std::move(policy),
              [this](
                  LogID logId, TermID logTermId, ClusterID logClusterId, const std::string& log) {
                return this->preProcessLog(logId, logTermId, logClusterId, log);
              },
              env->diskMan_,
              true);
        }
        if (!findPart) {
          LOG(ERROR) << folly::stringPrintf("part '%d' not exists.", part);
          return nebula::cpp2::ErrorCode::E_PART_NOT_FOUND;
        }
      } catch (std::exception& e) {
        LOG(FATAL) << "Invalid data directory \"" << sdir << "\"";
      }  // end try
    }

    if (!findSpace) {
      LOG(ERROR) << folly::stringPrintf("space '%d' not exists.", space);
      return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }

    // init the drainerTask environment
    auto iter = env->spaceClusters_.find(space);
    if (iter == env->spaceClusters_.end()) {
      LOG(ERROR) << folly::stringPrintf(
          "Space %d master cluster id and slave "
          "cluster id not exists.",
          space);
      return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    masterClusterId_ = iter->second.first;
    slaveClusterId_ = iter->second.second;

    auto retVidLens = env->schemaMan_->getSpaceVidLen(space);
    if (!retVidLens.ok()) {
      LOG(ERROR) << retVidLens.status();
      return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    vIdLen_ = retVidLens.value();

    // build subtask
    auto wal = env->wals_[space][part];
    auto ret = DrainerTask::genSubTask(space, part, wal.get());
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << folly::stringPrintf("Execute subtask failed, space %d part %d.", space, part);
      return ret;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  void addSubTask(std::function<nebula::cpp2::ErrorCode()> subTask, PartitionID part) {
    subTasks.emplace_back(subTask, part);
  }

  // Mock drainer sends data to storage successfully
  Status processorAndSend(std::vector<std::string> logs, PartitionID part) override {
    auto oldPartIter = env_->spaceOldParts_.find(spaceId_);
    if (oldPartIter == env_->spaceOldParts_.end()) {
      return Status::Error("Do not set old partNum in space %d", spaceId_);
    }
    auto oldPartNum = oldPartIter->second;

    auto newPartIter = env_->spaceNewParts_.find(spaceId_);
    if (newPartIter == env_->spaceNewParts_.end()) {
      return Status::Error("Do not set new partNum in space %d", spaceId_);
    }
    newPartNum_ = newPartIter->second;

    std::unordered_map<PartitionID, std::vector<std::string>> data;
    // If the number of parts in the space on the master-slave cluster is the same,
    // Do not re-partitioned, at this time, the number of parts is 1.
    if (oldPartNum == newPartNum_) {
      data.emplace(part, std::move(logs));
    } else {
      // re-partitioned, the number of parts is unknown at this time.
      auto retLogStats = rePartitionData(logs, part);
      if (!retLogStats.ok()) {
        return retLogStats.status();
      }
      data = retLogStats.value();
    }

    // mock drainer server to storage client, assuming that the data is sent successfully.
    return Status::OK();
  }

  std::function<ErrOrSubTasks()> fGenSubTasks;

  std::vector<DrainerSubTask> subTasks;
};

class DummyDrainerTaskManager : public DrainerTaskManager {
 public:
  static DummyDrainerTaskManager* instance() {
    static DummyDrainerTaskManager dummyDrainerTaskManager;
    return &dummyDrainerTaskManager;
  }

  bool init(DrainerEnv* env, std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool) {
    CHECK_NOTNULL(env);
    env_ = env;
    ioThreadPool_ = ioThreadPool, interClient_ = std::make_unique<storage::InternalStorageClient>(
                                      ioThreadPool_, env_->metaClient_);

    LOG(INFO) << "Max concurrenct sub drainer task: " << FLAGS_max_concurrent_subdrainertasks;
    pool_ = std::make_unique<ThreadPool>(FLAGS_max_concurrent_subdrainertasks);
    bgThread_ = std::make_unique<thread::GenericWorker>();
    if (!bgThread_->start()) {
      LOG(ERROR) << "Background thread start failed";
      return false;
    }
    shutdown_.store(false, std::memory_order_release);
    LOG(INFO) << "Exit DrainerTaskManager::init()";
    return true;
  }

  // Get a subtask execution from drainer task
  void runSubTask(GraphSpaceID spaceId) { return DrainerTaskManager::runSubTask(spaceId); }

  // schedule
  bool run() {
    std::chrono::milliseconds interval{20};  // 20ms
    if (tasks_.empty()) {
      LOG(ERROR) << "Drainer task is empty.";
      return false;
    }

    folly::Optional<GraphSpaceID> optTaskHandle{folly::none};
    if (!optTaskHandle && !shutdown_.load(std::memory_order_acquire) && taskQueue_.size() != 0) {
      // Because the subtask of each drainer task is executed concurrently
      // and asynchronously. So there is a case:
      // It was not empty when tasks_.empty() was judged earlier.
      // But here, it is judged that tasks_ is already empty and taskQueue_ is also empty.
      optTaskHandle = taskQueue_.try_take_for(interval);
    } else {
      LOG(ERROR) << "Init drainer tasks failed.";
      return false;
    }

    if (shutdown_.load(std::memory_order_acquire)) {
      LOG(INFO) << "Detect DrainerTaskManager::shutdown()";
      return false;
    }
    if (!optTaskHandle) {
      LOG(INFO) << "Get drainer task failed.";
      return false;
    }

    auto spaceId = *optTaskHandle;
    VLOG(3) << folly::stringPrintf("dequeue drainer task(%d)", spaceId);
    auto it = tasks_.find(spaceId);
    if (it == tasks_.end()) {
      LOG(INFO) << folly::stringPrintf("Trying to exec non-exist drainer task(%d)", spaceId);
      return false;
    }

    LOG(INFO) << apache::thrift::util::enumNameSafe(tasks_[spaceId]->rc_.load());

    auto drainerTask = it->second;
    auto statusCode = drainerTask->status();
    if (statusCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      // remove cancelled tasks
      LOG(INFO) << folly::sformat("Remove drainer task %d genSubTask failed, err={}",
                                  spaceId,
                                  apache::thrift::util::enumNameSafe(statusCode));
      tasks_.erase(spaceId);
      return false;
    }

    LOG(INFO) << "Waiting for incoming drainer task";
    auto errOrSubTasks = drainerTask->genSubTasks();
    if (!nebula::ok(errOrSubTasks)) {
      LOG(ERROR) << folly::sformat(
          "Drainer task %d genSubTask failed, err={}",
          spaceId,
          apache::thrift::util::enumNameSafe(nebula::error(errOrSubTasks)));
      drainerTask->finish(nebula::error(errOrSubTasks));
      tasks_.erase(spaceId);
      LOG(INFO) << "Drainer task generate subtask failed.";
      return false;
    }

    auto subTasks = nebula::value(errOrSubTasks);
    for (auto& subtask : subTasks) {
      drainerTask->subtasks_.add(subtask);
    }

    auto subTaskConcurrency =
        std::min(static_cast<size_t>(FLAGS_max_concurrent_subdrainertasks), subTasks.size());
    drainerTask->unFinishedSubTask_ = subTasks.size();

    if (0 == subTasks.size()) {
      FLOG_INFO("drainer task(%d) finished, no subtask", spaceId);
      drainerTask->finish();
      tasks_.erase(spaceId);
      return false;
    }

    VLOG(3) << folly::stringPrintf("Run drainer task(%d), %zu subtasks in %zu thread",
                                   spaceId,
                                   drainerTask->unFinishedSubTask_.load(),
                                   subTaskConcurrency);
    for (size_t i = 0; i < subTaskConcurrency; ++i) {
      // run task
      pool_->add(std::bind(&DummyDrainerTaskManager::runSubTask, this, spaceId));
    }

    LOG(INFO) << "DummyDrainerTaskManager::run";
    return true;
  }
};

// The directory structure of the data is as follows:
/* |--data/drainer
 * |----nebula
 * |------toSpaceId
 * |--------cluster_space_id (clusterId_spaceId_parts from master and clusterId from slave)
 * |--------partId1(from master space)
 * |----------wal
 * |-------------0000000000000000001.wal
 * |----------recv.log(last_log_id_recv)
 * |----------send.log(last_log_id_sent)
 */
// Use the real directory structure to test. But there is one part wal log data.
TEST(DrainerTaskTest, PartNumSameOnePartTest) {
  fs::TempDir rootPath("/tmp/drainerTaskTest.XXXXXX");
  auto taskMgr = DummyDrainerTaskManager::instance();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());

  mock::MockCluster cluster;
  std::string path = rootPath.path();
  cluster.initStorageKV(path.c_str());
  auto* storageEnv = cluster.storageEnv_.get();
  FLAGS_drainer_task_run_interval = 2;

  auto drainerEnv = std::make_unique<nebula::drainer::DrainerEnv>();
  auto datapath = path + "/data/drainer";
  drainerEnv->drainerPath_ = datapath;
  drainerEnv->schemaMan_ = storageEnv->schemaMan_;
  drainerEnv->indexMan_ = storageEnv->indexMan_;
  drainerEnv->metaClient_ = storageEnv->metaClient_;

  // Construct wal log data of two parts(partId is 1, 2)
  GraphSpaceID space = 1;

  // The space information(vidLen, vidType) of the master cluster must be consistent
  // with the information of the slave cluster space.
  // Here the information(vidLen, vidType) of the slave cluster space is used as
  // the information of the master cluster space
  auto toSpaceNameRet = drainerEnv->schemaMan_->toGraphSpaceName(space);
  ASSERT_TRUE(toSpaceNameRet.ok());
  auto toSpaceName = toSpaceNameRet.value();

  auto toSpaceVidLenRet = drainerEnv->schemaMan_->getSpaceVidLen(space);
  ASSERT_TRUE(toSpaceVidLenRet.ok());
  auto toSpaceVidLen = toSpaceVidLenRet.value();

  auto toSpaceVidTypeRet = drainerEnv->schemaMan_->getSpaceVidType(space);
  ASSERT_TRUE(toSpaceVidTypeRet.ok());
  auto toSpaceVidType = toSpaceVidTypeRet.value();

  auto partNumRet = drainerEnv->schemaMan_->getPartsNum(space);
  ASSERT_TRUE(partNumRet.ok());
  auto partNum = partNumRet.value();

  // check wal file in DrainerEnv
  EXPECT_EQ(0, drainerEnv->wals_.size());

  LOG(INFO) << "Test hookable task...";
  folly::Promise<int> p0;
  folly::Promise<int> p1;

  auto f0 = p0.getFuture();
  auto f1 = p1.getFuture();

  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(5);
  std::shared_ptr<DrainerTask> task = std::make_shared<HookableTask>();
  HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());

  size_t totalSubTask = 1;

  std::atomic<int> subTaskCalled{0};
  for (size_t i = 1; i <= totalSubTask; ++i) {
    PartitionID part = i;
    mockTask->addSubTask(
        [&, j = i]() -> nebula::cpp2::ErrorCode {
          LOG(INFO) << "before f0.wait()";
          f0.wait();
          LOG(INFO) << "after f0.wait()";
          // generate part data
          PartitionID partId = j;
          auto* processor = AppendLogProcessor::instance(drainerEnv.get(), nullptr);
          LOG(INFO) << "Build AppendLogRequest...";
          auto reqRet = mockAppendLogReq(drainerEnv->schemaMan_,
                                         space,
                                         toSpaceName,
                                         partNum,
                                         partId,
                                         toSpaceVidLen,
                                         toSpaceVidType);
          EXPECT_TRUE(reqRet.ok());

          auto req = reqRet.value();
          auto lastRecvLogIdReq = *req.last_log_id_to_send_ref();

          LOG(INFO) << "Test AppendLogProcessor...";
          auto fut = processor->getFuture();
          processor->process(req);
          auto resp = std::move(fut).get();
          EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, *resp.error_code_ref());

          LOG(INFO) << "Check recv.log file ...";
          // build recv log file path
          auto recvLogFile =
              folly::stringPrintf("%s/nebula/%d/%d/recv.log", datapath.c_str(), space, partId);
          auto recvLogIdRet = readRecvLogFile(recvLogFile);
          EXPECT_TRUE(recvLogIdRet.ok());
          auto recvLogId = recvLogIdRet.value();

          EXPECT_EQ(lastRecvLogIdReq, recvLogId);

          // build context & run subtask
          nebula::cpp2::ErrorCode retCode =
              mockTask->addTaskForPart(drainerEnv.get(), space, ioThreadPool, partId);
          ++subTaskCalled;
          p1.setValue(1);
          return retCode;
        },
        part);
  }

  // hookable task
  taskMgr->init(drainerEnv.get(), ioThreadPool);

  // After addAsyncTask, tasks_ in DrainerTaskManager is not empty,
  // so addAsyncTask is not executed.
  // Until tasks_ is empty, addAsyncTask is executed again.
  taskMgr->addAsyncTask(space, task);

  auto retVal = taskMgr->run();
  EXPECT_TRUE(retVal);

  // sleep for a period of time to ensure that the child thread has been executed.
  sleep(FLAGS_drainer_task_run_interval * 2);

  LOG(INFO) << "wait drainer task finished.";
  LOG(INFO) << "before p0.setValue()";
  p0.setValue(1);
  LOG(INFO) << "after p0.setValue()";
  LOG(INFO) << "before f1.wait()";
  f1.wait();
  LOG(INFO) << "after f1.wait()";

  while (!taskMgr->isFinished(space)) {
    usleep(1000);
  }
  sleep(1);

  EXPECT_EQ(subTaskCalled, totalSubTask);

  // check wal file in DrainerEnv
  EXPECT_EQ(1, drainerEnv->wals_.size());
  EXPECT_TRUE(drainerEnv->wals_.find(space) != drainerEnv->wals_.end());
  EXPECT_EQ(1, drainerEnv->wals_.find(space)->second.size());

  // check sendLog file and revcLog file
  // read sendLog file
  LOG(INFO) << "Read sendLog file...";
  for (int i = 1; i <= 1; i++) {
    PartitionID partId = i;
    auto sendLogFile = folly::stringPrintf(
        "%s/nebula/%d/%d/send.log", drainerEnv->drainerPath_.c_str(), space, partId);

    auto retSend = mockTask->readSendLog(sendLogFile, space, partId);
    EXPECT_TRUE(retSend.ok());
    auto lastLogIdSend = retSend.value();

    auto recvLogFile = folly::stringPrintf(
        "%s/nebula/%d/%d/recv.log", drainerEnv->drainerPath_.c_str(), space, partId);

    auto retRecv = mockTask->readRecvLog(recvLogFile, space, partId);
    EXPECT_TRUE(retRecv.ok());
    auto lastLogIdRecv = retRecv.value();

    EXPECT_EQ(lastLogIdSend, lastLogIdRecv);
  }

  while (taskMgr->tasks_.size() != 0) {
    LOG(INFO) << "wailt all tasks finished, current has " << taskMgr->tasks_.size() << " size.";
    usleep(1000);
  }

  taskMgr->shutdown();
  LOG(INFO) << taskMgr->taskQueue_.size();
  LOG(INFO) << taskMgr->tasks_.size();
}

// The directory structure of the data is as follows:
/* |--data/drainer
 * |----nebula
 * |------toSpaceId
 * |--------cluster_space_id (clusterId_spaceId_parts from master and clusterId from slave)
 * |--------partId1(from master space)
 * |----------wal
 * |-------------0000000000000000001.wal
 * |----------recv.log(last_log_id_recv)
 * |----------send.log(last_log_id_sent)
 * |--------partId2(from master space)
 * |----------wal
 * |-------------0000000000000000001.wal
 * |----------recv.log(last_log_id_recv)
 * |----------send.log(last_log_id_sent)
 */
// Use the real directory structure to test. But there is two parts wal log data.
TEST(DrainerTaskTest, PartNumSameMultiPartTest) {
  fs::TempDir rootPath("/tmp/drainerTaskTest.XXXXXX");
  auto taskMgr = DummyDrainerTaskManager::instance();
  LOG(INFO) << taskMgr->taskQueue_.size();
  LOG(INFO) << taskMgr->tasks_.size();

  mock::MockCluster cluster;
  std::string path = rootPath.path();
  cluster.initStorageKV(path.c_str());
  auto* storageEnv = cluster.storageEnv_.get();

  auto max_concurrent_subdrainertasks = FLAGS_max_concurrent_subdrainertasks;
  FLAGS_max_concurrent_subdrainertasks = 10;
  FLAGS_drainer_task_run_interval = 2;

  auto drainerEnv = std::make_unique<nebula::drainer::DrainerEnv>();
  auto datapath = path + "/data/drainer";
  drainerEnv->drainerPath_ = datapath;
  drainerEnv->schemaMan_ = storageEnv->schemaMan_;
  drainerEnv->indexMan_ = storageEnv->indexMan_;
  drainerEnv->metaClient_ = storageEnv->metaClient_;

  // Construct wal log data of two parts(partId is 1, 2)
  GraphSpaceID space = 1;

  // The space information(vidLen, vidType) of the master cluster must be consistent
  // with the information of the slave cluster space.
  // Here the information(vidLen, vidType) of the slave cluster space is used as
  // the information of the master cluster space
  auto toSpaceNameRet = drainerEnv->schemaMan_->toGraphSpaceName(space);
  ASSERT_TRUE(toSpaceNameRet.ok());
  auto toSpaceName = toSpaceNameRet.value();

  auto toSpaceVidLenRet = drainerEnv->schemaMan_->getSpaceVidLen(space);
  ASSERT_TRUE(toSpaceVidLenRet.ok());
  auto toSpaceVidLen = toSpaceVidLenRet.value();

  auto toSpaceVidTypeRet = drainerEnv->schemaMan_->getSpaceVidType(space);
  ASSERT_TRUE(toSpaceVidTypeRet.ok());
  auto toSpaceVidType = toSpaceVidTypeRet.value();

  // slave cluster partNum is 6
  // Suppose master cluster partNum is same to slave cluster partNum
  auto partNumRet = drainerEnv->schemaMan_->getPartsNum(space);
  ASSERT_TRUE(partNumRet.ok());
  auto partNum = partNumRet.value();

  // check wal file in DrainerEnv
  EXPECT_EQ(0, drainerEnv->wals_.size());

  LOG(INFO) << "Test hookable task...";
  folly::Promise<int> p0;
  folly::Promise<int> p1;
  folly::Promise<int> p2;

  auto f0 = p0.getFuture();
  auto f1 = p1.getFuture();
  auto f2 = p2.getFuture();

  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(5);
  std::shared_ptr<DrainerTask> task = std::make_shared<HookableTask>();
  HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());

  size_t totalSubTask = 2;
  std::atomic<int> subTaskCalled{0};

  for (size_t i = 1; i <= totalSubTask; ++i) {
    PartitionID part = i;
    mockTask->addSubTask(
        [&, j = i]() -> nebula::cpp2::ErrorCode {
          if (j == 1) {
            LOG(INFO) << "before f0.wait()";
            f0.wait();
            LOG(INFO) << "after f0.wait()";
          } else {
            LOG(INFO) << "before f1.wait()";
            f1.wait();
            LOG(INFO) << "after f1.wait()";
          }

          // generate part data
          PartitionID partId = j;
          auto* processor = AppendLogProcessor::instance(drainerEnv.get(), nullptr);
          LOG(INFO) << "Build AppendLogRequest...";
          auto reqRet = mockAppendLogReq(drainerEnv->schemaMan_,
                                         space,
                                         toSpaceName,
                                         partNum,
                                         partId,
                                         toSpaceVidLen,
                                         toSpaceVidType);
          EXPECT_TRUE(reqRet.ok());

          auto req = reqRet.value();
          auto lastRecvLogIdReq = *req.last_log_id_to_send_ref();

          LOG(INFO) << "Test AppendLogProcessor...";
          auto fut = processor->getFuture();
          processor->process(req);
          auto resp = std::move(fut).get();
          EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, *resp.error_code_ref());

          LOG(INFO) << "Check recv.log file ...";
          // build recv log file path
          auto recvLogFile =
              folly::stringPrintf("%s/nebula/%d/%d/recv.log", datapath.c_str(), space, partId);
          auto recvLogIdRet = readRecvLogFile(recvLogFile);
          EXPECT_TRUE(recvLogIdRet.ok());
          auto recvLogId = recvLogIdRet.value();

          EXPECT_EQ(lastRecvLogIdReq, recvLogId);

          // build context & run subtask
          nebula::cpp2::ErrorCode retCode =
              mockTask->addTaskForPart(drainerEnv.get(), space, ioThreadPool, partId);
          ++subTaskCalled;
          if (j == 1) {
            LOG(INFO) << "before p1.setValue()";
            p1.setValue(1);
            LOG(INFO) << "after p1.setValue()";
          } else {
            LOG(INFO) << "before p2.setValue()";
            p2.setValue(1);
            LOG(INFO) << "after p2.setValue()";
          }
          return retCode;
        },
        part);
  }

  if (taskMgr->tasks_.size() != 0) {
    for (auto& e : taskMgr->tasks_) LOG(INFO) << e.first;
  }

  // hookable task
  taskMgr->init(drainerEnv.get(), ioThreadPool);

  // After addAsyncTask, tasks_ in DrainerTaskManager is not empty,
  // so addAsyncTask is not executed.
  // Until tasks_ is empty, addAsyncTask is executed again.
  taskMgr->addAsyncTask(space, task);

  auto retVal = taskMgr->run();
  EXPECT_TRUE(retVal);
  // sleep for a period of time to ensure that the child thread has been executed.
  sleep(FLAGS_drainer_task_run_interval * 2);

  LOG(INFO) << "wait drainer task finished.";
  LOG(INFO) << "before p0.setValue()";
  p0.setValue(1);
  LOG(INFO) << "after p0.setValue()";
  LOG(INFO) << "before f2.wait()";
  f2.wait();
  LOG(INFO) << "after f2.wait()";

  while (!taskMgr->isFinished(space)) {
    usleep(1000);
  }
  sleep(1);
  EXPECT_EQ(subTaskCalled, totalSubTask);

  // check wal file in DrainerEnv
  EXPECT_EQ(1, drainerEnv->wals_.size());
  EXPECT_TRUE(drainerEnv->wals_.find(space) != drainerEnv->wals_.end());
  EXPECT_EQ(2, drainerEnv->wals_.find(space)->second.size());

  // check sendLog file and revcLog file
  // read sendLog file
  LOG(INFO) << "Read sendLog file...";
  for (size_t i = 1; i <= totalSubTask; i++) {
    PartitionID partId = i;
    auto sendLogFile = folly::stringPrintf(
        "%s/nebula/%d/%d/send.log", drainerEnv->drainerPath_.c_str(), space, partId);

    auto retSend = mockTask->readSendLog(sendLogFile, space, partId);
    EXPECT_TRUE(retSend.ok());
    auto lastLogIdSend = retSend.value();

    auto recvLogFile = folly::stringPrintf(
        "%s/nebula/%d/%d/recv.log", drainerEnv->drainerPath_.c_str(), space, partId);

    auto retRecv = mockTask->readRecvLog(recvLogFile, space, partId);
    EXPECT_TRUE(retRecv.ok());
    auto lastLogIdRecv = retRecv.value();

    EXPECT_EQ(lastLogIdSend, lastLogIdRecv);
  }

  while (taskMgr->tasks_.size() != 0) {
    LOG(INFO) << "wailt all tasks finished, current has " << taskMgr->tasks_.size() << " size.";
    usleep(1000);
  }
  taskMgr->shutdown();
  LOG(INFO) << taskMgr->taskQueue_.size();
  LOG(INFO) << taskMgr->tasks_.size();
  FLAGS_max_concurrent_subdrainertasks = max_concurrent_subdrainertasks;
}

// The directory structure of the data is as follows:
/* |--data/drainer
 * |----nebula
 * |------toSpaceId
 * |--------cluster_space_id (clusterId_spaceId_parts from master and clusterId from slave)
 * |--------partId1(from master space)
 * |----------wal
 * |-------------0000000000000000001.wal
 * |----------recv.log(last_log_id_recv)
 * |----------send.log(last_log_id_sent)
 * |--------partId2(from master space)
 * |----------wal
 * |-------------0000000000000000001.wal
 * |----------recv.log(last_log_id_recv)
 * |----------send.log(last_log_id_sent)
 */
// Use the real directory structure to test. But there is two parts wal log data.
// note: need metaclient
// Use DrainerTaskManager::pool_ to execute multiple subtasks concurrently.
TEST(DrainerTaskTest, PartNumNotSameOnePartTest) {
  FLAGS_heartbeat_interval_secs = 1;
  FLAGS_drainer_task_run_interval = 2;
  auto taskMgr = DummyDrainerTaskManager::instance();
  LOG(INFO) << taskMgr->taskQueue_.size();
  LOG(INFO) << taskMgr->tasks_.size();

  fs::TempDir rootPath("/tmp/drainerTaskTest.XXXXXX");
  mock::MockCluster cluster;
  std::string path = rootPath.path();
  cluster.startMeta(folly::stringPrintf("%s/meta", path.c_str()));

  HostAddr localHost(cluster.localIP(), network::NetworkUtils::getAvailablePort());
  meta::MetaClientOptions options;
  options.localHost_ = localHost;
  options.role_ = meta::cpp2::HostRole::STORAGE;
  cluster.initMetaClient(std::move(options));
  auto* mClient = cluster.metaClient_.get();

  cluster.initStorageKV(path.c_str(), localHost);
  auto* storageEnv = cluster.storageEnv_.get();

  // Construct wal log data of two parts(partId is 1)
  GraphSpaceID space;
  TagID tagId;
  {
    // note: need create tag schema
    sleep(FLAGS_heartbeat_interval_secs + 1);
    auto ret = storageEnv->schemaMan_->toGraphSpaceID("test_space");
    ASSERT_TRUE(ret.ok());
    space = std::move(ret).value();

    std::vector<meta::cpp2::ColumnDef> columns;
    columns.emplace_back();
    columns.back().set_name("name");
    columns.back().type.set_type(PropertyType::STRING);

    columns.emplace_back();
    columns.back().set_name("age");
    columns.back().type.set_type(PropertyType::INT64);

    columns.emplace_back();
    columns.back().set_name("playing");
    columns.back().type.set_type(PropertyType::BOOL);

    columns.emplace_back();
    columns.back().set_name("career");
    columns.back().type.set_type(PropertyType::INT64);

    columns.emplace_back();
    columns.back().set_name("startYear");
    columns.back().type.set_type(PropertyType::INT64);

    columns.emplace_back();
    columns.back().set_name("endYear");
    columns.back().type.set_type(PropertyType::INT64);

    columns.emplace_back();
    columns.back().set_name("games");
    columns.back().type.set_type(PropertyType::INT64);

    columns.emplace_back();
    columns.back().set_name("avgScore");
    columns.back().type.set_type(PropertyType::DOUBLE);

    columns.emplace_back();
    columns.back().set_name("serveTeams");
    columns.back().type.set_type(PropertyType::INT64);

    columns.emplace_back();
    columns.back().set_name("country");
    columns.back().type.set_type(PropertyType::STRING);
    columns.back().set_nullable(true);

    columns.emplace_back();
    columns.back().set_name("champions");
    columns.back().type.set_type(PropertyType::INT64);
    columns.back().set_nullable(true);

    meta::cpp2::Schema schema;
    schema.set_columns(std::move(columns));
    ret = mClient->createTagSchema(space, "player", schema).get();
    ASSERT_TRUE(ret.ok());
    tagId = std::move(ret).value();
  }

  // After creating the schema, need to wait for a while
  sleep(FLAGS_heartbeat_interval_secs + 2);

  auto drainerEnv = std::make_unique<nebula::drainer::DrainerEnv>();
  auto datapath = path + "/data/drainer";
  drainerEnv->drainerPath_ = datapath;
  drainerEnv->schemaMan_ = storageEnv->schemaMan_;
  drainerEnv->indexMan_ = storageEnv->indexMan_;
  drainerEnv->metaClient_ = mClient;

  // The space information(vidLen, vidType) of the master cluster must be consistent
  // with the information of the slave cluster space.
  // Here the information(vidLen, vidType) of the slave cluster space is used as
  // the information of the master cluster space
  auto toSpaceNameRet = drainerEnv->schemaMan_->toGraphSpaceName(space);
  ASSERT_TRUE(toSpaceNameRet.ok());
  auto toSpaceName = toSpaceNameRet.value();

  auto toSpaceVidLenRet = drainerEnv->schemaMan_->getSpaceVidLen(space);
  ASSERT_TRUE(toSpaceVidLenRet.ok());
  auto toSpaceVidLen = toSpaceVidLenRet.value();

  auto toSpaceVidTypeRet = drainerEnv->schemaMan_->getSpaceVidType(space);
  ASSERT_TRUE(toSpaceVidTypeRet.ok());
  auto toSpaceVidType = toSpaceVidTypeRet.value();

  // slave cluster partNum is 6
  // Suppose master cluster partNum is 2
  auto partNum = 2;

  // check wal file in DrainerEnv
  EXPECT_EQ(0, drainerEnv->wals_.size());

  LOG(INFO) << "Test hookable task...";
  folly::Promise<int> p0;
  folly::Promise<int> p1;

  auto f0 = p0.getFuture();
  auto f1 = p1.getFuture();

  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(5);
  std::shared_ptr<DrainerTask> task = std::make_shared<HookableTask>();
  HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());

  size_t totalSubTask = 1;
  std::atomic<int> subTaskCalled{0};

  for (size_t i = 1; i <= totalSubTask; ++i) {
    PartitionID part = i;
    mockTask->addSubTask(
        [&, j = i]() -> nebula::cpp2::ErrorCode {
          LOG(INFO) << "before f0.wait()";
          f0.wait();
          LOG(INFO) << "after f0.wait()";

          // generate part data
          PartitionID partId = j;
          auto* processor = AppendLogProcessor::instance(drainerEnv.get(), nullptr);
          LOG(INFO) << "Build AppendLogRequest...";
          auto reqRet = mockAppendLogWithMetaClientReq(drainerEnv->schemaMan_,
                                                       space,
                                                       toSpaceName,
                                                       partNum,
                                                       partId,
                                                       toSpaceVidLen,
                                                       toSpaceVidType,
                                                       tagId);
          EXPECT_TRUE(reqRet.ok());

          auto req = reqRet.value();
          auto lastRecvLogIdReq = *req.last_log_id_to_send_ref();

          LOG(INFO) << "Test AppendLogProcessor...";
          auto fut = processor->getFuture();
          processor->process(req);
          auto resp = std::move(fut).get();
          EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, *resp.error_code_ref());

          LOG(INFO) << "Check recv.log file ...";
          // build recv log file path
          auto recvLogFile =
              folly::stringPrintf("%s/nebula/%d/%d/recv.log", datapath.c_str(), space, partId);
          auto recvLogIdRet = readRecvLogFile(recvLogFile);
          EXPECT_TRUE(recvLogIdRet.ok());
          auto recvLogId = recvLogIdRet.value();

          EXPECT_EQ(lastRecvLogIdReq, recvLogId);

          // build context & run subtask
          nebula::cpp2::ErrorCode retCode =
              mockTask->addTaskForPart(drainerEnv.get(), space, ioThreadPool, partId);
          ++subTaskCalled;

          LOG(INFO) << "before p1.setValue()";
          p1.setValue(1);
          LOG(INFO) << "after p1.setValue()";

          return retCode;
        },
        part);
  }

  // hookable task
  taskMgr->init(drainerEnv.get(), ioThreadPool);

  // After addAsyncTask, tasks_ in DrainerTaskManager is not empty,
  // so addAsyncTask is not executed.
  // Until tasks_ is empty, addAsyncTask is executed again.
  taskMgr->addAsyncTask(space, task);

  auto retVal = taskMgr->run();
  EXPECT_TRUE(retVal);
  // sleep for a period of time to ensure that the child thread has been executed.
  sleep(FLAGS_drainer_task_run_interval * 2);

  LOG(INFO) << "wait drainer task finished.";
  LOG(INFO) << "before p0.setValue()";
  p0.setValue(1);
  LOG(INFO) << "after p0.setValue()";
  LOG(INFO) << "before f1.wait()";
  f1.wait();
  LOG(INFO) << "after f1.wait()";

  while (!taskMgr->isFinished(space)) {
    usleep(1000);
  }
  sleep(1);
  EXPECT_EQ(subTaskCalled, totalSubTask);

  // check wal file in DrainerEnv
  EXPECT_EQ(1, drainerEnv->wals_.size());
  EXPECT_TRUE(drainerEnv->wals_.find(space) != drainerEnv->wals_.end());
  EXPECT_EQ(1, drainerEnv->wals_.find(space)->second.size());

  // check sendLog file and revcLog file
  // read sendLog file
  LOG(INFO) << "Read sendLog file...";
  for (size_t i = 1; i <= totalSubTask; i++) {
    PartitionID partId = i;
    auto sendLogFile = folly::stringPrintf(
        "%s/nebula/%d/%d/send.log", drainerEnv->drainerPath_.c_str(), space, partId);

    auto retSend = mockTask->readSendLog(sendLogFile, space, partId);
    EXPECT_TRUE(retSend.ok());
    auto lastLogIdSend = retSend.value();

    auto recvLogFile = folly::stringPrintf(
        "%s/nebula/%d/%d/recv.log", drainerEnv->drainerPath_.c_str(), space, partId);

    auto retRecv = mockTask->readRecvLog(recvLogFile, space, partId);
    EXPECT_TRUE(retRecv.ok());
    auto lastLogIdRecv = retRecv.value();

    EXPECT_EQ(lastLogIdSend, lastLogIdRecv);
  }

  taskMgr->shutdown();
  LOG(INFO) << taskMgr->taskQueue_.size();
  LOG(INFO) << taskMgr->tasks_.size();
}

// The directory structure of the data is as follows:
/* |--data/drainer
 * |----nebula
 * |------toSpaceId
 * |--------cluster_space_id (clusterId_spaceId_parts from master and clusterId from slave)
 * |--------partId1(from master space)
 * |----------wal
 * |-------------0000000000000000001.wal
 * |----------recv.log(last_log_id_recv)
 * |----------send.log(last_log_id_sent)
 * |--------partId2(from master space)
 * |----------wal
 * |-------------0000000000000000001.wal
 * |----------recv.log(last_log_id_recv)
 * |----------send.log(last_log_id_sent)
 */
// Use the real directory structure to test. But there is two parts wal log data.
// note: need metaclient
TEST(DrainerTaskTest, PartNumNotSameMultiPartTest) {
  FLAGS_heartbeat_interval_secs = 1;
  auto taskMgr = DummyDrainerTaskManager::instance();
  LOG(INFO) << taskMgr->taskQueue_.size();
  LOG(INFO) << taskMgr->tasks_.size();

  fs::TempDir rootPath("/tmp/drainerTaskTest.XXXXXX");
  mock::MockCluster cluster;
  std::string path = rootPath.path();
  cluster.startMeta(folly::stringPrintf("%s/meta", path.c_str()));

  HostAddr localHost(cluster.localIP(), network::NetworkUtils::getAvailablePort());
  meta::MetaClientOptions options;
  options.localHost_ = localHost;
  options.role_ = meta::cpp2::HostRole::STORAGE;
  cluster.initMetaClient(std::move(options));
  auto* mClient = cluster.metaClient_.get();

  cluster.initStorageKV(path.c_str(), localHost);
  auto* storageEnv = cluster.storageEnv_.get();

  // Construct wal log data of two parts(partId is 1)
  GraphSpaceID space;
  TagID tagId;
  {
    // note: need create tag schema
    sleep(FLAGS_heartbeat_interval_secs + 1);
    auto ret = storageEnv->schemaMan_->toGraphSpaceID("test_space");
    ASSERT_TRUE(ret.ok());
    space = std::move(ret).value();

    std::vector<meta::cpp2::ColumnDef> columns;
    columns.emplace_back();
    columns.back().set_name("name");
    columns.back().type.set_type(PropertyType::STRING);

    columns.emplace_back();
    columns.back().set_name("age");
    columns.back().type.set_type(PropertyType::INT64);

    columns.emplace_back();
    columns.back().set_name("playing");
    columns.back().type.set_type(PropertyType::BOOL);

    columns.emplace_back();
    columns.back().set_name("career");
    columns.back().type.set_type(PropertyType::INT64);

    columns.emplace_back();
    columns.back().set_name("startYear");
    columns.back().type.set_type(PropertyType::INT64);

    columns.emplace_back();
    columns.back().set_name("endYear");
    columns.back().type.set_type(PropertyType::INT64);

    columns.emplace_back();
    columns.back().set_name("games");
    columns.back().type.set_type(PropertyType::INT64);

    columns.emplace_back();
    columns.back().set_name("avgScore");
    columns.back().type.set_type(PropertyType::DOUBLE);

    columns.emplace_back();
    columns.back().set_name("serveTeams");
    columns.back().type.set_type(PropertyType::INT64);

    columns.emplace_back();
    columns.back().set_name("country");
    columns.back().type.set_type(PropertyType::STRING);
    columns.back().set_nullable(true);

    columns.emplace_back();
    columns.back().set_name("champions");
    columns.back().type.set_type(PropertyType::INT64);
    columns.back().set_nullable(true);

    meta::cpp2::Schema schema;
    schema.set_columns(std::move(columns));
    ret = mClient->createTagSchema(space, "player", schema).get();
    ASSERT_TRUE(ret.ok());
    tagId = std::move(ret).value();
  }

  // After creating the schema, need to wait for a while
  sleep(FLAGS_heartbeat_interval_secs + 2);

  auto drainerEnv = std::make_unique<nebula::drainer::DrainerEnv>();
  auto datapath = path + "/data/drainer";
  drainerEnv->drainerPath_ = datapath;
  drainerEnv->schemaMan_ = storageEnv->schemaMan_;
  drainerEnv->indexMan_ = storageEnv->indexMan_;
  drainerEnv->metaClient_ = mClient;

  // The space information(vidLen, vidType) of the master cluster must be consistent
  // with the information of the slave cluster space.
  // Here the information(vidLen, vidType) of the slave cluster space is used as
  // the information of the master cluster space
  auto toSpaceNameRet = drainerEnv->schemaMan_->toGraphSpaceName(space);
  ASSERT_TRUE(toSpaceNameRet.ok());
  auto toSpaceName = toSpaceNameRet.value();

  auto toSpaceVidLenRet = drainerEnv->schemaMan_->getSpaceVidLen(space);
  ASSERT_TRUE(toSpaceVidLenRet.ok());
  auto toSpaceVidLen = toSpaceVidLenRet.value();

  auto toSpaceVidTypeRet = drainerEnv->schemaMan_->getSpaceVidType(space);
  ASSERT_TRUE(toSpaceVidTypeRet.ok());
  auto toSpaceVidType = toSpaceVidTypeRet.value();

  auto max_concurrent_subdrainertasks = FLAGS_max_concurrent_subdrainertasks;
  FLAGS_max_concurrent_subdrainertasks = 10;
  FLAGS_drainer_task_run_interval = 2;

  // slave cluster partNum is 6
  // Suppose master cluster partNum is 2
  auto partNum = 2;

  // check wal file in DrainerEnv
  EXPECT_EQ(0, drainerEnv->wals_.size());

  LOG(INFO) << "Test hookable task...";
  folly::Promise<int> p0;
  folly::Promise<int> p1;
  folly::Promise<int> p2;

  auto f0 = p0.getFuture();
  auto f1 = p1.getFuture();
  auto f2 = p2.getFuture();

  std::shared_ptr<DrainerTask> task = std::make_shared<HookableTask>();
  HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(5);

  size_t totalSubTask = 2;
  std::atomic<int> subTaskCalled{0};

  for (size_t i = 1; i <= totalSubTask; ++i) {
    PartitionID part = i;
    mockTask->addSubTask(
        [&, j = i]() -> nebula::cpp2::ErrorCode {
          if (j == 1) {
            LOG(INFO) << "before f0.wait()";
            f0.wait();
            LOG(INFO) << "after f0.wait()";
          } else {
            LOG(INFO) << "before f1.wait()";
            f1.wait();
            LOG(INFO) << "after f1.wait()";
          }

          // generate part data
          PartitionID partId = j;
          auto* processor = AppendLogProcessor::instance(drainerEnv.get(), nullptr);
          LOG(INFO) << "Build AppendLogRequest...";
          auto reqRet = mockAppendLogWithMetaClientReq(drainerEnv->schemaMan_,
                                                       space,
                                                       toSpaceName,
                                                       partNum,
                                                       partId,
                                                       toSpaceVidLen,
                                                       toSpaceVidType,
                                                       tagId);
          EXPECT_TRUE(reqRet.ok());

          auto req = reqRet.value();
          auto lastRecvLogIdReq = *req.last_log_id_to_send_ref();

          LOG(INFO) << "Test AppendLogProcessor...";
          auto fut = processor->getFuture();
          processor->process(req);
          auto resp = std::move(fut).get();
          EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, *resp.error_code_ref());

          LOG(INFO) << "Check recv.log file ...";
          // build recv log file path
          auto recvLogFile =
              folly::stringPrintf("%s/nebula/%d/%d/recv.log", datapath.c_str(), space, partId);
          auto recvLogIdRet = readRecvLogFile(recvLogFile);
          EXPECT_TRUE(recvLogIdRet.ok());
          auto recvLogId = recvLogIdRet.value();

          EXPECT_EQ(lastRecvLogIdReq, recvLogId);

          // build context & run subtask
          nebula::cpp2::ErrorCode retCode =
              mockTask->addTaskForPart(drainerEnv.get(), space, ioThreadPool, partId);
          ++subTaskCalled;
          if (j == 1) {
            LOG(INFO) << "before p1.setValue()";
            p1.setValue(1);
            LOG(INFO) << "after p1.setValue()";
          } else {
            LOG(INFO) << "before p2.setValue()";
            p2.setValue(1);
            LOG(INFO) << "after p2.setValue()";
          }
          return retCode;
        },
        part);
  }

  // hookable task
  taskMgr->init(drainerEnv.get(), ioThreadPool);

  // After addAsyncTask, tasks_ in DrainerTaskManager is not empty,
  // so addAsyncTask is not executed.
  // Until tasks_ is empty, addAsyncTask is executed again.
  taskMgr->addAsyncTask(space, task);

  auto retVal = taskMgr->run();
  EXPECT_TRUE(retVal);

  // sleep for a period of time to ensure that the child thread has been executed.
  sleep(FLAGS_drainer_task_run_interval * 2);

  LOG(INFO) << "wait drainer task finished.";
  LOG(INFO) << "before p0.setValue()";
  p0.setValue(1);
  LOG(INFO) << "after p0.setValue()";
  LOG(INFO) << "before f2.wait()";
  f2.wait();
  LOG(INFO) << "after f2.wait()";

  while (!taskMgr->isFinished(space)) {
    usleep(1000);
  }
  sleep(1);
  EXPECT_EQ(subTaskCalled, totalSubTask);

  // check wal file in DrainerEnv
  EXPECT_EQ(1, drainerEnv->wals_.size());
  EXPECT_TRUE(drainerEnv->wals_.find(space) != drainerEnv->wals_.end());
  EXPECT_EQ(2, drainerEnv->wals_.find(space)->second.size());

  // check sendLog file and revcLog file
  // read sendLog file
  LOG(INFO) << "Read sendLog file...";
  for (size_t i = 1; i <= totalSubTask; i++) {
    PartitionID partId = i;
    auto sendLogFile = folly::stringPrintf(
        "%s/nebula/%d/%d/send.log", drainerEnv->drainerPath_.c_str(), space, partId);

    auto retSend = mockTask->readSendLog(sendLogFile, space, partId);
    EXPECT_TRUE(retSend.ok());
    auto lastLogIdSend = retSend.value();

    auto recvLogFile = folly::stringPrintf(
        "%s/nebula/%d/%d/recv.log", drainerEnv->drainerPath_.c_str(), space, partId);

    auto retRecv = mockTask->readRecvLog(recvLogFile, space, partId);
    EXPECT_TRUE(retRecv.ok());
    auto lastLogIdRecv = retRecv.value();

    EXPECT_EQ(lastLogIdSend, lastLogIdRecv);
  }

  taskMgr->shutdown();
  LOG(INFO) << taskMgr->taskQueue_.size();
  LOG(INFO) << taskMgr->tasks_.size();
  FLAGS_max_concurrent_subdrainertasks = max_concurrent_subdrainertasks;
}

}  // namespace drainer
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
