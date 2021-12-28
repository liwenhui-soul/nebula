/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "drainer/DrainerTaskManager.h"

#include "common/fs/FileUtils.h"
#include "drainer/DrainerFlags.h"
#include "kvstore/wal/FileBasedWal.h"

DECLARE_int32(wal_ttl);
DECLARE_int64(wal_file_size);
DECLARE_int32(wal_buffer_size);
DECLARE_bool(wal_sync);
DECLARE_int32(cluster_id);

namespace nebula {
namespace drainer {

bool DrainerTaskManager::init(DrainerEnv* env,
                              std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool) {
  CHECK_NOTNULL(env);
  env_ = env;
  ioThreadPool_ = ioThreadPool,
  interClient_ = std::make_unique<storage::InternalStorageClient>(ioThreadPool_, env_->metaClient_);

  LOG(INFO) << "Max concurrenct sub drainer task: " << FLAGS_max_concurrent_subdrainertasks;
  pool_ = std::make_unique<ThreadPool>(FLAGS_max_concurrent_subdrainertasks);
  bgThread_ = std::make_unique<thread::GenericWorker>();
  if (!bgThread_->start()) {
    LOG(ERROR) << "Background thread start failed";
    return false;
  }
  shutdown_.store(false, std::memory_order_release);
  bgThread_->addTask(&DrainerTaskManager::schedule, this);
  LOG(INFO) << "Exit DrainerTaskManager::init()";
  return true;
}

// schedule
void DrainerTaskManager::schedule() {
  std::chrono::milliseconds interval{20};  // 20ms
  while (!shutdown_.load(std::memory_order_acquire)) {
    // Judge the start of the next round by the number of tasks in tasks_.
    if (tasks_.empty()) {
      // Each round creates tasks by traversing the space directory of data/drainer/nebula
      // Init drainer task, each space is one task
      // sleep for a period of time to ensure that a batch of data is processed.
      sleep(FLAGS_drainer_task_run_interval);
      auto ret = addAsyncTask();
      if (!ret.ok()) {
        VLOG(3) << "Init drainer tasks failed in current round " << ret.toString();
        continue;
      }
    }

    folly::Optional<GraphSpaceID> optTaskHandle{folly::none};
    while (!optTaskHandle && !shutdown_.load(std::memory_order_acquire) && taskQueue_.size() != 0) {
      // Because the subtask of each drainer task is executed concurrently
      // and asynchronously. So there is a case:
      // It was not empty when tasks_.empty() was judged earlier.
      // But here, it is judged that tasks_ is already empty and taskQueue_ is also empty.
      optTaskHandle = taskQueue_.try_take_for(interval);
    }

    if (shutdown_.load(std::memory_order_acquire)) {
      LOG(INFO) << "detect DrainerTaskManager::shutdown()";
      return;
    }
    if (!optTaskHandle) {
      continue;
    }

    auto spaceId = *optTaskHandle;
    VLOG(3) << folly::stringPrintf("dequeue drainer task(%d)", spaceId);
    auto it = tasks_.find(spaceId);
    if (it == tasks_.end()) {
      LOG(ERROR) << folly::stringPrintf("Trying to exec non-exist drainer task(%d)", spaceId);
      continue;
    }
    VLOG(3) << apache::thrift::util::enumNameSafe(tasks_[spaceId]->rc_.load());

    auto drainerTask = it->second;
    auto statusCode = drainerTask->status();
    if (statusCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      // remove cancelled tasks
      LOG(INFO) << folly::sformat("Remove drainer task {} genSubTask failed, err={}",
                                  spaceId,
                                  apache::thrift::util::enumNameSafe(statusCode));
      tasks_.erase(spaceId);
      continue;
    }

    VLOG(3) << "Waiting for incoming drainer task";
    auto errOrSubTasks = drainerTask->genSubTasks();
    if (!nebula::ok(errOrSubTasks)) {
      LOG(ERROR) << folly::sformat(
          "Drainer task {} genSubTask failed, err={}",
          spaceId,
          apache::thrift::util::enumNameSafe(nebula::error(errOrSubTasks)));
      drainerTask->finish(nebula::error(errOrSubTasks));
      tasks_.erase(spaceId);
      continue;
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
      continue;
    }

    VLOG(3) << folly::stringPrintf("Run drainer task(%d), %zu subtasks in %zu thread",
                                   spaceId,
                                   drainerTask->unFinishedSubTask_.load(),
                                   subTaskConcurrency);
    for (size_t i = 0; i < subTaskConcurrency; ++i) {
      // run task
      pool_->add(std::bind(&DrainerTaskManager::runSubTask, this, spaceId));
    }
  }  // end while (!shutdown_)
  LOG(INFO) << "DrainerTaskManager::pickTaskThread(~)";
}

void DrainerTaskManager::addAsyncTask(GraphSpaceID spaceId, std::shared_ptr<DrainerTask> task) {
  LOG(INFO) << taskQueue_.size();
  LOG(INFO) << tasks_.size();
  taskQueue_.add(spaceId);
  auto ret = tasks_.insert(spaceId, task).second;
  DCHECK(ret);
  LOG(INFO) << apache::thrift::util::enumNameSafe(tasks_[spaceId]->rc_.load());
}

// The directory structure of the data is as follows:
/* |--data/drainer
 * |----nebula
 * |------toSpaceId
 * |--------cluster_space_id (clusterId_spaceId_parts from master and clusterId from slave)
 * |--------partId1(from master space)
 * |----------wal
 * |----------recv.log(last_log_id_recv)
 * |----------send.log(last_log_id_sent)
 * |--------partId2
 */
Status DrainerTaskManager::addAsyncTask() {
  auto dataPath = env_->drainerPath_ + "/nebula";
  if (!fs::FileUtils::exist(dataPath)) {
    return Status::Error("Drainer data path '%s' not exists.", dataPath.c_str());
  }

  auto spaceDirs = fs::FileUtils::listAllDirsInDir(dataPath.c_str());
  for (auto& sdir : spaceDirs) {
    VLOG(3) << "Scan path \"" << dataPath << "/" << sdir << "\"";
    try {
      GraphSpaceID spaceId;
      auto spaceDir = folly::stringPrintf("%s/%s", dataPath.c_str(), sdir.c_str());
      try {
        spaceId = folly::to<GraphSpaceID>(sdir);
      } catch (const std::exception& ex) {
        LOG(ERROR) << "Data path " << spaceDir << " invalid: " << ex.what();
        continue;
      }

      // check space exists
      auto spaceNameRet = env_->schemaMan_->toGraphSpaceName(spaceId);
      if (!spaceNameRet.ok()) {
        LOG(ERROR) << "Space id " << spaceId << " no found";
        // remove invalid sync listener space data
        if (FLAGS_auto_remove_invalid_drainer_space) {
          removeSpaceDir(spaceDir);
        }
        continue;
      }

      // load space meta info
      auto ret = loadSpaceMeta(spaceDir, spaceId);
      if (!ret.ok()) {
        LOG(ERROR) << "Load space " << spaceId << " meta failed.";
        return ret;
      }

      // list all part dirs
      auto partDirs = fs::FileUtils::listAllDirsInDir(spaceDir.c_str());
      for (auto& pdir : partDirs) {
        PartitionID partId;
        auto partDir = folly::stringPrintf("%s/%s", spaceDir.c_str(), pdir.c_str());
        try {
          partId = folly::to<PartitionID>(pdir);
        } catch (const std::exception& ex) {
          return Status::Error("Data path %s invalid %s", partDir.c_str(), ex.what());
        }

        // insert into DrainerEnv.wals_
        auto spaceIter = env_->wals_.find(spaceId);
        if (spaceIter != env_->wals_.end()) {
          auto partIter = spaceIter->second.find(partId);
          if (partIter != spaceIter->second.end()) {
            continue;
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
        env_->wals_[spaceId].emplace(
            partId,
            wal::FileBasedWal::getWal(
                walPath,
                std::move(info),
                std::move(policy),
                [this](
                    LogID logId, TermID logTermId, ClusterID logClusterId, const std::string& log) {
                  return this->preProcessLog(logId, logTermId, logClusterId, log);
                },
                env_->diskMan_,
                true));
      }
    } catch (std::exception& e) {
      LOG(FATAL) << "Invalid data directory \"" << sdir << "\"";
    }  // end try
  }

  // create drainer task
  for (auto elem : env_->wals_) {
    auto spaceId = elem.first;
    auto iter = env_->spaceClusters_.find(spaceId);
    auto cluster = iter->second;
    auto masterClusterId = cluster.first;
    auto slaveClusterId = cluster.second;

    auto task = DrainerTaskFactory::createDrainerTask(
        env_, spaceId, interClient_.get(), ioThreadPool_, masterClusterId, slaveClusterId);
    CHECK_NOTNULL(task);
    auto ret = tasks_.insert(spaceId, task).second;
    DCHECK(ret);
    taskQueue_.add(spaceId);
  }
  return Status::OK();
}

Status DrainerTaskManager::loadSpaceMeta(std::string& spaceDir, GraphSpaceID toSpaceId) {
  auto spaceIter = env_->spaceMatch_.find(toSpaceId);

  // Get from cache
  if (spaceIter != env_->spaceMatch_.end()) {
    auto clustetIter = env_->spaceClusters_.find(toSpaceId);
    auto oldPartNumIter = env_->spaceOldParts_.find(toSpaceId);
    if (clustetIter == env_->spaceClusters_.end() || oldPartNumIter == env_->spaceOldParts_.end()) {
      LOG(ERROR) << "Not find space id " << toSpaceId
                 << " in spaceClusters or spaceOldParts of env.";
      return Status::Error("Get space %d meta failed.", toSpaceId);
    }
  } else {
    ClusterID fromClusterId;
    GraphSpaceID fromSpaceId;
    int32_t fromPartNum;
    ClusterID toClusterId;
    auto spaceFile = folly::stringPrintf("%s/cluster_space_id", spaceDir.c_str());

    auto spaceMetaRet = DrainerCommon::readSpaceMeta(spaceFile);
    if (!spaceMetaRet.ok()) {
      LOG(ERROR) << spaceMetaRet.status();
      return spaceMetaRet.status();
    }
    std::tie(fromClusterId, fromSpaceId, fromPartNum, toClusterId) = spaceMetaRet.value();
    env_->spaceMatch_.emplace(toSpaceId, fromSpaceId);
    env_->spaceClusters_.emplace(toSpaceId, std::make_pair(fromClusterId, toClusterId));
    env_->spaceOldParts_.emplace(toSpaceId, fromPartNum);
  }

  auto newIter = env_->spaceNewParts_.find(toSpaceId);
  if (newIter == env_->spaceNewParts_.end()) {
    // Get new parts from schema
    auto partRet = env_->schemaMan_->getPartsNum(toSpaceId);
    if (!partRet.ok()) {
      LOG(ERROR) << "Get space partition number failed from schema";
      return partRet.status();
    }
    auto parts = partRet.value();
    env_->spaceNewParts_.emplace(toSpaceId, parts);
  }
  return Status::OK();
}

// For the continuity of logId, do nothing
bool DrainerTaskManager::preProcessLog(LogID logId,
                                       TermID termId,
                                       ClusterID clusterId,
                                       const std::string& log) {
  UNUSED(logId);
  UNUSED(termId);
  UNUSED(clusterId);
  UNUSED(log);
  return true;
}

void DrainerTaskManager::runSubTask(GraphSpaceID spaceId) {
  auto it = tasks_.find(spaceId);
  if (it == tasks_.cend()) {
    FLOG_INFO("task(%d) runSubTask() exit", spaceId);
    return;
  }
  auto task = it->second;
  std::chrono::milliseconds take_dura{10};
  if (auto subTask = task->subtasks_.try_take_for(take_dura)) {
    // If one of the currently executed subtasks fails,
    // no new subtasks will be executed later.
    if (task->status() == nebula::cpp2::ErrorCode::SUCCEEDED) {
      auto rc = nebula::cpp2::ErrorCode::E_UNKNOWN;
      try {
        rc = subTask->invoke();
      } catch (std::exception& ex) {
        LOG(ERROR) << folly::sformat("task({}) invoke() throw exception: {}", spaceId, ex.what());
      } catch (...) {
        LOG(ERROR) << folly::sformat("task({}) invoke() throw unknown exception", spaceId);
      }
      task->subTaskFinish(rc, subTask->part());
    }

    auto unFinishedSubTask = --task->unFinishedSubTask_;
    VLOG(3) << folly::stringPrintf("subtask of task(%d, %d) finished, unfinished task %zu",
                                   spaceId,
                                   subTask->part(),
                                   unFinishedSubTask);
    if (0 == unFinishedSubTask) {
      task->finish();
      tasks_.erase(spaceId);
    } else {
      pool_->add(std::bind(&DrainerTaskManager::runSubTask, this, spaceId));
    }
  } else {
    FLOG_INFO("drainer task(%d) runSubTask() exit", spaceId);
  }
}

// TODO for stop drainer data to storage
nebula::cpp2::ErrorCode DrainerTaskManager::cancelTask(GraphSpaceID spaceId) {
  auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
  auto it = tasks_.find(spaceId);
  if (it == tasks_.cend()) {
    ret = nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND;
  } else {
    it->second->cancel();
  }
  return ret;
}

void DrainerTaskManager::shutdown() {
  LOG(INFO) << "Enter DrainerTaskManager::shutdown()";
  shutdown_.store(true, std::memory_order_release);
  bgThread_->stop();
  bgThread_->wait();

  for (auto it = tasks_.begin(); it != tasks_.end(); ++it) {
    it->second->cancel();  // cancelled_ = true;
  }

  pool_->join();
  LOG(INFO) << "Exit DrainerTaskManager::shutdown()";
}

bool DrainerTaskManager::isFinished(GraphSpaceID spaceId) {
  auto iter = tasks_.find(spaceId);
  // Task maybe erased when it's finished.
  if (iter == tasks_.cend()) {
    return true;
  }
  return iter->second->unFinishedSubTask_ == 0;
}

void DrainerTaskManager::removeSpaceDir(const std::string& dir) {
  try {
    LOG(INFO) << "Try to remove space directory: " << dir;
    boost::filesystem::remove_all(dir);
  } catch (const boost::filesystem::filesystem_error& e) {
    LOG(ERROR) << "Exception caught while remove directory, please delelte it "
                  "by manual: "
               << e.what();
  }
}

}  // namespace drainer
}  // namespace nebula
