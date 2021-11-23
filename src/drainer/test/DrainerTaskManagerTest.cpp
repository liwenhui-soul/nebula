/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/executors/IOThreadPoolExecutor.h>
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"
#include "drainer/CommonUtils.h"
#include "drainer/DrainerFlags.h"
#include "drainer/DrainerTaskManager.h"
#include "drainer/test/TestUtils.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"

namespace nebula {
namespace drainer {

using nebula::fs::FileType;
using nebula::fs::FileUtils;

/*
 * summary:
 * 0. validation for some basic data structure and component
 * 1. ctor
 * 2. happy_path:
 *      use 3 background thread to run 1, 2, 4, 8 sub tasks
 * 3. gen task failed directly
 *      check task manger will return the right error code
 * 4. some task return error code.
 *      check task manger will return the right error code
 * 5. cancel task in task queue
 *      5.1 cancel_a_running_task_with_only_1_sub_task
 *      5.2 cancel_1_task_in_a_2_tasks_queue
 * 6. cancel some sub task
 *      6.1 cancel_a_task_before_all_sub_task_running
 *      6.2 cancel_a_task_while_some_sub_task_running
 * */

using ErrOrSubTasks = ErrorOr<nebula::cpp2::ErrorCode, std::vector<DrainerSubTask>>;

auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;

struct HookableTask : public DrainerTask {
  HookableTask() {
    fGenSubTasks = [&]() { return subTasks; };
  }
  ErrOrSubTasks genSubTasks() override {
    LOG(INFO) << "HookableTask::genSubTasks() subTasks.size()=" << subTasks.size();
    return fGenSubTasks();
  }

  void addSubTask(std::function<nebula::cpp2::ErrorCode()> subTask, PartitionID part) {
    subTasks.emplace_back(subTask, part);
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

  // schedule, only execute one drainer task once.
  // And use the main thread to execute.
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

// *** 0. basic component
TEST(DrainerTaskManagerTest, extract_subtasks_to_context) {
  size_t numSubTask = 5;
  auto vtask = std::make_shared<HookableTask>();
  HookableTask* task = static_cast<HookableTask*>(vtask.get());
  std::atomic<int> subTaskCalled{0};
  for (size_t i = 0; i < numSubTask; ++i) {
    PartitionID part = i;
    task->addSubTask(
        [&subTaskCalled]() {
          ++subTaskCalled;
          return nebula::cpp2::ErrorCode::SUCCEEDED;
        },
        part);
  }
  for (auto& subtask : task->subTasks) {
    vtask->subtasks_.add(subtask);
  }

  std::chrono::milliseconds ms10{10};
  while (auto it = vtask->subtasks_.try_take_for(ms10)) {
    auto& subTask = *it;
    subTask.invoke();
  }

  EXPECT_EQ(subTaskCalled, numSubTask);
}

TEST(DrainerTaskManagerTest, data_structure_ConcurrentHashMap) {
  using TTask = std::shared_ptr<DrainerTask>;
  using TaskContainer = folly::ConcurrentHashMap<GraphSpaceID, TTask>;

  // CRUD
  // create
  std::shared_ptr<DrainerTask> t1 = std::make_shared<HookableTask>();
  TaskContainer tasks;
  GraphSpaceID space = 1;
  EXPECT_TRUE(tasks.empty());
  EXPECT_EQ(tasks.find(space), tasks.cend());
  auto r = tasks.insert(space, t1);
  EXPECT_TRUE(r.second);

  // read
  auto cit = tasks.find(space);

  // update
  auto newTask = tasks[space];
  newTask = t1;

  // delete
  auto nRemove = tasks.erase(space);
  EXPECT_EQ(1, nRemove);
  EXPECT_TRUE(tasks.empty());
}

// 1. ctor
TEST(DrainerTaskManagerTest, ctor) {
  auto taskMgr = DummyDrainerTaskManager::instance();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());

  auto env = std::make_unique<nebula::drainer::DrainerEnv>();
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(5);
  taskMgr->init(env.get(), ioThreadPool);
  taskMgr->shutdown();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());
}

// one subDrainerTask
TEST(DrainerTaskManagerTest, happy_path_task1_sub1) {
  FLAGS_max_concurrent_subdrainertasks = 10;
  auto taskMgr = DummyDrainerTaskManager::instance();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());

  auto env = std::make_unique<nebula::drainer::DrainerEnv>();
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(5);
  taskMgr->init(env.get(), ioThreadPool);

  size_t numSubTask = 1;
  GraphSpaceID space = 1;
  std::shared_ptr<DrainerTask> task = std::make_shared<HookableTask>();
  HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());

  std::atomic<int> subTaskCalled{0};

  auto subTask = [&subTaskCalled]() {
    ++subTaskCalled;
    LOG(INFO) << "subTask invoke()";
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  };

  for (size_t i = 0; i < numSubTask; ++i) {
    PartitionID part = i;
    mockTask->addSubTask(subTask, part);
  }

  taskMgr->addAsyncTask(space, task);
  auto retVal = taskMgr->run();
  EXPECT_TRUE(retVal);

  LOG(INFO) << "wait";
  while (!taskMgr->isFinished(space)) {
    usleep(1000);
  }
  sleep(1);

  EXPECT_EQ(numSubTask, subTaskCalled);
  taskMgr->shutdown();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());
}

// 256 subDrainerTask
TEST(DrainerTaskManagerTest, run_a_medium_task_before_a_huge_task) {
  FLAGS_max_concurrent_subdrainertasks = 10;
  auto env = std::make_unique<nebula::drainer::DrainerEnv>();
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(5);
  auto taskMgr = DummyDrainerTaskManager::instance();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());

  taskMgr->init(env.get(), ioThreadPool);

  {  // 256 sub tasks
    size_t numSubTask = 256;
    GraphSpaceID space = 1;
    std::shared_ptr<DrainerTask> task = std::make_shared<HookableTask>();
    HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());

    std::atomic<int> subTaskCalled{0};
    auto subTask = [&subTaskCalled]() {
      ++subTaskCalled;
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    };
    for (size_t i = 0; i < numSubTask; ++i) {
      PartitionID part = i;
      mockTask->addSubTask(subTask, part);
    }
    taskMgr->addAsyncTask(space, task);
    auto retVal = taskMgr->run();
    EXPECT_TRUE(retVal);

    while (!taskMgr->isFinished(space)) {
      usleep(1000);
    }
    sleep(1);

    EXPECT_EQ(numSubTask, subTaskCalled);
  }
  taskMgr->shutdown();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());
}

TEST(DrainerTaskManagerTest, happy_path) {
  FLAGS_max_concurrent_subdrainertasks = 10;
  auto env = std::make_unique<nebula::drainer::DrainerEnv>();
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(5);
  auto taskMgr = DummyDrainerTaskManager::instance();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());

  taskMgr->init(env.get(), ioThreadPool);
  GraphSpaceID space = 1;

  {
    // use FLAGS_max_concurrent_subdrainertasks = 10, run 1 subDrainerTask
    size_t numSubTask = 1;
    std::shared_ptr<DrainerTask> task = std::make_shared<HookableTask>();
    HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
    std::atomic<int> subTaskCalled{0};

    auto subTask = [&subTaskCalled]() {
      ++subTaskCalled;
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    };
    for (size_t i = 0; i < numSubTask; ++i) {
      PartitionID part = i;
      mockTask->addSubTask(subTask, part);
    }
    taskMgr->addAsyncTask(space, task);
    auto retVal = taskMgr->run();
    EXPECT_TRUE(retVal);

    LOG(INFO) << "wait";
    while (!taskMgr->isFinished(space)) {
      usleep(1000);
    }
    sleep(1);

    EXPECT_EQ(numSubTask, subTaskCalled);
  }

  {  // use 10 background to run 2 subDrainerTask
    size_t numSubTask = 2;
    std::shared_ptr<DrainerTask> task = std::make_shared<HookableTask>();
    HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
    std::atomic<int> subTaskCalled{0};

    auto subTask = [&subTaskCalled]() {
      ++subTaskCalled;
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    };
    for (size_t i = 0; i < numSubTask; ++i) {
      PartitionID part = i;
      mockTask->addSubTask(subTask, part);
    }
    taskMgr->addAsyncTask(space, task);
    auto retVal = taskMgr->run();
    EXPECT_TRUE(retVal);

    while (!taskMgr->isFinished(space)) {
      usleep(1000);
    }
    sleep(1);
    ASSERT_EQ(numSubTask, subTaskCalled);
  }

  {  // use 10 background to run 10 subDrainerTask
    size_t numSubTask = 10;
    std::shared_ptr<DrainerTask> task = std::make_shared<HookableTask>();
    HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());

    std::atomic<int> subTaskCalled{0};

    auto subTask = [&subTaskCalled]() {
      ++subTaskCalled;
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    };
    for (size_t i = 0; i < numSubTask; ++i) {
      PartitionID part = i;
      mockTask->addSubTask(subTask, part);
    }
    taskMgr->addAsyncTask(space, task);
    auto retVal = taskMgr->run();
    EXPECT_TRUE(retVal);

    while (!taskMgr->isFinished(space)) {
      usleep(1000);
    }
    sleep(1);

    EXPECT_EQ(numSubTask, subTaskCalled);
  }

  {  // use 10 background to run 15 drainer task
    size_t numSubTask = 15;
    std::shared_ptr<DrainerTask> task = std::make_shared<HookableTask>();
    HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());

    std::atomic<int> subTaskCalled{0};
    auto subTask = [&subTaskCalled]() {
      ++subTaskCalled;
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    };
    for (size_t i = 0; i < numSubTask; ++i) {
      PartitionID part = i;
      mockTask->addSubTask(subTask, part);
    }
    taskMgr->addAsyncTask(space, task);
    auto retVal = taskMgr->run();
    EXPECT_TRUE(retVal);

    LOG(INFO) << "wait";
    while (!taskMgr->isFinished(space)) {
      usleep(1000);
    }
    sleep(1);

    EXPECT_EQ(numSubTask, subTaskCalled);
  }

  {  // 20 subDrainerTask
    size_t numSubTask = 20;
    std::shared_ptr<DrainerTask> task = std::make_shared<HookableTask>();
    HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());
    std::atomic<int> subTaskCalled{0};
    auto subTask = [&subTaskCalled]() {
      ++subTaskCalled;
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    };
    for (size_t i = 0; i < numSubTask; ++i) {
      PartitionID part = i;
      mockTask->addSubTask(subTask, part);
    }
    taskMgr->addAsyncTask(space, task);
    auto retVal = taskMgr->run();
    EXPECT_TRUE(retVal);

    LOG(INFO) << "wait";
    while (!taskMgr->isFinished(space)) {
      usleep(1000);
    }
    sleep(1);

    EXPECT_EQ(numSubTask, subTaskCalled);
  }

  {  // 256 sub drainer tasks
    size_t numSubTask = 256;
    std::shared_ptr<DrainerTask> task = std::make_shared<HookableTask>();
    HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());

    std::atomic<int> subTaskCalled{0};
    auto subTask = [&subTaskCalled]() {
      ++subTaskCalled;
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    };
    for (size_t i = 0; i < numSubTask; ++i) {
      PartitionID part = i;
      mockTask->addSubTask(subTask, part);
    }
    taskMgr->addAsyncTask(space, task);
    auto retVal = taskMgr->run();
    EXPECT_TRUE(retVal);

    LOG(INFO) << "wait";
    while (!taskMgr->isFinished(space)) {
      usleep(1000);
    }
    sleep(1);
    EXPECT_EQ(numSubTask, subTaskCalled);
  }
  taskMgr->shutdown();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());
}

// genSubDrainerTask failed
TEST(DrainerTaskManagerTest, gen_sub_task_failed) {
  FLAGS_max_concurrent_subdrainertasks = 10;
  auto env = std::make_unique<nebula::drainer::DrainerEnv>();
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(5);
  auto taskMgr = DummyDrainerTaskManager::instance();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());

  taskMgr->init(env.get(), ioThreadPool);
  GraphSpaceID space = 1;

  {
    std::shared_ptr<DrainerTask> task = std::make_shared<HookableTask>();
    HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());

    mockTask->fGenSubTasks = [&]() { return nebula::cpp2::ErrorCode::E_INVALID_TASK_PARA; };

    taskMgr->addAsyncTask(space, task);
    auto retVal = taskMgr->run();
    EXPECT_FALSE(retVal);

    while (!taskMgr->isFinished(space)) {
      usleep(1000);
    }
    sleep(1);

    EXPECT_EQ(mockTask->status(), nebula::cpp2::ErrorCode::E_INVALID_TASK_PARA);
  }

  taskMgr->shutdown();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());
}

// subDrainerTask failed
TEST(DrainerTaskManagerTest, some_subtask_failed) {
  FLAGS_max_concurrent_subdrainertasks = 10;
  auto env = std::make_unique<nebula::drainer::DrainerEnv>();
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(5);
  auto taskMgr = DummyDrainerTaskManager::instance();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());

  taskMgr->init(env.get(), ioThreadPool);
  GraphSpaceID space = 1;

  std::vector<nebula::cpp2::ErrorCode> errorCode{nebula::cpp2::ErrorCode::SUCCEEDED,
                                                 nebula::cpp2::ErrorCode::E_PART_NOT_FOUND,
                                                 nebula::cpp2::ErrorCode::E_LEADER_CHANGED,
                                                 nebula::cpp2::ErrorCode::E_USER_CANCEL,
                                                 nebula::cpp2::ErrorCode::E_UNKNOWN};

  for (auto t = 0; t < 5; ++t) {
    size_t totalSubTask = std::pow(4, t);
    std::atomic<int> subTaskCalled{0};
    auto errCode = errorCode[t];

    std::shared_ptr<DrainerTask> task = std::make_shared<HookableTask>();
    HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());

    for (size_t i = 0; i < totalSubTask; ++i) {
      PartitionID part = i;
      mockTask->addSubTask(
          [&, j = i]() {
            ++subTaskCalled;
            return j == totalSubTask / 2 ? suc : errCode;
          },
          part);
    }
    taskMgr->addAsyncTask(space, task);
    auto retVal = taskMgr->run();
    EXPECT_TRUE(retVal);

    LOG(INFO) << "wait";
    while (!taskMgr->isFinished(space)) {
      usleep(1000);
    }
    sleep(1);

    EXPECT_LE(subTaskCalled, totalSubTask);
    EXPECT_EQ(mockTask->status(), errCode);
  }

  taskMgr->shutdown();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());
}

// cancel one drainer task, total 1 drainer task
TEST(DrainerTaskManagerTest, cancel_a_running_task_with_only_1_sub_task) {
  FLAGS_max_concurrent_subdrainertasks = 10;
  auto env = std::make_unique<nebula::drainer::DrainerEnv>();
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(5);
  auto taskMgr = DummyDrainerTaskManager::instance();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());

  taskMgr->init(env.get(), ioThreadPool);
  GraphSpaceID space = 1;

  std::shared_ptr<DrainerTask> task = std::make_shared<HookableTask>();
  HookableTask* mockTask = dynamic_cast<HookableTask*>(task.get());

  folly::Promise<int> pTaskRun;
  auto fTaskRun = pTaskRun.getFuture();

  folly::Promise<int> pCancel;
  folly::Future<int> fCancel = pCancel.getFuture();

  PartitionID part = 1;
  mockTask->addSubTask(
      [&]() {
        LOG(INFO) << "sub task running, waiting for cancel";
        pTaskRun.setValue(0);
        fCancel.wait();
        LOG(INFO) << "cancel called";
        return suc;
      },
      part);

  taskMgr->addAsyncTask(space, task);
  auto retVal = taskMgr->run();
  EXPECT_TRUE(retVal);

  fTaskRun.wait();
  taskMgr->cancelTask(space);
  pCancel.setValue(0);
  LOG(INFO) << "wait";
  while (!taskMgr->isFinished(space)) {
    usleep(1000);
  }
  sleep(1);

  auto err = nebula::cpp2::ErrorCode::E_USER_CANCEL;
  EXPECT_EQ(mockTask->status(), err);
  taskMgr->shutdown();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());
}

// cancel one drainer task, total 2 drainer task
// Execute drainer task sequentially
TEST(DrainerTaskManagerTest, cancel_1_task_in_a_2_tasks_queue) {
  FLAGS_max_concurrent_subdrainertasks = 10;
  auto env = std::make_unique<nebula::drainer::DrainerEnv>();
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(5);
  auto taskMgr = DummyDrainerTaskManager::instance();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());

  taskMgr->init(env.get(), ioThreadPool);
  GraphSpaceID space1 = 1;
  GraphSpaceID space2 = 2;
  auto err = nebula::cpp2::ErrorCode::E_USER_CANCEL;

  std::shared_ptr<DrainerTask> vtask1 = std::make_shared<HookableTask>();
  HookableTask* task1 = dynamic_cast<HookableTask*>(vtask1.get());

  task1->fGenSubTasks = [&]() { return task1->subTasks; };

  PartitionID part = 1;
  task1->addSubTask([&]() { return suc; }, part);

  std::shared_ptr<DrainerTask> vtask2 = std::make_shared<HookableTask>();
  HookableTask* task2 = static_cast<HookableTask*>(vtask2.get());

  task2->addSubTask([&]() { return suc; }, part);

  taskMgr->addAsyncTask(space1, vtask1);
  taskMgr->addAsyncTask(space2, vtask2);

  taskMgr->cancelTask(space2);

  // task1 success
  auto retVal = taskMgr->run();
  EXPECT_TRUE(retVal);

  // task2 failed
  retVal = taskMgr->run();
  EXPECT_FALSE(retVal);

  LOG(INFO) << "wait";
  while (!taskMgr->isFinished(space1) || !taskMgr->isFinished(space2)) {
    usleep(1000);
  }
  sleep(1);

  EXPECT_EQ(task1->status(), suc);
  EXPECT_EQ(task2->status(), err);

  taskMgr->shutdown();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());
}

// cancel one drainer task
TEST(DrainerTaskManagerTest, cancel_a_task_before_all_sub_task_running) {
  FLAGS_max_concurrent_subdrainertasks = 10;
  auto env = std::make_unique<nebula::drainer::DrainerEnv>();
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(5);
  auto taskMgr = DummyDrainerTaskManager::instance();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());

  taskMgr->init(env.get(), ioThreadPool);
  GraphSpaceID space = 1;

  auto err = nebula::cpp2::ErrorCode::E_USER_CANCEL;

  std::shared_ptr<DrainerTask> vtask0 = std::make_shared<HookableTask>();
  HookableTask* task0 = dynamic_cast<HookableTask*>(vtask0.get());

  task0->fGenSubTasks = [&]() { return task0->subTasks; };

  PartitionID part = 1;
  task0->addSubTask(
      [&]() {
        LOG(INFO) << "run subTask()";
        return suc;
      },
      part);

  taskMgr->addAsyncTask(space, vtask0);
  taskMgr->cancelTask(space);

  auto retVal = taskMgr->run();
  EXPECT_FALSE(retVal);

  LOG(INFO) << "wait";
  while (!taskMgr->isFinished(space)) {
    usleep(1000);
  }
  sleep(1);

  EXPECT_EQ(task0->status(), err);
  taskMgr->shutdown();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());
}

TEST(DrainerTaskManagerTest, task_run_after_a_gen_sub_task_failed) {
  FLAGS_max_concurrent_subdrainertasks = 10;
  auto env = std::make_unique<nebula::drainer::DrainerEnv>();
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(5);
  auto taskMgr = DummyDrainerTaskManager::instance();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());

  GraphSpaceID space1 = 10;
  GraphSpaceID space2 = 11;
  taskMgr->init(env.get(), ioThreadPool);
  auto err = nebula::cpp2::ErrorCode::E_INVALID_TASK_PARA;

  std::shared_ptr<DrainerTask> vtask1 = std::make_shared<HookableTask>();
  HookableTask* task1 = dynamic_cast<HookableTask*>(vtask1.get());

  std::shared_ptr<DrainerTask> vtask2 = std::make_shared<HookableTask>();
  HookableTask* task2 = dynamic_cast<HookableTask*>(vtask2.get());

  PartitionID part = 1;
  task1->addSubTask(
      [&]() {
        LOG(INFO) << "Drainer task 1";
        return nebula::cpp2::ErrorCode::E_INVALID_TASK_PARA;
      },
      part);

  task2->addSubTask(
      [&]() {
        LOG(INFO) << "Drainer task 2";
        return nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      part);

  taskMgr->addAsyncTask(space1, vtask1);
  taskMgr->addAsyncTask(space2, vtask2);

  // task1 failed
  auto retVal = taskMgr->run();
  EXPECT_TRUE(retVal);

  // task2 success
  retVal = taskMgr->run();
  EXPECT_TRUE(retVal);

  LOG(INFO) << "wait";
  while (!taskMgr->isFinished(space1) || !taskMgr->isFinished(space2)) {
    usleep(1000);
  }
  sleep(1);

  EXPECT_EQ(task1->status(), err);
  EXPECT_EQ(task2->status(), suc);

  taskMgr->shutdown();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());
}

TEST(DrainerTaskManagerTest, cancel_a_task_while_some_sub_task_running) {
  FLAGS_max_concurrent_subdrainertasks = 10;
  auto env = std::make_unique<nebula::drainer::DrainerEnv>();
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(5);
  auto taskMgr = DummyDrainerTaskManager::instance();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());

  taskMgr->init(env.get(), ioThreadPool);
  GraphSpaceID space = 1;

  auto usr_cancel = nebula::cpp2::ErrorCode::E_USER_CANCEL;

  folly::Promise<int> cancle_p;
  folly::Future<int> cancel = cancle_p.getFuture();

  folly::Promise<int> subtask_run_p;
  folly::Future<int> subtask_run_f = subtask_run_p.getFuture();

  PartitionID part = 1;
  std::shared_ptr<DrainerTask> vtask1 = std::make_shared<HookableTask>();
  HookableTask* task1 = dynamic_cast<HookableTask*>(vtask1.get());

  task1->addSubTask(
      [&]() {
        LOG(INFO) << "wait for cancel()";
        subtask_run_p.setValue(0);
        cancel.wait();
        LOG(INFO) << "run subTask(1)";
        return suc;
      },
      part);

  PartitionID part2 = 2;
  task1->addSubTask(
      [&]() {
        LOG(INFO) << "run subTask(2)";
        return suc;
      },
      part2);

  taskMgr->addAsyncTask(space, vtask1);
  auto retVal = taskMgr->run();
  EXPECT_TRUE(retVal);

  subtask_run_f.wait();
  LOG(INFO) << "before taskMgr cancelTask;";
  taskMgr->cancelTask(space);
  LOG(INFO) << "after taskMgr cancelTask;";
  cancle_p.setValue(0);

  while (!taskMgr->isFinished(space)) {
    usleep(1000);
  }
  sleep(1);

  EXPECT_EQ(task1->status(), usr_cancel);

  taskMgr->shutdown();
  EXPECT_EQ(0, taskMgr->taskQueue_.size());
  EXPECT_EQ(0, taskMgr->tasks_.size());
}

}  // namespace drainer
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
