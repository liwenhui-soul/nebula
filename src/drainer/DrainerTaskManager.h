/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef DRAINER_DRAINERTASKMANAGER_H_
#define DRAINER_DRAINERTASKMANAGER_H_

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/task_queue/UnboundedBlockingQueue.h>
#include <gtest/gtest_prod.h>

#include "clients/storage/InternalStorageClient.h"
#include "common/base/Base.h"
#include "drainer/DrainerTask.h"

namespace nebula {
namespace drainer {

/**
 * @brief This class manages the drainer task
 */
class DrainerTaskManager {
  FRIEND_TEST(DrainerTaskManagerTest, ctor);
  FRIEND_TEST(DrainerTaskManagerTest, happy_path_task1_sub1);
  FRIEND_TEST(DrainerTaskManagerTest, run_a_medium_task_before_a_huge_task);
  FRIEND_TEST(DrainerTaskManagerTest, happy_path);
  FRIEND_TEST(DrainerTaskManagerTest, gen_sub_task_failed);
  FRIEND_TEST(DrainerTaskManagerTest, some_subtask_failed);
  FRIEND_TEST(DrainerTaskManagerTest, cancel_a_running_task_with_only_1_sub_task);
  FRIEND_TEST(DrainerTaskManagerTest, cancel_1_task_in_a_2_tasks_queue);
  FRIEND_TEST(DrainerTaskManagerTest, cancel_a_task_before_all_sub_task_running);
  FRIEND_TEST(DrainerTaskManagerTest, task_run_after_a_gen_sub_task_failed);
  FRIEND_TEST(DrainerTaskManagerTest, cancel_a_task_while_some_sub_task_running);
  FRIEND_TEST(DrainerTaskTest, PartNumSameOnePartTest);
  FRIEND_TEST(DrainerTaskTest, PartNumSameMultiPartTest);
  FRIEND_TEST(DrainerTaskTest, PartNumNotSameOnePartTest);
  FRIEND_TEST(DrainerTaskTest, PartNumNotSameMultiPartTest);

 public:
  using ThreadPool = folly::IOThreadPoolExecutor;
  using TTask = std::shared_ptr<DrainerTask>;
  using TaskContainer = folly::ConcurrentHashMap<GraphSpaceID, TTask>;

  // Each space is a task
  using TaskQueue = folly::UnboundedBlockingQueue<GraphSpaceID>;

  DrainerTaskManager() = default;

  static DrainerTaskManager* instance() {
    static DrainerTaskManager drainerTaskManager;
    return &drainerTaskManager;
  }

  /**
   * @brief Initialize some member information, then use a background thread to execute the
   * schedule function.
   *
   * @param env
   * @param ioThreadPool
   * @return True if init succeeded
   */
  bool init(DrainerEnv* env, std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool);

  /**
   * @brief Add an asynchronous task，only used for UT.
   *
   * @param spaceId
   * @param task
   */
  void addAsyncTask(GraphSpaceID spaceId, std::shared_ptr<DrainerTask> task);

  /**
   * @brief Read the directory under drainerPath_ in DrainerEnv and initialize the drainer task.
   * Then put the drainer task on the task queue. A space creates a drainer task.
   *
   * @return Status
   */
  Status addAsyncTask();

  /**
   * @brief If the spaceId drainer task is on the task queue, cancel the execution of the task.
   *
   * @param spaceId
   * @return nebula::cpp2::ErrorCode Whether cancelTask was successful
   */
  nebula::cpp2::ErrorCode cancelTask(GraphSpaceID spaceId);

  /**
   * @brief Wait for the background thread to exit, exit the DrainerTaskManager
   */
  void shutdown();

  /**
   * @brief Check whether the drainer task of spaceId is finished
   *
   * @param spaceId the drainer task corresponding to spaceId
   * @return True if the drainer task corresponding to spaceid is not on the task queue or has been
   * executed.
   */
  bool isFinished(GraphSpaceID spaceId);

  /**
   * @brief Remove invalid sync listener space data
   *
   * @param dir Space directory name
   */
  void removeSpaceDir(const std::string& dir);

  /**
   * @brief For the continuity of logId, do nothing
   *
   * @return True
   */
  bool preProcessLog(LogID, TermID, ClusterID, const std::string&) {
    return true;
  }

  /**
   * @brief Get some meta information of the toSpaceId
   *
   * @param spaceDir Space path
   * @param toSpaceId The Space Id to process.
   * @return Status::OK() If successful.
   */
  Status loadSpaceMeta(std::string& spaceDir, GraphSpaceID toSpaceId);

 protected:
  /**
   * @brief General schedule interface.
   * When the drainer task queue is empty, execute addAsyncTask periodically.
   * 1）Generate drainer tasks according to the space and put them on the drainer task queue.
   * 2）Then drainer tasks are executed serially.
   * 3）Each drainer task generates multiple drainer subtasks according to the part, and the drainer
   * subtasks are executed in parallel.
   *
   */
  void schedule();

  /**
   * @brief Execute a subtask of the drainer task of spaceId
   *
   * @param spaceId
   */
  void runSubTask(GraphSpaceID spaceId);

 protected:
  DrainerEnv* env_{nullptr};
  std::atomic<bool> shutdown_{false};
  std::unique_ptr<ThreadPool> pool_{nullptr};
  TaskContainer tasks_;
  TaskQueue taskQueue_;
  std::unique_ptr<thread::GenericWorker> bgThread_{nullptr};

  std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_{nullptr};
  std::unique_ptr<nebula::storage::InternalStorageClient> interClient_{nullptr};
};

}  // namespace drainer
}  // namespace nebula

#endif  // DRAINER_DRAINERTASKMANAGER_H_
