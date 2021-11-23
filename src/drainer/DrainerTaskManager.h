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

  // By default, one space can be synchronized at the same time,
  // Each space synchronizes max_concurrent_subdrainertasks parts at the same time.
  // Initialize wals_ according to drainerPath_ from DrainerEnv
  bool init(DrainerEnv* env, std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool);

  // Only for test
  void addAsyncTask(GraphSpaceID spaceId, std::shared_ptr<DrainerTask> task);

  // Read the directory under drainerPath_ in DrainerEnv and initialize the drainer task
  Status addAsyncTask();

  void invoke();

  // For drainer, stop sending data to storage
  nebula::cpp2::ErrorCode cancelTask(GraphSpaceID spaceId);

  void shutdown();

  bool isFinished(GraphSpaceID spaceId);

  void removeSpaceDir(const std::string& dir);

  // For the continuity of logId, do nothing
  bool preProcessLog(LogID logId, TermID termId, ClusterID clusterId, const std::string& log);

  // load space meta
  Status loadSpaceMeta(std::string& spaceDir, GraphSpaceID toSpaceId);

 protected:
  void schedule();

  // Get a subtask execution from drainer task
  void runSubTask(GraphSpaceID spaceId);

 protected:
  DrainerEnv* env_{nullptr};
  std::atomic<bool> shutdown_{false};
  std::unique_ptr<ThreadPool> pool_{nullptr};
  TaskContainer tasks_;
  TaskQueue taskQueue_;
  std::unique_ptr<thread::GenericWorker> bgThread_{nullptr};

  std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_{nullptr};
  // inter storage client
  std::unique_ptr<nebula::storage::InternalStorageClient> interClient_{nullptr};
};

}  // namespace drainer
}  // namespace nebula

#endif  // DRAINER_DRAINERTASKMANAGER_H_
