/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_DRAINEREXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_DRAINEREXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class AddDrainerExecutor final : public Executor {
 public:
  AddDrainerExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("AddDrainerExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class RemoveDrainerExecutor final : public Executor {
 public:
  RemoveDrainerExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("RemoveDrainerExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class ListDrainersExecutor final : public Executor {
 public:
  ListDrainersExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ListDrainersExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_DRAINEREXECUTOR_H_
