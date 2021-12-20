/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_SPACESERVICEEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_SPACESERVICEEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class SignInSpaceServiceExecutor final : public Executor {
 public:
  SignInSpaceServiceExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("SignInSpaceServiceExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class SignOutSpaceServiceExecutor final : public Executor {
 public:
  SignOutSpaceServiceExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("SignOutSpaceServiceExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class ShowSpaceServiceClientsExecutor final : public Executor {
 public:
  ShowSpaceServiceClientsExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowSpaceServiceClientsExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_SPACESERVICEEXECUTOR_H_
