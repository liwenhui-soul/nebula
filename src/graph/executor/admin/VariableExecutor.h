/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_VARIABLEEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_VARIABLEEXECUTOR_H_

#include "graph/executor/Executor.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace graph {

class SetVariableExecutor final : public Executor {
 public:
  SetVariableExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("SetVariableExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  std::string name_;
  Value value_;
};

class GetVariableExecutor final : public Executor {
 public:
  GetVariableExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("GetVariableExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  DataSet generateResult(const meta::cpp2::VariableItem &item);

 private:
  std::string name_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_VARIABLEEXECUTOR_H_
