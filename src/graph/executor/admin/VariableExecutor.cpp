/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/VariableExecutor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/time/ScopedTimer.h"
#include "graph/context/QueryContext.h"
#include "graph/planner/plan/Admin.h"
#include "graph/util/SchemaUtil.h"

namespace nebula {
namespace graph {

folly::Future<Status> SetVariableExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *scNode = asNode<SetVariable>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()
      ->getMetaClient()
      ->setVariable(spaceId, scNode->getName(), scNode->getValue())
      .via(runner())
      .thenValue([scNode](StatusOr<bool> resp) {
        if (!resp.ok()) {
          LOG(ERROR) << "Set variable `" << scNode->getName() << "' failed: " << resp.status();
          return Status::Error("Set variable `%s' failed: %s",
                               scNode->getName().c_str(),
                               resp.status().toString().c_str());
        }
        return Status::OK();
      });
}

DataSet GetVariableExecutor::generateResult(const meta::cpp2::VariableItem &item) {
  DataSet result;
  result.colNames = {"name", "type", "value"};
  std::vector<Value> columns;
  columns.resize(3);
  auto value = item.get_value();
  columns[0].setStr(item.get_name());
  columns[1].setStr(value.typeName());
  columns[2] = std::move(value);
  result.rows.emplace_back(std::move(columns));
  return result;
}

folly::Future<Status> GetVariableExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *gcNode = asNode<GetVariable>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()
      ->getMetaClient()
      ->getVariable(spaceId, gcNode->getName())
      .via(runner())
      .thenValue([this, gcNode](StatusOr<meta::cpp2::VariableItem> resp) {
        if (!resp.ok()) {
          LOG(ERROR) << "Get variable `" << gcNode->getName() << "' failed: " << resp.status();
          return Status::Error("Get variable `%s' failed: %s",
                               gcNode->getName().c_str(),
                               resp.status().toString().c_str());
        }
        auto result = generateResult(resp.value());
        return finish(ResultBuilder().value(Value(std::move(result))).build());
      });
}

}  // namespace graph
}  // namespace nebula
