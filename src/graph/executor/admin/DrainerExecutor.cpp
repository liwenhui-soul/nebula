/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/executor/admin/DrainerExecutor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> AddDrainerExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* alNode = asNode<AddDrainer>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()
      ->getMetaClient()
      ->addDrainer(spaceId, alNode->hosts())
      .via(runner())
      .thenValue([this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> RemoveDrainerExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()->getMetaClient()->removeDrainer(spaceId).via(runner()).thenValue(
      [this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> ListDrainersExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()->getMetaClient()->listDrainers(spaceId).via(runner()).thenValue(
      [this](StatusOr<std::vector<meta::cpp2::DrainerInfo>> resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }

        auto drainerInfos = std::move(resp).value();
        std::sort(drainerInfos.begin(), drainerInfos.end(), [](const auto& a, const auto& b) {
          return a.host_ref() < b.host_ref();
        });

        DataSet result({"Host", "Status"});
        for (const auto& info : drainerInfos) {
          Row row;
          row.values.emplace_back((*info.host_ref()).toString());
          row.values.emplace_back(apache::thrift::util::enumNameSafe(info.get_status()));
          result.emplace_back(std::move(row));
        }

        return finish(std::move(result));
      });
}

}  // namespace graph
}  // namespace nebula
