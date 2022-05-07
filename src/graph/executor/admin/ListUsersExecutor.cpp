// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/ListUsersExecutor.h"

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> ListUsersExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return listUsers();
}

folly::Future<Status> ListUsersExecutor::listUsers() {
  return qctx()->getMetaClient()->listIpWhitelists().via(runner()).thenValue(
      [this](StatusOr<std::unordered_map<std::string, std::unordered_set<std::string>>> &&resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          return std::move(resp).status();
        }
        nebula::DataSet v({"Account", "IP Whitelist"});
        auto items = std::move(resp).value();
        for (const auto &item : items) {
          v.emplace_back(nebula::Row({
              std::move(item).first,
              folly::join(",", std::move(item).second),
          }));
        }
        return finish(std::move(v));
      });
}

}  // namespace graph
}  // namespace nebula
