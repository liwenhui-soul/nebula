/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/SpaceServiceExecutor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> SignInSpaceServiceExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *siNode = asNode<SignInSpaceService>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  auto type = siNode->type();
  return qctx()
      ->getMetaClient()
      ->signInSpaceService(spaceId, type, siNode->clients())
      .via(runner())
      .thenValue([this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(resp);
        if (!resp.value()) {
          return Status::Error("Sign in space service failed!");
        }
        return Status::OK();
      });
}

folly::Future<Status> SignOutSpaceServiceExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *siNode = asNode<SignOutSpaceService>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  auto type = siNode->type();
  return qctx()
      ->getMetaClient()
      ->signOutSpaceService(spaceId, type)
      .via(runner())
      .thenValue([this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(resp);
        if (!resp.value()) {
          return Status::Error("Sign out space service failed!");
        }
        return Status::OK();
      });
}

folly::Future<Status> ShowSpaceServiceClientsExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *siNode = asNode<ShowSpaceServiceClients>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  auto type = siNode->type();

  return qctx()
      ->getMetaClient()
      ->listSpaceServiceClients(spaceId, type)
      .via(runner())
      .thenValue([this](auto &&resp) {
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }
        auto values = std::move(resp).value();
        DataSet v({"Type", "Host", "Port"});
        for (const auto &value : values) {
          for (const auto &client : value.second) {
            nebula::Row r({apache::thrift::util::enumNameSafe(value.first),
                           client.host.host,
                           client.host.port});
            v.emplace_back(std::move(r));
          }
        }
        return finish(std::move(v));
      });
}

}  // namespace graph
}  // namespace nebula
