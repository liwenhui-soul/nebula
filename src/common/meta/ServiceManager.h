/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_META_SERVICEMANAGER_H_
#define COMMON_META_SERVICEMANAGER_H_

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

// This class manages some external services.
// For example, es client, drainer client, drainer server, etc.
class ServiceManager final {
 public:
  ServiceManager() = default;
  ~ServiceManager();

  // Get the service client, such as es client.
  StatusOr<std::vector<nebula::meta::cpp2::ServiceClient>> getServiceClients(
      cpp2::ExternalServiceType type);

  // Sync storage listener drainer client for master cluster.
  StatusOr<nebula::meta::cpp2::DrainerClientInfo> getDrainerClient(GraphSpaceID space,
                                                                   PartitionID partId);
  // Sync meta listener drainer client for master cluster.
  StatusOr<nebula::meta::cpp2::DrainerClientInfo> getMetaListenerDrainerClient(GraphSpaceID space);

  // Drainer server for slave cluster
  StatusOr<std::vector<cpp2::DrainerInfo>> getDrainerServer(GraphSpaceID spaceId);

  // Get meta listener info
  StatusOr<std::vector<std::pair<GraphSpaceID, std::string>>> getMetaListenerInfo(HostAddr host);

  void init(MetaClient *client);

  static std::unique_ptr<ServiceManager> create(MetaClient *client);

 private:
  MetaClient *metaClient_{nullptr};
};

}  // namespace meta
}  // namespace nebula
#endif  // COMMON_META_SERVICEMANAGER_H_
