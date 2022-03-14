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

/**
 * @brief This class manages some external services.
 * For example, es client, drainer client, drainer server, etc.
 *
 */
class ServiceManager final {
 public:
  ServiceManager() = default;
  ~ServiceManager();

  /**
   * @brief Get the service client, such as es client.
   *
   * @param type
   * @return StatusOr<std::vector<nebula::meta::cpp2::ServiceClient>>
   */
  StatusOr<std::vector<nebula::meta::cpp2::ServiceClient>> getServiceClients(
      cpp2::ExternalServiceType type);

  /**
   * @brief Get sync storage listener drainer client of specified space on master cluster.
   *
   * @param space
   * @param partId
   * @return StatusOr<nebula::meta::cpp2::DrainerClientInfo>
   */
  StatusOr<nebula::meta::cpp2::DrainerClientInfo> getDrainerClient(GraphSpaceID space,
                                                                   PartitionID partId);

  /**
   * @brief Get sync meta listener drainer client of specified space on master cluster.
   *
   * @param space
   * @return StatusOr<nebula::meta::cpp2::DrainerClientInfo>
   */
  StatusOr<nebula::meta::cpp2::DrainerClientInfo> getMetaListenerDrainerClient(GraphSpaceID space);

  /**
   * @brief Get drainer server of specified space on slave cluster
   *
   * @param spaceId
   * @return StatusOr<std::vector<cpp2::DrainerInfo>>
   */
  StatusOr<std::vector<cpp2::DrainerInfo>> getDrainerServer(GraphSpaceID spaceId);

  /**
   * @brief Get the meta listener info of specified host
   *
   * @param host
   * @return StatusOr<std::vector<std::pair<GraphSpaceID, std::string>>>
   */
  StatusOr<std::vector<std::pair<GraphSpaceID, std::string>>> getMetaListenerInfo(HostAddr host);

  /**
   * @brief Check if listener can sync data.
   *
   * @param spaceId
   * @return StatusOr<bool>
   */
  StatusOr<bool> checkListenerCanSync(GraphSpaceID spaceId);

  void init(MetaClient *client);

  static std::unique_ptr<ServiceManager> create(MetaClient *client);

 private:
  MetaClient *metaClient_{nullptr};
};

}  // namespace meta
}  // namespace nebula
#endif  // COMMON_META_SERVICEMANAGER_H_
