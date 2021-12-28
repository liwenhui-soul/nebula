/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/meta/ServiceManager.h"

#include "common/fs/FileUtils.h"

namespace nebula {
namespace meta {

ServiceManager::~ServiceManager() {
  if (nullptr != metaClient_) {
    metaClient_ = nullptr;
  }
}

void ServiceManager::init(MetaClient *client) {
  CHECK_NOTNULL(client);
  metaClient_ = client;
}

std::unique_ptr<ServiceManager> ServiceManager::create(MetaClient *client) {
  auto mgr = std::make_unique<ServiceManager>();
  mgr->init(client);
  return mgr;
}

StatusOr<std::vector<nebula::meta::cpp2::ServiceClient>> ServiceManager::getServiceClients(
    meta::cpp2::ExternalServiceType type) {
  auto ret = metaClient_->getServiceClientsFromCache(type);
  if (!ret.ok()) {
    return ret.status();
  }
  if (ret.value().empty()) {
    return Status::Error("Service list is empty");
  }
  return std::move(ret).value();
}

StatusOr<nebula::meta::cpp2::DrainerClientInfo> ServiceManager::getDrainerClient(
    GraphSpaceID space, PartitionID partId) {
  auto ret = metaClient_->getDrainerClientFromCache(space, partId);
  if (!ret.ok()) {
    return ret.status();
  }
  return std::move(ret).value();
}

StatusOr<std::vector<cpp2::DrainerInfo>> ServiceManager::getDrainerServer(GraphSpaceID space) {
  auto ret = metaClient_->getDrainerFromCache(space);
  if (!ret.ok()) {
    return ret.status();
  }
  return std::move(ret).value();
}

StatusOr<nebula::meta::cpp2::DrainerClientInfo> ServiceManager::getMetaListenerDrainerClient(
    GraphSpaceID space) {
  auto ret = metaClient_->getMetaListenerDrainerOnSpaceFromCache(space);
  if (!ret.ok()) {
    return ret.status();
  }
  return std::move(ret).value();
}

StatusOr<std::vector<std::pair<GraphSpaceID, std::string>>> ServiceManager::getMetaListenerInfo(
    HostAddr host) {
  auto ret = metaClient_->getMetaListenerInfoFromCache(host);
  if (!ret.ok()) {
    return ret.status();
  }
  return std::move(ret).value();
}

}  // namespace meta
}  // namespace nebula
