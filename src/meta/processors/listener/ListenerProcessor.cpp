/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/listener/ListenerProcessor.h"

#include "meta/ActiveHostsMan.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

void AddListenerProcessor::process(const cpp2::AddListenerReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto type = req.get_type();
  const auto& storageHosts = req.get_storage_hosts();

  if (type == cpp2::ListenerType::SYNC &&
      (!req.meta_host_ref().has_value() || !req.space_name_ref().has_value())) {
    LOG(ERROR) << "Add listener failed, invalid listener parameter.";
    auto ret = nebula::cpp2::ErrorCode::E_INVALID_PARM;
    handleErrorCode(ret);
    onFinished();
    return;
  }

  HostAddr metaHost;
  if (type == cpp2::ListenerType::SYNC) {
    metaHost = *req.meta_host_ref();
  }

  auto ret = listenerExist(space, type, storageHosts, metaHost);
  if (ret != nebula::cpp2::ErrorCode::E_LISTENER_NOT_FOUND) {
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Add listener failed, listener already exists.";
      ret = nebula::cpp2::ErrorCode::E_EXISTED;
    } else {
      LOG(INFO) << "Add listener failed, error: " << apache::thrift::util::enumNameSafe(ret);
    }
    handleErrorCode(ret);
    onFinished();
    return;
  }

  // TODO : (sky) if type is elasticsearch, need check text search service.
  const auto& prefix = MetaKeyUtils::partPrefix(space);
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "List parts failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  std::vector<PartitionID> parts;
  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    parts.emplace_back(MetaKeyUtils::parsePartKeyPartId(iter->key()));
    iter->next();
  }

  // When the type is sync, the drainer must have been registered
  // Because each sync listener part uses a fixed drainer
  std::vector<cpp2::ServiceClient> drainerClients;
  if (type == cpp2::ListenerType::SYNC) {
    const auto& serviceKey =
        MetaKeyUtils::spaceServiceKey(space, cpp2::ExternalSpaceServiceType::DRAINER);
    auto sRet = doGet(serviceKey);
    if (!nebula::ok(sRet)) {
      auto retCode = nebula::error(sRet);
      if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
        retCode = nebula::cpp2::ErrorCode::E_DRAINER_CLIENT_NOT_FOUND;
      }
      LOG(ERROR) << "List drainer service failed, error: "
                 << apache::thrift::util::enumNameSafe(retCode);
      handleErrorCode(retCode);
      onFinished();
      return;
    }

    drainerClients = MetaKeyUtils::parseServiceClients(nebula::value(sRet));
    if (drainerClients.empty()) {
      LOG(ERROR) << "Drainer service clients not exit.";
      handleErrorCode(nebula::cpp2::ErrorCode::E_SERVICE_NOT_FOUND);
      onFinished();
      return;
    }
  }

  std::vector<kvstore::KV> data;
  // Sync listener tospace name
  std::string spaceName;
  if (type == cpp2::ListenerType::SYNC) {
    spaceName = *req.space_name_ref();
  }

  for (size_t i = 0; i < parts.size(); i++) {
    data.emplace_back(MetaKeyUtils::listenerKey(space, parts[i], type),
                      MetaKeyUtils::listenerVal(storageHosts[i % storageHosts.size()], spaceName));

    // use the specified drainer client
    // TODO(pandasheep) drainer maybe use user and passwd
    if (type == cpp2::ListenerType::SYNC) {
      auto key = MetaKeyUtils::listenerDrainerKey(space, parts[i]);
      auto drainerClient = drainerClients[i % drainerClients.size()];
      auto val = MetaKeyUtils::listenerDrainerVal(drainerClient.get_host(), spaceName);
      data.emplace_back(std::move(key), std::move(val));
    }
  }

  // For meta listener, spaceId is space, partId is 0
  if (type == cpp2::ListenerType::SYNC) {
    data.emplace_back(MetaKeyUtils::listenerKey(space, 0, type),
                      MetaKeyUtils::listenerVal(metaHost, spaceName));
    auto key = MetaKeyUtils::listenerDrainerKey(space, 0);
    // use first drainer client
    auto drainerClient = drainerClients[0];
    auto val = MetaKeyUtils::listenerDrainerVal(drainerClient.get_host(), spaceName);
    data.emplace_back(std::move(key), std::move(val));
  }

  doSyncPutAndUpdate(std::move(data));
}

void RemoveListenerProcessor::process(const cpp2::RemoveListenerReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto type = req.get_type();
  auto ret = listenerExist(space, type);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (ret == nebula::cpp2::ErrorCode::E_LISTENER_NOT_FOUND) {
      LOG(INFO) << "Remove listener failed, listener not exists.";
    } else {
      LOG(INFO) << "Remove listener failed, error: " << apache::thrift::util::enumNameSafe(ret);
    }
    handleErrorCode(ret);
    onFinished();
    return;
  }

  std::vector<std::string> keys;
  const auto& prefix = MetaKeyUtils::listenerPrefix(space, type);
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "Remove listener failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    keys.emplace_back(iter->key());
    iter->next();
  }

  // remove listener drainer data
  if (type == cpp2::ListenerType::SYNC) {
    auto listenerDrainerPrefix = MetaKeyUtils::listenerDrainerPrefix(space);
    auto literRet = doPrefix(listenerDrainerPrefix);
    if (!nebula::ok(literRet)) {
      auto retCode = nebula::error(literRet);
      LOG(ERROR) << "List drainer client failed, error: "
                 << apache::thrift::util::enumNameSafe(retCode);
      handleErrorCode(retCode);
      onFinished();
      return;
    }
    auto liter = nebula::value(literRet).get();
    while (liter->valid()) {
      keys.emplace_back(liter->key());
      liter->next();
    }
  }

  doSyncMultiRemoveAndUpdate(std::move(keys));
}

void ListListenersProcessor::process(const cpp2::ListListenersReq& req) {
  auto space = req.get_space_id();
  auto type = req.get_type();
  CHECK_SPACE_ID_AND_RETURN(space);
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());

  std::string prefix;
  if (type == cpp2::ListenerType::ALL) {
    prefix = MetaKeyUtils::listenerPrefix(space);
  } else {
    prefix = MetaKeyUtils::listenerPrefix(space, type);
  }

  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "List listener failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto activeHostsRet =
      ActiveHostsMan::getActiveHosts(kvstore_,
                                     FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor,
                                     cpp2::HostRole::STORAGE_LISTENER);
  if (!nebula::ok(activeHostsRet)) {
    handleErrorCode(nebula::error(activeHostsRet));
    onFinished();
    return;
  }
  auto activeHosts = std::move(nebula::value(activeHostsRet));

  auto metaActiveHostsRet =
      ActiveHostsMan::getActiveHosts(kvstore_,
                                     FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor,
                                     cpp2::HostRole::META_LISTENER);
  if (!nebula::ok(metaActiveHostsRet)) {
    handleErrorCode(nebula::error(metaActiveHostsRet));
    onFinished();
    return;
  }
  auto metaActiveHosts = std::move(nebula::value(metaActiveHostsRet));

  std::vector<cpp2::ListenerInfo> listeners;
  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    auto partId = MetaKeyUtils::parseListenerPart(iter->key());
    auto listenerType = MetaKeyUtils::parseListenerType(iter->key());

    cpp2::ListenerInfo listener;
    if (listenerType == cpp2::ListenerType::SYNC) {
      listener.space_name_ref() = MetaKeyUtils::parseListenerSpacename(iter->val());
    }
    listener.type_ref() = listenerType;
    listener.host_ref() = MetaKeyUtils::parseListenerHost(iter->val());
    listener.part_id_ref() = partId;
    if (partId == 0) {
      if (std::find(metaActiveHosts.begin(), metaActiveHosts.end(), *listener.host_ref()) !=
          metaActiveHosts.end()) {
        listener.status_ref() = cpp2::HostStatus::ONLINE;
      } else {
        listener.status_ref() = cpp2::HostStatus::OFFLINE;
      }
    } else {
      if (std::find(activeHosts.begin(), activeHosts.end(), *listener.host_ref()) !=
          activeHosts.end()) {
        listener.status_ref() = cpp2::HostStatus::ONLINE;
      } else {
        listener.status_ref() = cpp2::HostStatus::OFFLINE;
      }
    }
    listeners.emplace_back(std::move(listener));
    iter->next();
  }

  resp_.listeners_ref() = std::move(listeners);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

// For the master cluster, the survival status of the drainer cluster is unknown
void ListListenerDrainersProcessor::process(const cpp2::ListListenerDrainersReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());

  // meta listener partId is 0
  auto prefix = MetaKeyUtils::listenerDrainerPrefix(space);
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(ERROR) << "List listener drainer failed, error: "
               << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  std::unordered_map<PartitionID, cpp2::DrainerClientInfo> drainerClients;
  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    auto partId = MetaKeyUtils::parseListenerDrainerPart(iter->key());
    cpp2::DrainerClientInfo drainClientInfo;
    drainClientInfo.host_ref() = MetaKeyUtils::parseListenerDrainerHost(iter->val());
    drainClientInfo.space_name_ref() = MetaKeyUtils::parseListenerDrainerSpacename(iter->val());
    drainerClients.emplace(partId, std::move(drainClientInfo));
    iter->next();
  }
  resp_.drainerClients_ref() = std::move(drainerClients);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
