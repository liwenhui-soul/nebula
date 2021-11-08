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
  auto type = req.get_type();

  if (type == cpp2::ListenerType::SYNC && !req.space_name_ref().has_value()) {
    LOG(ERROR) << "Add listener failed, invalid listener parameter.";
    auto ret = nebula::cpp2::ErrorCode::E_INVALID_PARM;
    handleErrorCode(ret);
    onFinished();
    return;
  }

  const auto& hosts = req.get_hosts();
  auto ret = listenerExist(space, type, hosts);
  if (ret != nebula::cpp2::ErrorCode::E_LISTENER_NOT_FOUND) {
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Add listener failed, listener already exists.";
      ret = nebula::cpp2::ErrorCode::E_EXISTED;
    } else {
      LOG(ERROR) << "Add listener failed, error: " << apache::thrift::util::enumNameSafe(ret);
    }
    handleErrorCode(ret);
    onFinished();
    return;
  }

  // TODO : (sky) if type is elasticsearch, need check text search service.
  folly::SharedMutex::WriteHolder wHolder(LockUtils::listenerLock());
  folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
  const auto& prefix = MetaKeyUtils::partPrefix(space);
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(ERROR) << "List parts failed, error: " << apache::thrift::util::enumNameSafe(retCode);
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
    const auto& serviceKey = MetaKeyUtils::serviceKey(cpp2::ExternalServiceType::DRAINER);
    auto sRet = doGet(serviceKey);
    if (!nebula::ok(sRet)) {
      auto retCode = nebula::error(sRet);
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
                      MetaKeyUtils::listenerVal(hosts[i % hosts.size()], spaceName));

    // use the specified drainer client
    // TODO(pandasheep) drainer maybe use user and passwd
    if (type == cpp2::ListenerType::SYNC) {
      auto key = MetaKeyUtils::listenerDrainerKey(space, parts[i]);
      auto drainerClient = drainerClients[i % drainerClients.size()];
      auto val = MetaKeyUtils::listenerDrainerVal(drainerClient.get_host(), spaceName);
      data.emplace_back(std::move(key), std::move(val));
    }
  }

  doSyncPutAndUpdate(std::move(data));
}

void RemoveListenerProcessor::process(const cpp2::RemoveListenerReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  auto type = req.get_type();
  auto ret = listenerExist(space, type);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (ret == nebula::cpp2::ErrorCode::E_LISTENER_NOT_FOUND) {
      LOG(ERROR) << "Remove listener failed, listener not exists.";
    } else {
      LOG(ERROR) << "Remove listener failed, error: " << apache::thrift::util::enumNameSafe(ret);
    }
    handleErrorCode(ret);
    onFinished();
    return;
  }

  folly::SharedMutex::WriteHolder wHolder(LockUtils::listenerLock());
  std::vector<std::string> keys;
  const auto& prefix = MetaKeyUtils::listenerPrefix(space, type);
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(ERROR) << "Remove listener failed, error: " << apache::thrift::util::enumNameSafe(retCode);
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
  folly::SharedMutex::ReadHolder rHolder(LockUtils::listenerLock());

  std::string prefix;
  if (type == cpp2::ListenerType::ALL) {
    prefix = MetaKeyUtils::listenerPrefix(space);
  } else {
    prefix = MetaKeyUtils::listenerPrefix(space, type);
  }

  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(ERROR) << "List listener failed, error: " << apache::thrift::util::enumNameSafe(retCode);
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

  std::vector<cpp2::ListenerInfo> listeners;
  auto activeHosts = std::move(nebula::value(activeHostsRet));
  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    cpp2::ListenerInfo listener;
    if (type == cpp2::ListenerType::SYNC) {
      listener.set_space_name(MetaKeyUtils::parseListenerSpacename(iter->val()));
    }
    listener.set_type(MetaKeyUtils::parseListenerType(iter->key()));
    listener.set_host(MetaKeyUtils::parseListenerHost(iter->val()));
    listener.set_part_id(MetaKeyUtils::parseListenerPart(iter->key()));
    if (std::find(activeHosts.begin(), activeHosts.end(), *listener.host_ref()) !=
        activeHosts.end()) {
      listener.set_status(cpp2::HostStatus::ONLINE);
    } else {
      listener.set_status(cpp2::HostStatus::OFFLINE);
    }
    listeners.emplace_back(std::move(listener));
    iter->next();
  }
  resp_.set_listeners(std::move(listeners));
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

// For the master cluster, the survival status of the drainer cluster is unknown
void ListListenerDrainersProcessor::process(const cpp2::ListListenerDrainersReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  folly::SharedMutex::ReadHolder rHolder(LockUtils::listenerLock());

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
    drainClientInfo.set_host(MetaKeyUtils::parseListenerDrainerHost(iter->val()));
    drainClientInfo.set_space_name(MetaKeyUtils::parseListenerDrainerSpacename(iter->val()));
    drainerClients.emplace(partId, std::move(drainClientInfo));
    iter->next();
  }
  resp_.set_drainerClients(std::move(drainerClients));
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
