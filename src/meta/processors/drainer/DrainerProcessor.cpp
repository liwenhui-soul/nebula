/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/drainer/DrainerProcessor.h"

#include "kvstore/LogEncoder.h"
#include "meta/ActiveHostsMan.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

// For Slave cluster
void AddDrainerProcessor::process(const cpp2::AddDrainerReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  const auto& hosts = req.get_hosts();
  auto ret = drainerExist(space);
  if (ret != nebula::cpp2::ErrorCode::E_DRAINER_NOT_FOUND) {
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Add drainer failed, drainer already exists in space " << space;
      ret = nebula::cpp2::ErrorCode::E_EXISTED;
    } else {
      LOG(ERROR) << "Add drainer failed, error: " << apache::thrift::util::enumNameSafe(ret);
    }
    handleErrorCode(ret);
    onFinished();
    return;
  }

  // For the slave cluster, after the drainer server is started,
  // it will send a heartbeat to the meta. Therefore, the drainer server added
  // under this space must be in the global drainer server
  auto activeHostsRet = ActiveHostsMan::getActiveHosts(
      kvstore_, FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor, cpp2::HostRole::DRAINER);
  if (!nebula::ok(activeHostsRet)) {
    handleErrorCode(nebula::error(activeHostsRet));
    onFinished();
    return;
  }

  auto activeDrainerHosts = std::move(nebula::value(activeHostsRet));
  for (auto& host : hosts) {
    if (std::find(activeDrainerHosts.begin(), activeDrainerHosts.end(), host) ==
        activeDrainerHosts.end()) {
      LOG(ERROR) << "Add drainer failed, host " << host << " is not active drainer";
      handleErrorCode(nebula::cpp2::ErrorCode::E_NO_VALID_DRAINER);
      onFinished();
      return;
    }
  }

  std::vector<kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::drainerKey(space), MetaKeyUtils::drainerVal(hosts));
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  LOG(INFO) << "Add drainer, spaceId " << space;
  auto result = doSyncPut(std::move(data));
  handleErrorCode(result);
  onFinished();
}

void RemoveDrainerProcessor::process(const cpp2::RemoveDrainerReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);

  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto drainerKey = MetaKeyUtils::drainerKey(space);
  auto ret = doGet(drainerKey);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      retCode = nebula::cpp2::ErrorCode::E_DRAINER_NOT_FOUND;
      LOG(ERROR) << "Remove drainer failed, drainer not exists.";
    } else {
      LOG(ERROR) << "Remove drainer failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    }
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  LOG(INFO) << "Remove drainer, spaceId " << space;
  auto batchHolder = std::make_unique<kvstore::BatchHolder>();
  batchHolder->remove(std::move(drainerKey));
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(batchHolder.get(), timeInMilliSec);
  auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
  doBatchOperation(std::move(batch));
}

void ListDrainersProcessor::process(const cpp2::ListDrainersReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());

  const auto& drainerKey = MetaKeyUtils::drainerKey(space);
  auto ret = doGet(drainerKey);
  if (!nebula::ok(ret) && nebula::error(ret) != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
    auto retCode = nebula::error(ret);
    LOG(ERROR) << "List drainer failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  // When the drainer does not exist, it returns success, but the drainers is empty.
  std::vector<cpp2::DrainerInfo> drainers;
  if (nebula::ok(ret)) {
    auto drainerHosts = MetaKeyUtils::parseDrainerHosts(nebula::value(ret));

    // For the slave cluster, meta knows the survival status of the drainer
    auto activeHostsRet =
        ActiveHostsMan::getActiveHosts(kvstore_,
                                       FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor,
                                       cpp2::HostRole::DRAINER);
    if (!nebula::ok(activeHostsRet)) {
      handleErrorCode(nebula::error(activeHostsRet));
      onFinished();
      return;
    }

    auto activeHosts = std::move(nebula::value(activeHostsRet));
    for (auto& host : drainerHosts) {
      cpp2::DrainerInfo drainer;
      drainer.host_ref() = host;
      if (std::find(activeHosts.begin(), activeHosts.end(), *drainer.host_ref()) !=
          activeHosts.end()) {
        drainer.status_ref() = cpp2::HostStatus::ONLINE;
      } else {
        drainer.status_ref() = cpp2::HostStatus::OFFLINE;
      }
      drainers.emplace_back(std::move(drainer));
    }
  }
  resp_.drainers_ref() = std::move(drainers);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
