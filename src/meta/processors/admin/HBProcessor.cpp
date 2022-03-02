/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/HBProcessor.h"

#include <folly/Format.h>

#include "common/encryption/License.h"
#include "common/time/WallClock.h"
#include "meta/ActiveHostsMan.h"
#include "meta/KVBasedClusterIdMan.h"
#include "meta/MetaVersionMan.h"

namespace nebula {
namespace meta {

HBCounters kHBCounters;

std::atomic<int64_t> HBProcessor::metaVersion_ = -1;

void HBProcessor::onFinished() {
  if (counters_) {
    stats::StatsManager::addValue(counters_->numCalls_);
    stats::StatsManager::addValue(counters_->latency_, this->duration_.elapsedInUSec());
  }

  Base::onFinished();
}

void HBProcessor::process(const cpp2::HBReq& req) {
  HostAddr host((*req.host_ref()).host, (*req.host_ref()).port);

  auto role = req.get_role();
  LOG(INFO) << "Receive heartbeat from " << host
            << ", role = " << apache::thrift::util::enumNameSafe(role);

  // Check current node number
  auto ret = checkNodeNumber(role, host);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(ret);
    onFinished();
    return;
  }

  std::vector<kvstore::KV> data;
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  if (role == cpp2::HostRole::STORAGE || role == cpp2::HostRole::META_LISTENER ||
      role == cpp2::HostRole::STORAGE_LISTENER) {
    if (role == cpp2::HostRole::STORAGE) {
      if (!ActiveHostsMan::machineRegisted(kvstore_, host)) {
        LOG(INFO) << "Machine " << host << " is not registed";
        handleErrorCode(nebula::cpp2::ErrorCode::E_MACHINE_NOT_FOUND);
        onFinished();
        return;
      }
    }

    // set or check storaged's cluster id
    ClusterID peerClusterId = req.get_cluster_id();
    if (peerClusterId == 0) {
      LOG(INFO) << "Set clusterId for new host " << host << "!";
      resp_.cluster_id_ref() = clusterId_;
    } else if (peerClusterId != clusterId_) {
      LOG(INFO) << "Reject wrong cluster host " << host << "!";
      handleErrorCode(nebula::cpp2::ErrorCode::E_WRONGCLUSTER);
      onFinished();
      return;
    }

    // set disk parts map
    if (req.disk_parts_ref().has_value()) {
      for (const auto& [spaceId, partDiskMap] : *req.get_disk_parts()) {
        for (const auto& [path, partList] : partDiskMap) {
          auto partListVal = MetaKeyUtils::diskPartsVal(partList);
          auto key = MetaKeyUtils::diskPartsKey(host, spaceId, path);
          data.emplace_back(key, partListVal);
        }
      }
    }
  }

  // update host info
  HostInfo info(time::WallClock::fastNowInMilliSec(), role, req.get_git_info_sha());
  if (req.leader_partIds_ref().has_value()) {
    ret = ActiveHostsMan::updateHostInfo(kvstore_, host, info, data, &*req.leader_partIds_ref());
  } else {
    ret = ActiveHostsMan::updateHostInfo(kvstore_, host, info, data);
  }
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(ret);
    onFinished();
    return;
  }

  // update host dir info
  if (req.get_role() == cpp2::HostRole::STORAGE || req.get_role() == cpp2::HostRole::GRAPH) {
    if (req.dir_ref().has_value()) {
      LOG(INFO) << folly::sformat("Update host {} dir info, root path: {}, data path size: {}",
                                  host.toString(),
                                  req.get_dir()->get_root(),
                                  req.get_dir()->get_data().size());
      data.emplace_back(std::make_pair(MetaKeyUtils::hostDirKey(host.host, host.port),
                                       MetaKeyUtils::hostDirVal(*req.get_dir())));
    }
  }

  // set update time and meta version
  auto lastUpdateTimeKey = MetaKeyUtils::lastUpdateTimeKey();
  auto lastUpdateTimeRet = doGet(lastUpdateTimeKey);
  if (nebula::ok(lastUpdateTimeRet)) {
    auto val = nebula::value(lastUpdateTimeRet);
    int64_t time = *reinterpret_cast<const int64_t*>(val.data());
    resp_.last_update_time_in_ms_ref() = time;
  } else if (nebula::error(lastUpdateTimeRet) == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
    resp_.last_update_time_in_ms_ref() = 0;
  }

  auto version = metaVersion_.load();
  if (version == -1) {
    metaVersion_.store(static_cast<int64_t>(MetaVersionMan::getMetaVersionFromKV(kvstore_)));
  }

  resp_.meta_version_ref() = metaVersion_.load();
  ret = doSyncPut(std::move(data));
  handleErrorCode(ret);
  onFinished();
}

// Checks the active node number of the given role
// Rejects heartbeat if number reaches the max value
// The license only limits the number of graph and storage nodes, other role types won't be checked.
nebula::cpp2::ErrorCode HBProcessor::checkNodeNumber(const cpp2::HostRole role,
                                                     const HostAddr& host) {
  // Get license instance
  auto licenseIns = encryption::License::getInstance();

  // Get max nodes alloed from license
  unsigned maxNodesNum = -1;
  if (role == cpp2::HostRole::GRAPH) {
    maxNodesNum = licenseIns->getContent()["graphdSpec"]["nodes"].asInt();
  } else if (role == cpp2::HostRole::STORAGE) {
    maxNodesNum = licenseIns->getContent()["storagedSpec"]["nodes"].asInt();
  } else {  // No need to check other role types
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  // Get active hosts
  auto activeHostsRet = ActiveHostsMan::getActiveHosts(kvstore_, 0, role);
  if (!nebula::ok(activeHostsRet)) {
    return nebula::error(activeHostsRet);
  }
  auto activeHosts = nebula::value(activeHostsRet);

  // No need to check hosts number if the host is already an active host
  if (std::find(activeHosts.begin(), activeHosts.end(), host) != activeHosts.end()) {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  // Compare current active hosts and max hosts allowed
  if ((role == cpp2::HostRole::GRAPH || role == cpp2::HostRole::STORAGE) &&
      activeHosts.size() >= maxNodesNum) {
    LOG(ERROR) << folly::sformat(
        "Current the number of active {} nodes has reach the node maximum value allowed, "
        "heartbeat "
        "from {} is rejected",
        apache::thrift::util::enumNameSafe(role),
        host.toString());
    return nebula::cpp2::ErrorCode::E_NODE_NUMBER_EXCEED_LIMIT;
  }

  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
