/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef DRAINER_COMMON_H_
#define DRAINER_COMMON_H_

#include <folly/concurrency/ConcurrentHashMap.h>

#include "common/base/Base.h"
#include "common/meta/IndexManager.h"
#include "common/meta/SchemaManager.h"
#include "common/meta/ServiceManager.h"
#include "common/stats/StatsManager.h"
#include "kvstore/KVStore.h"
#include "kvstore/wal/FileBasedWal.h"

namespace nebula {
namespace drainer {

struct ProcessorCounters {
  stats::CounterId numCalls_;
  stats::CounterId numErrors_;
  stats::CounterId latency_;

  virtual ~ProcessorCounters() = default;

  virtual void init(const std::string &counterName) {
    if (!numCalls_.valid()) {
      numCalls_ = stats::StatsManager::registerStats("num_" + counterName, "rate, sum");
      numErrors_ =
          stats::StatsManager::registerStats("num_" + counterName + "_errors", "rate, sum");
      latency_ = stats::StatsManager::registerHisto(
          counterName + "_latency_us", 1000, 0, 20000, "avg, p75, p95, p99");
      VLOG(1) << "Succeeded in initializing the ProcessorCounters instance";
    } else {
      VLOG(1) << "ProcessorCounters instance has been initialized";
    }
  }
};

/**
 * @brief The environment of the drainer service.
 */
class DrainerEnv {
 public:
  meta::SchemaManager *schemaMan_{nullptr};
  meta::ServiceManager *serviceMan_{nullptr};
  meta::IndexManager *indexMan_{nullptr};
  meta::MetaClient *metaClient_{nullptr};

  // local drainer host
  HostAddr localHost_;

  // the path of drainer data, usually is data/drainer/nebula.
  std::string drainerPath_;

  // Write wal info
  // writes sync listener data to drainer wal.
  // and read drainer wal data and then send to storage client or meta client.
  // The spaceId here is from the slave cluster, so it is unique.
  std::unordered_map<GraphSpaceID,
                     std::unordered_map<PartitionID, std::shared_ptr<nebula::wal::FileBasedWal>>>
      wals_;

  // get space meta info
  // slave cluster spaceId-> <masterClusterId, slaveClusterId>
  std::unordered_map<GraphSpaceID, std::pair<ClusterID, ClusterID>> spaceClusters_;
  // slave cluster spaceId-> oldPartsNum
  std::unordered_map<GraphSpaceID, int32_t> spaceOldParts_;
  // slave cluster toSpaceId_ -> fromSpaceId
  std::unordered_map<GraphSpaceID, GraphSpaceID> spaceMatch_;

  // slave cluster spaceId-> newPartsNum
  std::unordered_map<GraphSpaceID, int32_t> spaceNewParts_;

  // lock
  std::unordered_map<std::pair<GraphSpaceID, PartitionID>, std::atomic<bool>> requestOnGoing_;

  // Todo(pandasheep)
  std::shared_ptr<kvstore::DiskManager> diskMan_;

  // slave cluster toSpaceId_ -> <SpaceVidType, VidLen>
  std::unordered_map<GraphSpaceID, std::pair<nebula::cpp2::PropertyType, int32_t>> spaceVidTypeLen_;

  // <slave cluster toSpaceId, fromPartId> -> recv.log fd
  folly::ConcurrentHashMap<std::pair<GraphSpaceID, PartitionID>, int32_t> recvLogIdFd_;

  // <slave cluster toSpaceId, fromPartId> -> send.log fd
  folly::ConcurrentHashMap<std::pair<GraphSpaceID, PartitionID>, int32_t> sendLogIdFd_;
};

class DrainerCommon final {
 public:
  /**
   * @brief Write cluster_space_id file
   *
   * @param path The path of cluster_space_id file
   * @param fromClusterId
   * @param fromSpaceId
   * @param fromPartNum
   * @param toClusterId
   * @return Status Status::OK if successful.
   */
  static Status writeSpaceMeta(const std::string &path,
                               ClusterID fromClusterId,
                               GraphSpaceID fromSpaceId,
                               int32_t fromPartNum,
                               ClusterID toClusterId);

  /**
   * @brief Read cluster_space_id file
   *
   * @param path The path of cluster_space_id file
   * @return StatusOr<std::tuple<ClusterID, GraphSpaceID, int32_t, ClusterID>>
   * If successful, return the corresponding values.
   */
  static StatusOr<std::tuple<ClusterID, GraphSpaceID, int32_t, ClusterID>> readSpaceMeta(
      const std::string &path);
};

}  // namespace drainer
}  // namespace nebula

#endif  // DRAINER_COMMON_H_
