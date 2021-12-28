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

class DrainerEnv {
 public:
  meta::SchemaManager *schemaMan_{nullptr};
  meta::ServiceManager *serviceMan_{nullptr};
  meta::IndexManager *indexMan_{nullptr};
  meta::MetaClient *metaClient_{nullptr};

  // local drainer host
  HostAddr localHost_;

  // data/drainer/nebula
  std::string drainerPath_;

  // write wal info, writes sync listener data to drainer wal.
  // and read drainer wal data to storage client or meta client(schema data)
  // The spaceId here is the slave cluster spaceId, so it is unique.
  std::unordered_map<GraphSpaceID,
                     std::unordered_map<PartitionID, std::shared_ptr<nebula::wal::FileBasedWal>>>
      wals_;

  // get from space meta
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

  // add cache for acceleration
  // slave cluster toSpaceId_ -> <SpaceVidType, VidLen>
  std::unordered_map<GraphSpaceID, std::pair<nebula::cpp2::PropertyType, int32_t>> spaceVidTypeLen_;

  // recv part
  // slave cluster <toSpaceId_, fromPartId_> -> recv.log fd
  folly::ConcurrentHashMap<std::pair<GraphSpaceID, PartitionID>, int32_t> recvLogIdFd_;

  // send part
  // slave cluster <toSpaceId_, fromPartId_> -> send.log fd
  folly::ConcurrentHashMap<std::pair<GraphSpaceID, PartitionID>, int32_t> sendLogIdFd_;
};

class DrainerCommon final {
 public:
  // write cluster_space_id file
  static Status writeSpaceMeta(const std::string &path,
                               ClusterID fromClusterId,
                               GraphSpaceID fromSpaceId,
                               int32_t fromPartNum,
                               ClusterID toClusterId);

  // read cluster_space_id file
  static StatusOr<std::tuple<ClusterID, GraphSpaceID, int32_t, ClusterID>> readSpaceMeta(
      const std::string &path);
};

}  // namespace drainer
}  // namespace nebula

#endif  // DRAINER_COMMON_H_
