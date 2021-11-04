/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef DRAINER_COMMON_H_
#define DRAINER_COMMON_H_

#include "common/base/Base.h"
#include "common/meta/IndexManager.h"
#include "common/meta/SchemaManager.h"
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

  virtual void init(const std::string& counterName) {
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
  meta::SchemaManager* schemaMan_{nullptr};
  meta::MetaClient* metaClient_{nullptr};

  // data/drainer/nebula
  std::string drainerPath_;

  std::unordered_map<GraphSpaceID,
                     std::unordered_map<PartitionID, std::shared_ptr<nebula::wal::FileBasedWal>>>
      wals_;

  // lock
  std::unordered_map<std::pair<GraphSpaceID, PartitionID>, std::atomic<bool>> requestOnGoing_;

  std::shared_ptr<kvstore::DiskManager> diskMan_;
};

}  // namespace drainer
}  // namespace nebula

#endif  // DRAINER_COMMON_H_
