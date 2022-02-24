/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_KV_SYNCDATAPROCESSOR_H_
#define STORAGE_KV_SYNCDATAPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kSyncDataCounters;

/**
 * @brief Sync storage(vertex, tag, edge, index) data.
 * Receive storage data from drainer and write to part leader.
 *
 */
class SyncDataProcessor : public BaseProcessor<cpp2::ExecResponse> {
 public:
  static SyncDataProcessor* instance(StorageEnv* env,
                                     const ProcessorCounters* counters = &kSyncDataCounters) {
    return new SyncDataProcessor(env, counters);
  }

  void process(const cpp2::SyncDataRequest& req);

 private:
  explicit SyncDataProcessor(StorageEnv* env, const ProcessorCounters* counters)
      : BaseProcessor<cpp2::ExecResponse>(env, counters) {}

 private:
  GraphSpaceID spaceId_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_KV_SYNCDATAPROCESSOR_H_
