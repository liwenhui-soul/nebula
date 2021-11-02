/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DRAINER_APPENDLOGPROCESSOR_H_
#define DRAINER_APPENDLOGPROCESSOR_H_

#include "common/base/Base.h"
#include "common/base/ConcurrentLRUCache.h"
#include "drainer/CommonUtils.h"
#include "drainer/processor/BaseProcessor.h"
#include "interface/gen-cpp2/drainer_types.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace drainer {

extern ProcessorCounters kAppendLogCounters;

// Receive wal log data sent from the sync listener of the master cluster
// The directory structure of the data is as follows:
/* |--data/drainer
 * |----nebula
 * |------toSpaceId
 * |--------cluster_space.id (clusterId_spaceId from master and clusterId_spaceId from slave)
 * |--------partId1(from master space)
 * |----------wal
 * |----------recv.log(partId from master, last_log_id_recv, last_log_term_recv)
 * |----------send.log(partId from master, last_log_id_sent,last_log_term_sent)
 * |--------partId2
 */
class AppendLogProcessor : public BaseProcessor<cpp2::AppendLogRequest, cpp2::AppendLogResponse> {
 public:
  static AppendLogProcessor* instance(DrainerEnv* env,
                                      const ProcessorCounters* counters = &kAppendLogCounters) {
    return new AppendLogProcessor(env, counters);
  }

  void process(const cpp2::AppendLogRequest& req) override;

  void onProcessFinished() override;

 private:
  AppendLogProcessor(DrainerEnv* env, const ProcessorCounters* counters)
      : BaseProcessor<cpp2::AppendLogRequest, cpp2::AppendLogResponse>(env, counters) {}

  nebula::cpp2::ErrorCode checkAndBuildContexts(const cpp2::AppendLogRequest& req) override;
};

}  // namespace drainer
}  // namespace nebula

#endif  // DRAINER_APPENDLOGPROCESSOR_H_
