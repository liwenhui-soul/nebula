/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
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

// Receive wal log data sent from the sync listener of the master cluster.
// drainer use tospace spaceId and master cluster partId as directory name.
// This class only processes files related to receiving data.
// The directory structure of the data is as follows:
/* |--data/drainer
 * |----nebula
 * |------toSpaceId
 * |--------cluster_space_id (clusterId_spaceId_parts from master and clusterId from slave)
 * |--------partId1(from master space)
 * |----------wal
 * |----------recv.log(last_log_id_recv)
 * |----------send.log(last_log_id_sent)  // If no data is sent, it may not exist.
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

  std::string recvLogFile() { return recvLogFile_; }

 private:
  AppendLogProcessor(DrainerEnv* env, const ProcessorCounters* counters)
      : BaseProcessor<cpp2::AppendLogRequest, cpp2::AppendLogResponse>(env, counters) {}

  // Check whether it is legal
  nebula::cpp2::ErrorCode checkAndBuildContexts(const cpp2::AppendLogRequest& req) override;

  // The space vidtype, vidLen of the master cluster and the slave cluster must be the same
  nebula::cpp2::ErrorCode checkSpaceVidType(const cpp2::AppendLogRequest& req);

  // write clusterSpaceId file
  // format: master_clusterId, master_spaceId, partNum, slave_clusterId
  bool writeSpaceMetaFile(ClusterID fromClusterId,
                          GraphSpaceID fromSpaceId,
                          int32_t partNum,
                          ClusterID toClusterId);

  // Check the cache first, and then open the clusterSpaceId file
  // if it does not exist in the cache.
  bool checkSpaceMeta();

  // Update lastLogRecv
  bool updateRecvLog(LogID lastLogIdRecv);

  // Get lastLogRecv and check
  nebula::cpp2::ErrorCode checkLastRecvLogId();

  // For the continuity of logId, do nothing
  bool preProcessLog(LogID logId, TermID termId, ClusterID clusterId, const std::string& log);

  // Create or open a wal file, and write additional wal data
  nebula::cpp2::ErrorCode appendWalData();

  // set wal_
  void wal();

 private:
  // The spaceId of the slave cluster
  GraphSpaceID toSpaceId_{0};

  // For the master cluster
  ClusterID fromClusterId_{0};
  GraphSpaceID fromSpaceId_{0};
  bool syncMeta_;
  int32_t fromPartNum_{0};
  PartitionID fromPartId_{0};
  LogID lastLogIdRecv_{0};
  LogID lastLogId_;
  TermID term_;
  std::vector<nebula::cpp2::LogEntry> logStrs_;

  bool sendSnapshot_;

  // format: master_clusterId, master_spaceId, slave_clusterId, slave_spaceId
  std::string clusterSpaceIdFile_;
  // wal directory
  std::string walPath_;

  // format: last_log_id_recv
  std::string recvLogFile_;

  // The result of the response, next time hope to receive logId is (nextLastLogIdRecv_ + 1)
  LogID nextLastLogIdRecv_{0};

  // Write-ahead Log
  std::shared_ptr<wal::FileBasedWal> wal_;
};

}  // namespace drainer
}  // namespace nebula

#endif  // DRAINER_APPENDLOGPROCESSOR_H_
