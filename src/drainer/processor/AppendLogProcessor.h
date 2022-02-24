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

/**
 * @brief Receive wal log data sent from the sync listener of the master cluster.
 * Drainer uses tospace spaceId and partId of master cluster as directory name.
 * This class only processes files related to receive data.
 *
 * The directory structure of the data is as follows:
 * |--data/drainer
 * |----nebula
 * |------toSpaceId
 * |--------cluster_space_id (clusterId_spaceId_parts from master and clusterId from slave)
 * |--------partId1(from master space)
 * |----------wal
 * |----------recv.log(last_log_id_recv)
 * |----------send.log(last_log_id_sent)  // If no data has sent, it may not exist.
 * |--------partId2
 */
class AppendLogProcessor : public BaseProcessor<cpp2::AppendLogRequest, cpp2::AppendLogResponse> {
 public:
  static AppendLogProcessor* instance(DrainerEnv* env,
                                      const ProcessorCounters* counters = &kAppendLogCounters) {
    return new AppendLogProcessor(env, counters);
  }

  /**
   * @brief Process the data in the request and write it to the corresponding local directory.
   *
   * @param req
   */
  void process(const cpp2::AppendLogRequest& req) override;

  void onProcessFinished() override;

  std::string recvLogFile() {
    return recvLogFile_;
  }

 private:
  AppendLogProcessor(DrainerEnv* env, const ProcessorCounters* counters)
      : BaseProcessor<cpp2::AppendLogRequest, cpp2::AppendLogResponse>(env, counters) {}

  /**
   * @brief Do some preparatory work.
   *
   * @param req
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode checkAndBuildContexts(const cpp2::AppendLogRequest& req) override;

  /**
   * @brief Check whether the space vid type and vid length of the corresponding spaces
   * in the master cluster and slave cluster are consistent.
   * notice: This interface only handles storage data.
   *
   * @param req
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode checkSpaceVidType(const cpp2::AppendLogRequest& req);

  /**
   * @brief Write space meta info to the file，filename is cluster_space_id.
   * Format: master_clusterId, master_spaceId, partNum, slave_clusterId
   *
   * @param fromClusterId
   * @param fromSpaceId
   * @param partNum
   * @param toClusterId
   * @return True if successful
   */
  bool writeSpaceMetaFile(ClusterID fromClusterId,
                          GraphSpaceID fromSpaceId,
                          int32_t partNum,
                          ClusterID toClusterId);

  /**
   * @brief Check space meta info.
   * Check space meta info from the cache first. If it does not exist in the cache,
   * open the clusterSpaceId file to check.
   *
   * @return True if check successful.
   */
  bool checkSpaceMeta();

  /**
   * @brief Update lastLogIdRecv in recv.log
   *
   * @param lastLogIdRecv
   * @return True if update successful.
   */
  bool updateRecvLog(LogID lastLogIdRecv);

  /**
   * @brief Update lastLogIdSend in send.log
   *
   * @param lastLogIdSend
   * @return True if update successful.
   */
  bool updateSendLog(LogID lastLogIdSend);

  /**
   * @brief Get lastLogRecv and check
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode checkLastRecvLogId();

  /**
   * @brief For the continuity of logId, do nothing
   *
   * @param logId
   * @param termId
   * @param clusterId
   * @param log
   * @return true
   * @return false
   */
  bool preProcessLog(LogID logId, TermID termId, ClusterID clusterId, const std::string& log);

  /**
   * @brief Create or open a wal file, and write additional wal data
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode appendWalData();

  /**
   * @brief Set wal_ variable
   */
  void wal();

 private:
  // The spaceId of the slave cluster
  GraphSpaceID toSpaceId_{0};

  // For the master cluster
  ClusterID fromClusterId_{0};
  GraphSpaceID fromSpaceId_{0};
  // Whether sync meta data
  bool syncMeta_;
  int32_t fromPartNum_{0};
  PartitionID fromPartId_{0};
  LogID lastLogIdRecv_{0};
  LogID lastLogId_;
  TermID term_;
  std::vector<nebula::cpp2::LogEntry> logStrs_;

  // Whether to clear data before receiving snapshot data
  bool cleanupData_;

  // Format: master_clusterId, master_spaceId, slave_clusterId, slave_spaceId
  std::string clusterSpaceIdFile_;
  // wal directory
  std::string walPath_;

  // Format: last_log_id_recv
  std::string recvLogFile_;

  // Format: last_log_id_send
  std::string sendLogFile_;

  // The result of the response, next time hope to receive logId is (nextLastLogIdRecv_ + 1)
  LogID nextLastLogIdRecv_{0};

  std::shared_ptr<wal::FileBasedWal> wal_;
};

}  // namespace drainer
}  // namespace nebula

#endif  // DRAINER_APPENDLOGPROCESSOR_H_
