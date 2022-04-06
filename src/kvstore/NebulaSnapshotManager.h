/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_NEBULASNAPSHOTMANAGER_H
#define KVSTORE_NEBULASNAPSHOTMANAGER_H

#include <folly/TokenBucket.h>

#include "common/base/Base.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/RateLimiter.h"
#include "kvstore/raftex/SnapshotManager.h"

namespace nebula {
namespace kvstore {

class NebulaSnapshotManager : public raftex::SnapshotManager {
 public:
  NebulaSnapshotManager();

  /**
   * @brief Scan all data and trigger callback to send to peer
   *
   * @param spaceId
   * @param partId
   * @param cb Callback when scan some amount of data
   */
  void accessAllRowsInSnapshot(std::shared_ptr<raftex::RaftPart> part,
                               raftex::SnapshotCallback cb) override;

 private:
  /**
   * @brief Collect some data by prefix, and trigger callback when scan some amount of data
   *
   * @param spaceId
   * @param partId
   * @param prefix Prefix to scan
   * @param cb Callback when scan some amount of data
   * @param data Data container
   * @param totalCount Data count
   * @param totalSize Data size in bytes
   * @param rateLimiter Rate limiter to restrict sending speed
   * @return True if succeed. False if failed.
   */
  bool accessTable(Part* part,
                   const void* snapshot,
                   const std::string& prefix,
                   raftex::SnapshotCallback& cb,
                   LogID commitLogId,
                   TermID commitLogTerm,
                   std::vector<std::string>& data,
                   int64_t& totalCount,
                   int64_t& totalSize,
                   kvstore::RateLimiter* rateLimiter);

  nebula::cpp2::ErrorCode prefixScan(Part* part,
                                     const std::string& prefix,
                                     std::unique_ptr<KVIterator>* iter,
                                     const void* snapshot);
};

}  // namespace kvstore
}  // namespace nebula
#endif
