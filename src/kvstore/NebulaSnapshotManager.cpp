/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/NebulaSnapshotManager.h"

#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/LogEncoder.h"
#include "kvstore/RateLimiter.h"

DEFINE_uint32(snapshot_part_rate_limit,
              1024 * 1024 * 10,
              "max bytes of pulling snapshot for each partition in one second");
DEFINE_uint32(snapshot_batch_size, 1024 * 512, "batch size for snapshot, in bytes");

namespace nebula {
namespace kvstore {

const int32_t kReserveNum = 1024 * 4;

NebulaSnapshotManager::NebulaSnapshotManager() {
  // Snapshot rate is limited to FLAGS_snapshot_worker_threads * FLAGS_snapshot_part_rate_limit.
  // So by default, the total send rate is limited to 4 * 10Mb = 40Mb.
  LOG(INFO) << "Send snapshot is rate limited to " << FLAGS_snapshot_part_rate_limit
            << " for each part by default";
}

void NebulaSnapshotManager::accessAllRowsInSnapshot(std::shared_ptr<raftex::RaftPart> part,
                                                    raftex::SnapshotCallback cb) {
  static constexpr LogID kInvalidLogId = -1;
  static constexpr TermID kInvalidLogTerm = -1;
  std::vector<std::string> data;
  int64_t totalSize = 0;
  int64_t totalCount = 0;
  auto partPtr = dynamic_cast<Part*>(part.get());
  // Create a rocksdb snapshot
  auto snapshot = partPtr->engine()->GetSnapshot();
  SCOPE_EXIT {
    partPtr->engine()->ReleaseSnapshot(snapshot);
  };
  // Get the commit log id and commit log term of specified partition
  std::string val;
  auto commitRet =
      partPtr->engine()->get(NebulaKeyUtils::systemCommitKey(part->partitionId()), &val, snapshot);
  if (commitRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << folly::sformat("Cannot fetch the commit log id and term of space {} part {}",
                                partPtr->spaceId(),
                                partPtr->partitionId());
    cb(kInvalidLogId, kInvalidLogTerm, data, totalCount, totalSize, raftex::SnapshotStatus::FAILED);
    return;
  }
  CHECK_EQ(val.size(), sizeof(LogID) + sizeof(TermID));
  LogID commitLogId;
  TermID commitLogTerm;
  memcpy(reinterpret_cast<void*>(&commitLogId), val.data(), sizeof(LogID));
  memcpy(reinterpret_cast<void*>(&commitLogTerm), val.data() + sizeof(LogID), sizeof(TermID));

  LOG(INFO) << folly::sformat(
      "Space {} Part {} start send snapshot of commitLogId {} commitLogTerm {}, rate limited to "
      "{}, batch size is {}",
      partPtr->spaceId(),
      partPtr->partitionId(),
      commitLogId,
      commitLogTerm,
      FLAGS_snapshot_part_rate_limit,
      FLAGS_snapshot_batch_size);

  auto rateLimiter = std::make_unique<kvstore::RateLimiter>();
  auto tables = NebulaKeyUtils::snapshotPrefix(part->partitionId());
  for (const auto& prefix : tables) {
    if (!accessTable(partPtr,
                     snapshot,
                     prefix,
                     cb,
                     commitLogId,
                     commitLogTerm,
                     data,
                     totalCount,
                     totalSize,
                     rateLimiter.get())) {
      cb(commitLogId, commitLogTerm, data, totalCount, totalSize, raftex::SnapshotStatus::FAILED);
      return;
    }
  }
  if (!cb(commitLogId, commitLogTerm, data, totalCount, totalSize, raftex::SnapshotStatus::DONE)) {
    cb(commitLogId, commitLogTerm, data, totalCount, totalSize, raftex::SnapshotStatus::FAILED);
  }
}

nebula::cpp2::ErrorCode NebulaSnapshotManager::prefixScan(Part* part,
                                                          const std::string& prefix,
                                                          std::unique_ptr<KVIterator>* iter,
                                                          const void* snapshot) {
  // Use kv-engine directly to avoid acquire lock in kvstore, and we **DO NOT** check leader lease:
  // 1. To avoid lots of snapshot impact leader stability
  // 2. The snapshot of state machine could be applied to other nodes even if leader has changed
  return part->engine()->prefix(prefix, iter, snapshot);
}

// Promise is set in callback. Access part of the data, and try to send to
// peers. If send failed, will return false.
bool NebulaSnapshotManager::accessTable(Part* part,
                                        const void* snapshot,
                                        const std::string& prefix,
                                        raftex::SnapshotCallback& cb,
                                        LogID commitLogId,
                                        TermID commitLogTerm,
                                        std::vector<std::string>& data,
                                        int64_t& totalCount,
                                        int64_t& totalSize,
                                        kvstore::RateLimiter* rateLimiter) {
  std::unique_ptr<KVIterator> iter;

  auto ret = prefixScan(part, prefix, &iter, snapshot);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(2) << "[spaceId:" << part->spaceId() << ", partId:" << part->partitionId()
            << "] access prefix failed"
            << ", error code:" << static_cast<int32_t>(ret);
    iter.reset(nullptr);
    return false;
  }
  data.reserve(kReserveNum);
  size_t batchSize = 0;
  while (iter && iter->valid()) {
    if (batchSize >= FLAGS_snapshot_batch_size) {
      rateLimiter->consume(static_cast<double>(batchSize),                        // toConsume
                           static_cast<double>(FLAGS_snapshot_part_rate_limit),   // rate
                           static_cast<double>(FLAGS_snapshot_part_rate_limit));  // burstSize
      if (cb(commitLogId,
             commitLogTerm,
             data,
             totalCount,
             totalSize,
             raftex::SnapshotStatus::IN_PROGRESS)) {
        data.clear();
        batchSize = 0;
      } else {
        VLOG(2) << "[spaceId:" << part->spaceId() << ", partId:" << part->partitionId()
                << "] send snapshot failed";
        iter.reset(nullptr);
        return false;
      }
    }
    auto key = iter->key();
    auto val = iter->val();
    data.emplace_back(encodeKV(key, val));
    batchSize += data.back().size();
    totalSize += data.back().size();
    totalCount++;
    iter->next();
  }
  return true;
}

}  // namespace kvstore
}  // namespace nebula
