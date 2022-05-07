/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "drainer/processor/AppendLogProcessor.h"

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"
#include "common/utils/LogStrListIterator.h"
#include "kvstore/wal/FileBasedWal.h"

DECLARE_int32(wal_ttl);
DECLARE_int64(wal_file_size);
DECLARE_int32(wal_buffer_size);
DECLARE_bool(wal_sync);
DECLARE_int32(cluster_id);

namespace nebula {
namespace drainer {

using nebula::fs::FileType;
using nebula::fs::FileUtils;

ProcessorCounters kAppendLogCounters;

void AppendLogProcessor::process(const cpp2::AppendLogRequest& req) {
  auto toSpaceName = req.get_to_space_name();
  auto space = env_->schemaMan_->toGraphSpaceID(toSpaceName);
  if (!space.ok()) {
    VLOG(2) << folly::stringPrintf("space %s not found", toSpaceName.c_str());
    pushResultCode(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND);
    onFinished();
    return;
  }
  toSpaceId_ = space.value();
  fromClusterId_ = req.get_clusterId();

  // when it is meta listener data, fromSpaceId_ is sync space Id.
  fromSpaceId_ = req.get_space();
  fromPartId_ = req.get_part();
  syncMeta_ = req.get_sync_meta();

  // If it is meta listener data, the fromPartId_ is 0.
  if (syncMeta_) {
    CHECK_EQ(fromPartId_, 0);
  }

  fromPartNum_ = req.get_part_num();

  // When receiving a snapshot data, lastLogIdRecv_, lastLogId_, term_ are 0
  lastLogIdRecv_ = req.get_last_log_id_sent();
  lastLogId_ = req.get_last_log_id_to_send();
  term_ = req.get_log_term();
  logStrs_ = req.get_log_str_list();
  needCleanup_ = req.get_need_cleanup();
  isSnapshot_ = req.get_is_snapshot();
  snapshotFinished_ = req.get_snapshot_finished();
  snapshotCommitLogId_ = req.get_snapshot_commitLogId();

  // The sender also ensures that only one request is being sent
  // at a time for the same part of the same space
  auto pair = std::make_pair(toSpaceId_, fromPartId_);
  auto iter = env_->requestOnGoing_.find(pair);
  if (iter != env_->requestOnGoing_.end()) {
    if (env_->requestOnGoing_[pair].load()) {
      VLOG(2) << "part is processing request";
      pushResultCode(nebula::cpp2::ErrorCode::E_REQ_CONFLICT);
      onFinished();
      return;
    }
  }
  env_->requestOnGoing_[pair].store(true);

  auto retCode = checkAndBuildContexts(req);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(2) << "Failure build contexts!";
    env_->requestOnGoing_[pair].store(false);
    pushResultCode(retCode);
    onProcessFinished();
    onFinished();
    return;
  }

  VLOG(2) << "Write wal data to the latest wal file of the current part, partId " << fromPartId_;
  retCode = appendWalData();
  env_->requestOnGoing_[pair].store(false);
  pushResultCode(retCode);
  onProcessFinished();
  onFinished();
  return;
}

void AppendLogProcessor::wal() {
  auto spaceIter = env_->wals_.find(toSpaceId_);
  if (spaceIter != env_->wals_.end()) {
    auto partIter = spaceIter->second.find(fromPartId_);
    if (partIter != spaceIter->second.end()) {
      wal_ = partIter->second;
    }
  }

  if (!wal_) {
    wal::FileBasedWalInfo info;
    info.idStr_ = folly::stringPrintf("[Space: %d, Part: %d] ", toSpaceId_, fromPartId_);
    info.spaceId_ = toSpaceId_;
    info.partId_ = fromPartId_;
    wal::FileBasedWalPolicy policy;
    policy.fileSize = FLAGS_wal_file_size;
    policy.bufferSize = FLAGS_wal_buffer_size;
    policy.sync = FLAGS_wal_sync;

    // Assuming that the wal logId from sync listener to drainer is continuous
    // That is, the received wal log contains heartbeat information
    env_->wals_[toSpaceId_].emplace(
        fromPartId_,
        wal::FileBasedWal::getWal(
            walPath_,
            std::move(info),
            std::move(policy),
            [this](LogID logId, TermID logTermId, ClusterID logClusterId, const std::string& log) {
              return this->preProcessLog(logId, logTermId, logClusterId, log);
            },
            env_->diskMan_,
            true));
    wal_ = env_->wals_[toSpaceId_][fromPartId_];
  }
}

nebula::cpp2::ErrorCode AppendLogProcessor::checkAndBuildContexts(
    const cpp2::AppendLogRequest& req) {
  // 1) check space vid type and vid len
  auto retCode = checkSpaceVidType(req);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return retCode;
  }

  // 2) create or check data/drainer/nebula/toSpaceId directory
  auto datapath = folly::stringPrintf("%s/nebula/%d", env_->drainerPath_.c_str(), toSpaceId_);
  if (FileUtils::fileType(datapath.c_str()) == FileType::NOTEXIST) {
    if (!FileUtils::makeDir(datapath)) {
      VLOG(2) << "MakeDir " << datapath << " failed!";
      return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
    }
  }

  if (FileUtils::fileType(datapath.c_str()) != FileType::DIRECTORY) {
    VLOG(2) << "Data path " << datapath << " is not a directory!";
    return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
  }

  // 3) create or check data/drainer/nebula/toSpaceId/cluster_space_id file
  // If it is meta data. partId is 0, But partNum is the parts of this space.
  clusterSpaceIdFile_ = folly::stringPrintf("%s/cluster_space_id", datapath.c_str());
  bool result;
  if (access(clusterSpaceIdFile_.c_str(), 0) != 0) {
    // file not exists
    result = writeSpaceMetaFile(fromClusterId_, fromSpaceId_, fromPartNum_, FLAGS_cluster_id);
  } else {
    // file exists and need check
    result = checkSpaceMeta();
  }
  if (!result) {
    return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
  }

  // 4) create or check data/drainer/nebula/toSpaceId/frompartId/wal/ directory
  // Note that the partId here is from the master cluster.
  // If fromPartId_ is 0, it is meta listener data.
  walPath_ = folly::stringPrintf("%s/%d/wal", datapath.c_str(), fromPartId_);
  if (FileUtils::fileType(walPath_.c_str()) == FileType::NOTEXIST) {
    if (!FileUtils::makeDir(walPath_)) {
      VLOG(2) << "MakeDir " << walPath_ << " failed";
      return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
    }
  }

  if (FileUtils::fileType(walPath_.c_str()) != FileType::DIRECTORY) {
    VLOG(2) << "Wal path " << walPath_ << " is not directory";
    return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
  }

  // 5) create or check data/drainer/nebula/toSpaceId/frompartId/recv.log file
  recvLogFile_ = folly::stringPrintf("%s/%d/recv.log", datapath.c_str(), fromPartId_);
  wal();

  intervalLogFile_ = folly::stringPrintf("%s/%d/interval.log", datapath.c_str(), fromPartId_);
  if (isSnapshot_) {
    if (needCleanup_) {
      // TODO(pandasheep) clear the data in the space under slave cluster.
      // Clear wal, clear the recvLogFile, reset interval, clear sendLogFile file
      wal_->reset();
      result = updateRecvLog(0);
      if (!result) {
        return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
      }

      // reset interval value, create the interval.log if it does not exist
      result = updateLogInterval(0);
      if (!result) {
        return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
      }

      // reset send.log
      sendLogFile_ = folly::stringPrintf("%s/%d/send.log", datapath.c_str(), fromPartId_);
      if (access(sendLogFile_.c_str(), 0) == 0) {
        result = updateSendLog(0);
        if (!result) {
          return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
        }
      }
    }
    // recv.log, interval.log, send.log file exist
    retCode = checkLastRecvLogIdFromSnapshot();
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return retCode;
    }
  } else {
    // file exists and need check
    retCode = checkLastRecvLogIdForWal();
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return retCode;
    }
  }

  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode AppendLogProcessor::checkSpaceVidType(const cpp2::AppendLogRequest& req) {
  // meta listener data not need check
  if (syncMeta_) {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  if (!req.space_vid_type_ref().has_value() || !req.space_vid_len_ref().has_value()) {
    return nebula::cpp2::ErrorCode::E_SPACE_MISMATCH;
  }
  auto fromSpaceVidType = *req.space_vid_type_ref();
  auto fromSpaceVidLen = *req.space_vid_len_ref();

  nebula::cpp2::PropertyType vType;
  int32_t vLen;

  auto spaceIter = env_->spaceVidTypeLen_.find(toSpaceId_);
  if (spaceIter != env_->spaceVidTypeLen_.end()) {
    vType = spaceIter->second.first;
    vLen = spaceIter->second.second;
  } else {
    auto vidType = env_->schemaMan_->getSpaceVidType(toSpaceId_);
    if (!vidType.ok()) {
      VLOG(2) << "Get target space vid type failed: " << vidType.status();
      return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    vType = vidType.value();

    auto vidLen = env_->schemaMan_->getSpaceVidLen(toSpaceId_);
    if (!vidLen.ok()) {
      VLOG(2) << "Get target space vid len failed: " << vidLen.status();
      return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    vLen = vidLen.value();
    env_->spaceVidTypeLen_.emplace(toSpaceId_, std::make_pair(vType, vLen));
  }

  if (vType != fromSpaceVidType || vLen != fromSpaceVidLen) {
    VLOG(2) << "The vidType of the source space and the target space are inconsistent.";
    return nebula::cpp2::ErrorCode::E_SPACE_MISMATCH;
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

bool AppendLogProcessor::writeSpaceMetaFile(ClusterID fromClusterId,
                                            GraphSpaceID fromSpaceId,
                                            int32_t fromPartNum,
                                            ClusterID toClusterId) {
  auto writeRet = DrainerCommon::writeSpaceMeta(
      clusterSpaceIdFile_, fromClusterId, fromSpaceId, fromPartNum, toClusterId);
  if (!writeRet.ok()) {
    VLOG(2) << writeRet.toString();
    return false;
  }
  env_->spaceMatch_.emplace(toSpaceId_, fromSpaceId);
  env_->spaceClusters_.emplace(toSpaceId_, std::make_pair(fromClusterId, toClusterId));
  env_->spaceOldParts_.emplace(toSpaceId_, fromPartNum);
  return true;
}

bool AppendLogProcessor::checkSpaceMeta() {
  // read fromCluster, fromSpaceId, partNum, toCluster
  ClusterID fromClusterId;
  GraphSpaceID fromSpaceId;
  int32_t fromPartNum;
  ClusterID toClusterId;

  auto spaceIter = env_->spaceMatch_.find(toSpaceId_);
  // Get from cache
  if (spaceIter != env_->spaceMatch_.end()) {
    fromSpaceId = spaceIter->second;
    auto clustetIter = env_->spaceClusters_.find(toSpaceId_);
    auto oldPartNumIter = env_->spaceOldParts_.find(toSpaceId_);
    if (clustetIter == env_->spaceClusters_.end() || oldPartNumIter == env_->spaceOldParts_.end()) {
      VLOG(2) << "Not find space id " << toSpaceId_ << " in spaceClusters or spaceOldParts of env.";
      return false;
    }
    fromClusterId = clustetIter->second.first;
    toClusterId = clustetIter->second.second;
    fromPartNum = oldPartNumIter->second;
  } else {
    auto spaceMetaRet = DrainerCommon::readSpaceMeta(clusterSpaceIdFile_);
    if (!spaceMetaRet.ok()) {
      VLOG(2) << spaceMetaRet.status();
      return false;
    }
    std::tie(fromClusterId, fromSpaceId, fromPartNum, toClusterId) = spaceMetaRet.value();
    env_->spaceMatch_.emplace(toSpaceId_, fromSpaceId);
    env_->spaceClusters_.emplace(toSpaceId_, std::make_pair(fromClusterId, toClusterId));
    env_->spaceOldParts_.emplace(toSpaceId_, fromPartNum);
  }

  if (fromClusterId != fromClusterId_ || fromSpaceId != fromSpaceId_ ||
      fromPartNum != fromPartNum_ || toClusterId != FLAGS_cluster_id) {
    VLOG(2) << "The clusterId and spaceId of source and destination do not match."
            << " source clusterId " << fromClusterId << " expect clusterId " << fromClusterId_
            << " source spaceId " << fromSpaceId << " expect spaceId " << fromSpaceId_
            << " source space partNum " << fromPartNum << " expect spaceId " << fromPartNum_
            << " desc clusterId " << toClusterId << " expect clusterId " << FLAGS_cluster_id;
    return false;
  }
  return true;
}

bool AppendLogProcessor::updateRecvLog(LogID lastLogIdRecv) {
  auto key = std::make_pair(toSpaceId_, fromPartId_);
  auto recvLogFdIter = env_->recvLogIdFd_.find(key);
  if (recvLogFdIter == env_->recvLogIdFd_.end()) {
    int32_t fd = open(recvLogFile_.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_LARGEFILE, 0644);
    if (fd < 0) {
      VLOG(2) << "Failed to open file " << recvLogFile_ << "errno(" << errno
              << "): " << strerror(errno);
      return false;
    }
    auto ret = env_->recvLogIdFd_.insert(key, fd).second;
    if (!ret) {
      close(fd);
    }
  }

  int32_t currFd;
  try {
    currFd = env_->recvLogIdFd_.at(key);
  } catch (...) {
    VLOG(2) << "Failed to read the file " << recvLogFile_;
    return false;
  }
  if (lseek(currFd, 0, SEEK_SET) < 0) {
    VLOG(2) << "Failed to seek the recv.log, space " << toSpaceId_ << " part " << fromPartId_
            << "error: " << strerror(errno);
    close(currFd);
    env_->recvLogIdFd_.erase(key);
    return false;
  }

  std::string val;
  val.reserve(sizeof(LogID));
  val.append(reinterpret_cast<const char*>(&lastLogIdRecv), sizeof(LogID));
  ssize_t written = write(currFd, val.c_str(), val.size());
  if (written != (ssize_t)val.size()) {
    VLOG(2) << "Bytes written:" << written << ", expected:" << val.size()
            << ", error:" << strerror(errno);
    close(currFd);
    env_->recvLogIdFd_.erase(key);
    return false;
  }
  fsync(currFd);
  return true;
}

bool AppendLogProcessor::updateLogInterval(LogID interval) {
  auto key = std::make_pair(toSpaceId_, fromPartId_);
  int32_t fd = open(intervalLogFile_.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_LARGEFILE, 0644);
  if (fd < 0) {
    LOG(ERROR) << "Failed to open file " << intervalLogFile_ << "errno(" << errno
               << "): " << strerror(errno);
    env_->logIntervals_.erase(key);
    return false;
  }
  if (lseek(fd, 0, SEEK_SET) < 0) {
    LOG(ERROR) << "Failed to seek the interval.log, space " << toSpaceId_ << " part " << fromPartId_
               << "error: " << strerror(errno);
    close(fd);
    env_->logIntervals_.erase(key);
    return false;
  }

  std::string val;
  val.reserve(sizeof(LogID));
  val.append(reinterpret_cast<const char*>(&interval), sizeof(LogID));
  ssize_t written = write(fd, val.c_str(), val.size());
  if (written != (ssize_t)val.size()) {
    LOG(ERROR) << "Bytes written:" << written << ", expected:" << val.size()
               << ", error:" << strerror(errno);
    close(fd);
    env_->logIntervals_.erase(key);
    return false;
  }

  env_->logIntervals_.insert_or_assign(key, interval);

  fsync(fd);
  close(fd);
  return true;
}

bool AppendLogProcessor::updateSendLog(LogID lastLogIdSend) {
  auto key = std::make_pair(toSpaceId_, fromPartId_);
  auto sendLogFdIter = env_->sendLogIdFd_.find(key);
  if (sendLogFdIter == env_->sendLogIdFd_.end()) {
    int32_t fd = open(sendLogFile_.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_LARGEFILE, 0644);
    if (fd < 0) {
      VLOG(2) << "Failed to open file " << sendLogFile_ << "errno(" << errno
              << "): " << strerror(errno);
      return false;
    }
    auto ret = env_->sendLogIdFd_.insert(key, fd).second;
    if (!ret) {
      close(fd);
    }
  }

  int32_t currFd;
  try {
    currFd = env_->sendLogIdFd_.at(key);
  } catch (...) {
    VLOG(2) << "Failed to read the file " << sendLogFile_;
    return false;
  }
  if (lseek(currFd, 0, SEEK_SET) < 0) {
    VLOG(2) << "Failed to seek the send.log, space " << toSpaceId_ << " part " << fromPartId_
            << "error: " << strerror(errno);
    close(currFd);
    env_->sendLogIdFd_.erase(key);
    return false;
  }

  std::string val;
  val.reserve(sizeof(LogID));
  val.append(reinterpret_cast<const char*>(&lastLogIdSend), sizeof(LogID));
  ssize_t written = write(currFd, val.c_str(), val.size());
  if (written != (ssize_t)val.size()) {
    VLOG(2) << "Bytes written:" << written << ", expected:" << val.size()
            << ", error:" << strerror(errno);
    close(currFd);
    env_->sendLogIdFd_.erase(key);
    return false;
  }
  fsync(currFd);
  return true;
}

nebula::cpp2::ErrorCode AppendLogProcessor::checkLastRecvLogIdFromSnapshot() {
  // The lastLogIdRecv_ is always 0 during the process of receiving the snapshot
  if (lastLogIdRecv_ != 0) {
    // There is a gap in the wal log data
    VLOG(2) << "Sync data from snapshot of listener, lastLogIdRecv_ " << lastLogIdRecv_;
    nextLastLogIdRecv_ = 0;
    return nebula::cpp2::ErrorCode::E_LOG_GAP;
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode AppendLogProcessor::checkLastRecvLogIdForWal() {
  auto key = std::make_pair(toSpaceId_, fromPartId_);
  // Indicates that interval.log has been read
  if (env_->logIntervals_.find(key) != env_->logIntervals_.end()) {
    logInterval_ = env_->logIntervals_.at(key);
  } else {
    // Check if the interval.log file exists. Read if it exists.
    if (access(intervalLogFile_.c_str(), 0) == 0) {
      int32_t fd = open(intervalLogFile_.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_LARGEFILE, 0644);
      if (fd < 0) {
        VLOG(2) << "Failed to open file " << intervalLogFile_ << "errno(" << errno
                << "): " << strerror(errno);
        return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
      }
      LogID logInterval;
      auto ret = pread(fd, reinterpret_cast<char*>(&logInterval), sizeof(LogID), 0);
      if (ret != static_cast<ssize_t>(sizeof(LogID))) {
        VLOG(2) << "Failed to read the file " << intervalLogFile_ << " (errno: " << errno
                << "): " << strerror(errno);
        close(fd);
        return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
      }
      close(fd);
      env_->logIntervals_.insert_or_assign(key, logInterval);
      logInterval_ = env_->logIntervals_.at(key);
    }
  }

  // Normally, before sending data for the first time, the recv.log file does not exist.
  // When sending snapshot data, the recv.log file exists, and 0 is written in it
  if (access(recvLogFile_.c_str(), 0) == 0) {
    auto recvLogFdIter = env_->recvLogIdFd_.find(key);
    if (recvLogFdIter == env_->recvLogIdFd_.end()) {
      int32_t fd = open(recvLogFile_.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_LARGEFILE, 0644);
      if (fd < 0) {
        VLOG(2) << "Failed to open file " << recvLogFile_ << "errno(" << errno
                << "): " << strerror(errno);
        return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
      }
      auto ret = env_->recvLogIdFd_.insert(key, fd).second;
      if (!ret) {
        close(fd);
      }
    }

    LogID lastLogIdRecv;
    int32_t currFd;
    try {
      currFd = env_->recvLogIdFd_.at(key);
    } catch (...) {
      VLOG(2) << "Failed to read the file " << recvLogFile_;
      return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
    }
    auto ret = pread(currFd, reinterpret_cast<char*>(&lastLogIdRecv), sizeof(LogID), 0);
    if (ret != static_cast<ssize_t>(sizeof(LogID))) {
      VLOG(2) << "Failed to read the file " << recvLogFile_ << " (errno: " << errno
              << "): " << strerror(errno);
      close(currFd);
      env_->recvLogIdFd_.erase(key);
      return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
    }

    // The wal log sent has expired.
    // LogId is continuous. But the term is not necessarily continuous
    CHECK_GE(lastLogIdRecv_ + logInterval_, 0);
    CHECK_GE(lastLogIdRecv - logInterval_, 0);
    if (lastLogIdRecv > lastLogIdRecv_ + logInterval_) {
      VLOG(2) << "Sync data expired! lastLogIdRecv from Listener: " << lastLogIdRecv_
              << " logInterval is " << logInterval_
              << ", lastLogIdRecv from drainer: " << lastLogIdRecv;
      // Maybe needs more detailed processing
      nextLastLogIdRecv_ = lastLogIdRecv - logInterval_;
      return nebula::cpp2::ErrorCode::E_LOG_STALE;
    } else if (lastLogIdRecv < lastLogIdRecv_ + logInterval_) {
      // When the wal data is sent for the first time after the snapshot,
      // the lastLogIdRecv in the request is committedLogId, and the lastLogIdRecv in the drainer
      if (lastLogIdRecv != 0) {
        // There is a gap in the wal log data
        VLOG(2) << "Sync data has a gap! lastLogIdRecv from listener: " << lastLogIdRecv_
                << ", lastLogIdRecv from drainer: " << lastLogIdRecv;
        nextLastLogIdRecv_ = lastLogIdRecv - logInterval_;
        return nebula::cpp2::ErrorCode::E_LOG_GAP;
      }
    }
  } else {
    // When receiving data for the first time from wal, it should be 0
    if (lastLogIdRecv_ != 0) {
      // There is a gap in the wal log data
      VLOG(2) << "Sync data has a gap! lastLogIdRecv from listener: " << lastLogIdRecv_
              << ", drainer not receive log";
      nextLastLogIdRecv_ = 0;
      return nebula::cpp2::ErrorCode::E_LOG_GAP;
    }
  }
  // Checks whether the drainer's wal lastlogId matches the stored lastLogIdRecv
  if (wal_->lastLogId() > lastLogIdRecv_ + logInterval_) {
    wal_->rollbackToLog(lastLogIdRecv_ + logInterval_);
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

// For the continuity of logId, do nothing
bool AppendLogProcessor::preProcessLog(LogID, TermID, ClusterID, const std::string&) {
  return true;
}

// Create or open a wal file, and write additional wal data
nebula::cpp2::ErrorCode AppendLogProcessor::appendWalData() {
  // Append new logs
  size_t numLogs = logStrs_.size();

  if (isSnapshot_) {
    // When receiving a snapshot, lastLogIdRecv_ and lastLogId_ are both 0
    // Use wal lastLogId directly
    LogID walLastLogId = wal_->lastLogId();
    LogID firstId = walLastLogId + 1;

    VLOG(2) << "Writing snapshot data [" << firstId << ", " << firstId + numLogs - 1 << "] to WAL";

    LogStrListIterator iter(firstId, term_, logStrs_);
    if (wal_->appendLogs(iter)) {
      auto lastLogId = wal_->lastLogId();
      if (numLogs != 0) {
        CHECK_EQ(firstId + numLogs - 1, lastLogId) << "First Id is " << firstId;
      }

      // If the snapshot is sent, calculate the interval logId
      if (snapshotFinished_) {
        auto logInterval = lastLogId - snapshotCommitLogId_;
        // set logInterval value
        auto result = updateLogInterval(logInterval);
        if (!result) {
          return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
        }

        // Set recv.log when snapshot has finished to send
        result = updateRecvLog(lastLogId);
        if (!result) {
          return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
        }
        LOG(INFO) << "Receive snapshot data completed!";
      }
      // In the process of receiving snapshots, recv.log is always 0
      nextLastLogIdRecv_ = 0;
    } else {
      VLOG(1) << "Failed to append snapshot data to WAL";
      return nebula::cpp2::ErrorCode::E_LOG_STALE;
    }
  } else {
    LogID firstId = lastLogIdRecv_ + logInterval_ + 1;

    VLOG(2) << "Writing log [" << firstId << ", " << firstId + numLogs - 1 << "] to WAL";

    LogStrListIterator iter(firstId, term_, logStrs_);

    if (wal_->appendLogs(iter)) {
      auto lastLogId = wal_->lastLogId();
      if (numLogs != 0) {
        CHECK_EQ(firstId + numLogs - 1, lastLogId) << "First Id is " << firstId;
      }

      // write recvLogFile_
      nextLastLogIdRecv_ = lastLogId - logInterval_;

      // When drainer writes wal successfully.But persistence failed, wal logs are to be rolled
      // back.
      auto result = updateRecvLog(lastLogId);
      if (!result) {
        return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
      }
    } else {
      VLOG(1) << "Failed to append logs to WAL";
      return nebula::cpp2::ErrorCode::E_LOG_STALE;
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void AppendLogProcessor::onProcessFinished() {
  if (nextLastLogIdRecv_ != 0) {
    resp_.last_matched_log_id_ref() = std::move(nextLastLogIdRecv_);
  }
}

}  // namespace drainer
}  // namespace nebula
