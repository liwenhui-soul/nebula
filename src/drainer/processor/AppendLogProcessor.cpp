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
    LOG(ERROR) << folly::stringPrintf("space %s not found", toSpaceName.c_str());
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
  lastLogIdRecv_ = req.get_last_log_id_sent();
  lastLogId_ = req.get_last_log_id_to_send();
  term_ = req.get_log_term();
  logStrs_ = req.get_log_str_list();
  cleanupData_ = req.get_cleanup_data();

  // The sender also ensures that only one request is being sent
  // at a time for the same part of the same space
  auto pair = std::make_pair(toSpaceId_, fromPartId_);
  auto iter = env_->requestOnGoing_.find(pair);
  if (iter != env_->requestOnGoing_.end()) {
    if (env_->requestOnGoing_[pair].load()) {
      LOG(ERROR) << "part is processing request";
      pushResultCode(nebula::cpp2::ErrorCode::E_REQ_CONFLICT);
      onFinished();
      return;
    }
  }
  env_->requestOnGoing_[pair].store(true);

  auto retCode = checkAndBuildContexts(req);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Failure build contexts!";
    env_->requestOnGoing_[pair].store(false);
    pushResultCode(retCode);
    onProcessFinished();
    onFinished();
    return;
  }

  VLOG(1) << "Write wal data to the latest wal file of the current part, partId " << fromPartId_;
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
      LOG(ERROR) << "makeDir " << datapath << " failed!";
      return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
    }
  }

  if (FileUtils::fileType(datapath.c_str()) != FileType::DIRECTORY) {
    LOG(ERROR) << datapath << " is not a directory!";
    return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
  }

  // 3)  create or check data/drainer/nebula/toSpaceId/cluster_space_id file
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
      LOG(ERROR) << "makeDir " << walPath_ << " failed";
      return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
    }
  }

  if (FileUtils::fileType(walPath_.c_str()) != FileType::DIRECTORY) {
    LOG(ERROR) << walPath_ << " is not directory";
    return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
  }

  // 5) create or check data/drainer/nebula/toSpaceId/frompartId/recv.log file
  recvLogFile_ = folly::stringPrintf("%s/%d/recv.log", datapath.c_str(), fromPartId_);
  wal();

  if (cleanupData_) {
    // TODO(pandasheep) clear the data in the space under slave cluster.
    // Clear wal, clear the recvLogFile, sendLogFile file
    wal_->reset();
    result = updateRecvLog(0);
    if (!result) {
      return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
    }
    sendLogFile_ = folly::stringPrintf("%s/%d/send.log", datapath.c_str(), fromPartId_);
    if (access(sendLogFile_.c_str(), 0) == 0) {
      result = updateSendLog(0);
      if (!result) {
        return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
      }
    }
  } else {
    // file exists and need check
    retCode = checkLastRecvLogId();
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return retCode;
    }
  }

  // Checks whether the drainer's wal lastlogId matches the stored lastLogIdRecv
  if (wal_->lastLogId() > lastLogIdRecv_) {
    wal_->rollbackToLog(lastLogIdRecv_);
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
      LOG(ERROR) << "Get target space vid type failed: " << vidType.status();
      return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    vType = vidType.value();

    auto vidLen = env_->schemaMan_->getSpaceVidLen(toSpaceId_);
    if (!vidLen.ok()) {
      LOG(ERROR) << "Get target space vid len failed: " << vidLen.status();
      return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    vLen = vidLen.value();
    env_->spaceVidTypeLen_.emplace(toSpaceId_, std::make_pair(vType, vLen));
  }

  if (vType != fromSpaceVidType || vLen != fromSpaceVidLen) {
    LOG(ERROR) << "The vidType of the source space and the target space are inconsistent.";
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
    LOG(ERROR) << writeRet.toString();
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
      LOG(ERROR) << "Not find space id " << toSpaceId_
                 << " in spaceClusters or spaceOldParts of env.";
      return false;
    }
    fromClusterId = clustetIter->second.first;
    toClusterId = clustetIter->second.second;
    fromPartNum = oldPartNumIter->second;
  } else {
    auto spaceMetaRet = DrainerCommon::readSpaceMeta(clusterSpaceIdFile_);
    if (!spaceMetaRet.ok()) {
      LOG(ERROR) << spaceMetaRet.status();
      return false;
    }
    std::tie(fromClusterId, fromSpaceId, fromPartNum, toClusterId) = spaceMetaRet.value();
    env_->spaceMatch_.emplace(toSpaceId_, fromSpaceId);
    env_->spaceClusters_.emplace(toSpaceId_, std::make_pair(fromClusterId, toClusterId));
    env_->spaceOldParts_.emplace(toSpaceId_, fromPartNum);
  }

  if (fromClusterId != fromClusterId_ || fromSpaceId != fromSpaceId_ ||
      fromPartNum != fromPartNum_ || toClusterId != FLAGS_cluster_id) {
    LOG(ERROR) << "The clusterId and spaceId of source and destination do not match."
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
      LOG(ERROR) << "Failed to open file " << recvLogFile_ << "errno(" << errno
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
    LOG(ERROR) << "Failed to read the file " << recvLogFile_;
    return false;
  }
  if (lseek(currFd, 0, SEEK_SET) < 0) {
    LOG(ERROR) << "Failed to seek the recv.log, space " << toSpaceId_ << " part " << fromPartId_
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
    LOG(ERROR) << "Bytes written:" << written << ", expected:" << val.size()
               << ", error:" << strerror(errno);
    close(currFd);
    env_->recvLogIdFd_.erase(key);
    return false;
  }
  fsync(currFd);
  return true;
}

bool AppendLogProcessor::updateSendLog(LogID lastLogIdSend) {
  auto key = std::make_pair(toSpaceId_, fromPartId_);
  auto sendLogFdIter = env_->sendLogIdFd_.find(key);
  if (sendLogFdIter == env_->sendLogIdFd_.end()) {
    int32_t fd = open(sendLogFile_.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_LARGEFILE, 0644);
    if (fd < 0) {
      LOG(ERROR) << "Failed to open file " << sendLogFile_ << "errno(" << errno
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
    LOG(ERROR) << "Failed to read the file " << sendLogFile_;
    return false;
  }
  if (lseek(currFd, 0, SEEK_SET) < 0) {
    LOG(ERROR) << "Failed to seek the send.log, space " << toSpaceId_ << " part " << fromPartId_
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
    LOG(ERROR) << "Bytes written:" << written << ", expected:" << val.size()
               << ", error:" << strerror(errno);
    close(currFd);
    env_->sendLogIdFd_.erase(key);
    return false;
  }
  fsync(currFd);
  return true;
}

nebula::cpp2::ErrorCode AppendLogProcessor::checkLastRecvLogId() {
  // By checking the existence of the recv.Log file to determine
  // whether it is the first time to receive data, or the first time to receive data after the
  // snapshot. Normally, before sending data for the first time, the recv.log file does not exist.
  // When sending snapshot data, the recv.log file exists, and 0 is written in it
  if (access(recvLogFile_.c_str(), 0) == 0) {
    auto key = std::make_pair(toSpaceId_, fromPartId_);
    auto recvLogFdIter = env_->recvLogIdFd_.find(key);
    if (recvLogFdIter == env_->recvLogIdFd_.end()) {
      int32_t fd = open(recvLogFile_.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_LARGEFILE, 0644);
      if (fd < 0) {
        LOG(ERROR) << "Failed to open file " << recvLogFile_ << "errno(" << errno
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
      LOG(ERROR) << "Failed to read the file " << recvLogFile_;
      return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
    }
    auto ret = pread(currFd, reinterpret_cast<char*>(&lastLogIdRecv), sizeof(LogID), 0);
    if (ret != static_cast<ssize_t>(sizeof(LogID))) {
      LOG(ERROR) << "Failed to read the file " << recvLogFile_ << " (errno: " << errno
                 << "): " << strerror(errno);
      close(currFd);
      env_->recvLogIdFd_.erase(key);
      return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
    }

    // the wal log data sent has expired
    // logId is continuous. But the term is not necessarily continuous
    if (lastLogIdRecv > lastLogIdRecv_) {
      LOG(ERROR) << "Sync data expired! lastLogIdRecv from Listener: " << lastLogIdRecv_
                 << ", lastLogIdRecv from drainer: " << lastLogIdRecv;
      // Maybe needs more detailed processing
      nextLastLogIdRecv_ = lastLogIdRecv;
      return nebula::cpp2::ErrorCode::E_LOG_STALE;
    } else if (lastLogIdRecv < lastLogIdRecv_) {
      // When sending snapshot data, the lastLogIdRecv in the request and in the file are both 0
      // When the wal data is sent for the first time after the snapshot,
      // the lastLogIdRecv in the request is committedLogId, and the lastLogIdRecv in the drainer
      // file is 0
      if (lastLogIdRecv != 0) {
        // There is a gap in the wal log data
        LOG(ERROR) << "Sync data has a gap! lastLogIdRecv from listener: " << lastLogIdRecv_
                   << ", lastLogIdRecv from drainer: " << lastLogIdRecv;
        nextLastLogIdRecv_ = lastLogIdRecv;
        return nebula::cpp2::ErrorCode::E_LOG_GAP;
      }
    }
  } else {
    // Regardless of receiving snapshot or receiving wal, when receiving for the first time, it
    // should be 0
    if (lastLogIdRecv_ != 0) {
      // There is a gap in the wal log data
      LOG(ERROR) << "Sync data has a gap! lastLogIdRecv from listener: " << lastLogIdRecv_
                 << ", drainer not receive log";
      nextLastLogIdRecv_ = 0;
      return nebula::cpp2::ErrorCode::E_LOG_GAP;
    }
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
  LogID firstId = lastLogIdRecv_ + 1;

  VLOG(2) << "Writing log [" << firstId << ", " << firstId + numLogs - 1 << "] to WAL";

  LogStrListIterator iter(firstId, term_, logStrs_);

  if (wal_->appendLogs(iter)) {
    if (numLogs != 0) {
      CHECK_EQ(firstId + numLogs - 1, wal_->lastLogId()) << "First Id is " << firstId;
    }

    // write recvLogFile_
    nextLastLogIdRecv_ = wal_->lastLogId();

    // When drainer writes wal successfully.But persistence failed, wal logs are to be rolled back.
    auto result = updateRecvLog(nextLastLogIdRecv_);
    if (!result) {
      return nebula::cpp2::ErrorCode::E_INVALID_DRAINER_STORE;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  } else {
    LOG_EVERY_N(WARNING, 100) << "Failed to append logs to WAL";
    return nebula::cpp2::ErrorCode::E_LOG_STALE;
  }
}

void AppendLogProcessor::onProcessFinished() {
  if (nextLastLogIdRecv_ != 0) {
    resp_.set_last_log_id(std::move(nextLastLogIdRecv_));
  }
}

}  // namespace drainer
}  // namespace nebula
