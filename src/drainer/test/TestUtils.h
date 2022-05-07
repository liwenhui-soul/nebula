/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef DRAINER_TEST_TESTUTILS_H_
#define DRAINER_TEST_TESTUTILS_H_

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "drainer/processor/AppendLogProcessor.h"
#include "kvstore/LogEncoder.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"

namespace nebula {
namespace drainer {

using nebula::fs::FileType;
using nebula::fs::FileUtils;

bool writeClusterSpaceIdFile(const std::string& datapath,
                             ClusterID fromClusterId,
                             GraphSpaceID fromSpaceId,
                             int32_t partNum,
                             ClusterID toClusterId) {
  int32_t fd = open(datapath.c_str(), O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    return false;
  }

  std::string val;
  val.reserve(sizeof(ClusterID) * 2 + sizeof(GraphSpaceID) * 2 + sizeof(int32_t));
  val.append(reinterpret_cast<const char*>(&fromClusterId), sizeof(ClusterID))
      .append(reinterpret_cast<const char*>(&fromSpaceId), sizeof(GraphSpaceID))
      .append(reinterpret_cast<const char*>(&partNum), sizeof(int32_t))
      .append(reinterpret_cast<const char*>(&toClusterId), sizeof(ClusterID));
  ssize_t written = write(fd, val.c_str(), val.size());
  if (written != (ssize_t)val.size()) {
    LOG(ERROR) << "bytesWritten:" << written << ", expected:" << val.size()
               << ", error:" << strerror(errno);
    close(fd);
    return false;
  }
  close(fd);
  return true;
}

bool writeRecvLogFile(const std::string& datapath, LogID lastLogIdRecv) {
  int32_t fd = open(datapath.c_str(), O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    return false;
  }

  std::string val;
  val.reserve(sizeof(LogID));
  val.append(reinterpret_cast<const char*>(&lastLogIdRecv), sizeof(LogID));
  ssize_t written = write(fd, val.c_str(), val.size());
  if (written != (ssize_t)val.size()) {
    LOG(ERROR) << "bytesWritten:" << written << ", expected:" << val.size()
               << ", error:" << strerror(errno);
    close(fd);
    return false;
  }
  close(fd);
  return true;
}

bool writeSendLogFile(const std::string& datapath, LogID lastLogIdSend) {
  int32_t fd = open(datapath.c_str(), O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    return false;
  }

  std::string val;
  val.reserve(sizeof(LogID));
  val.append(reinterpret_cast<const char*>(&lastLogIdSend), sizeof(LogID));
  ssize_t written = write(fd, val.c_str(), val.size());
  if (written != (ssize_t)val.size()) {
    LOG(ERROR) << "bytesWritten:" << written << ", expected:" << val.size()
               << ", error:" << strerror(errno);
    close(fd);
    return false;
  }
  close(fd);
  return true;
}

// The directory structure of the data is as follows:
/* |--data/drainer
 * |----nebula
 * |------toSpaceId
 * |--------cluster_space_id (clusterId_spaceId_parts from master and clusterId from slave)
 * |--------partId1(from master space)
 * |----------wal
 * |----------recv.log(last_log_id_recv)
 * |----------send.log(last_log_id_sent)
 * |--------partId2
 */
// Create an empty directory structure without wal log.
bool mockData(const std::string& dataPath, GraphSpaceID spaceId) {
  auto path = folly::stringPrintf("%s/%d", dataPath.c_str(), spaceId);
  if (FileUtils::fileType(path.c_str()) == FileType::NOTEXIST) {
    if (!FileUtils::makeDir(path)) {
      return false;
    }
  }
  if (FileUtils::fileType(path.c_str()) != FileType::DIRECTORY) {
    return false;
  }
  // create data/drainer/nebula/toSpaceId/cluster_space_id file
  auto clusterSpaceIdFile = folly::stringPrintf("%s/cluster_space_id", path.c_str());
  auto result = writeClusterSpaceIdFile(clusterSpaceIdFile, 1, 1, 10, spaceId);
  if (!result) {
    return false;
  }

  // ten parts
  for (int part = 0; part < 10; part++) {
    // create or check data/drainer/nebula/toSpaceId/frompartId/wal/ directory
    auto walPath = folly::stringPrintf("%s/%d/wal", path.c_str(), part);
    if (FileUtils::fileType(walPath.c_str()) == FileType::NOTEXIST) {
      if (!FileUtils::makeDir(walPath)) {
        LOG(ERROR) << "makeDir " << walPath << " failed";
        return false;
      }
    }
    if (FileUtils::fileType(walPath.c_str()) != FileType::DIRECTORY) {
      return false;
    }

    // create data/drainer/nebula/toSpaceId/frompartId/recv.log file
    auto recvLogFile = folly::stringPrintf("%s/%d/recv.log", path.c_str(), part);
    result = writeRecvLogFile(recvLogFile, 0);
    if (!result) {
      return false;
    }

    // create data/drainer/nebula/toSpaceId/frompartId/send.log file
    auto sendLogFile = folly::stringPrintf("%s/%d/send.log", path.c_str(), part);
    result = writeSendLogFile(sendLogFile, 0);
    if (!result) {
      return false;
    }
  }
  return true;
}

bool removeData(const std::string& dataPath) {
  return fs::FileUtils::remove(dataPath.c_str(), true);
}

bool encodeValue(const meta::NebulaSchemaProvider* schema,
                 const std::vector<Value>& props,
                 std::string& val) {
  RowWriterV2 writer(schema);
  for (size_t i = 0; i < props.size(); i++) {
    auto r = writer.setValue(i, props[i]);
    if (r != WriteResult::SUCCEEDED) {
      LOG(ERROR) << "Invalid prop " << i;
      return false;
    }
  }
  auto ret = writer.finish();
  if (ret != WriteResult::SUCCEEDED) {
    LOG(ERROR) << "Failed to write data";
    return false;
  }
  val = std::move(writer).moveEncodedStr();
  return true;
}

// use for AdHocSchemaManager, not metaclient
StatusOr<cpp2::AppendLogRequest> mockAppendLogReq(meta::SchemaManager* schemaMan,
                                                  GraphSpaceID space,
                                                  std::string spaceName,
                                                  int32_t partNum,
                                                  PartitionID partId,
                                                  int32_t vidLen,
                                                  nebula::cpp2::PropertyType vidType) {
  cpp2::AppendLogRequest req;
  req.clusterId_ref() = 1;
  req.space_ref() = space;
  req.space_vid_type_ref() = vidType;
  req.space_vid_len_ref() = vidLen;
  req.part_num_ref() = partNum;
  req.part_ref() = partId;
  req.last_log_id_sent_ref() = 0;
  req.log_term_ref() = 1;
  req.need_cleanup_ref() = false;
  req.is_snapshot_ref() = false;
  req.to_space_name_ref() = spaceName;

  auto verticesPart = mock::MockData::mockVerticesofPart(partNum);
  std::vector<nebula::mock::VertexData> data = verticesPart[partId];

  LOG(INFO) << "Build AppendLogRequest...";
  LOG(INFO) << "Vertex data size " << data.size();

  auto sourceDataSize = data.size();

  int count = 0;
  std::vector<nebula::cpp2::LogEntry> logStrVec;

  for (auto& vertex : data) {
    nebula::cpp2::LogEntry log;
    log.cluster_ref() = 1;
    TagID tagId = vertex.tId_;
    std::string value;
    auto key = NebulaKeyUtils::tagKey(vidLen, partId, vertex.vId_, tagId);
    auto schema = schemaMan->getTagSchema(space, tagId);
    if (!schema) {
      LOG(ERROR) << "Invalid tagId " << tagId;
      return Status::Error("Invalid tagId %d", tagId);
    }
    auto encodeRet = encodeValue(schema.get(), vertex.props_, value);
    if (!encodeRet) {
      LOG(ERROR) << "Encode failed, tagid %d" << tagId;
      return Status::Error("Encode failed, tagid %d", tagId);
    }
    log.log_str_ref() = encodeMultiValues(kvstore::OP_PUT, key, value);
    logStrVec.emplace_back(std::move(log));
    count++;
  }
  req.log_str_list_ref() = std::move(logStrVec);
  req.last_log_id_to_send_ref() = count;

  EXPECT_EQ(sourceDataSize, count);
  return req;
}

// use for ServerBasedSchemaManager, has metaclient
StatusOr<cpp2::AppendLogRequest> mockAppendLogWithMetaClientReq(meta::SchemaManager* schemaMan,
                                                                GraphSpaceID space,
                                                                std::string spaceName,
                                                                int32_t partNum,
                                                                PartitionID partId,
                                                                int32_t vidLen,
                                                                nebula::cpp2::PropertyType vidType,
                                                                TagID tagId) {
  cpp2::AppendLogRequest req;
  req.clusterId_ref() = 1;
  req.space_ref() = space;
  req.space_vid_type_ref() = vidType;
  req.space_vid_len_ref() = vidLen;
  req.part_num_ref() = partNum;
  req.part_ref() = partId;
  req.last_log_id_sent_ref() = 0;
  req.log_term_ref() = 1;
  req.need_cleanup_ref() = false;
  req.to_space_name_ref() = spaceName;

  auto verticesPart = mock::MockData::mockVerticesofPart(partNum);
  std::vector<nebula::mock::VertexData> data = verticesPart[partId];

  LOG(INFO) << "Build AppendLogRequest...";
  LOG(INFO) << "Vertex data size " << data.size();

  auto sourceDataSize = data.size();

  size_t count = 0;
  std::vector<nebula::cpp2::LogEntry> logStrVec;

  // only used player data, player tagId is 1
  for (auto& vertex : data) {
    if (vertex.tId_ != 1) {
      continue;
    }
    nebula::cpp2::LogEntry log;
    log.cluster_ref() = 1;

    std::string value;
    auto key = NebulaKeyUtils::tagKey(vidLen, partId, vertex.vId_, tagId);
    auto schema = schemaMan->getTagSchema(space, tagId);
    if (!schema) {
      LOG(ERROR) << "Invalid tagId " << tagId;
      return Status::Error("Invalid tagId %d", tagId);
    }
    auto encodeRet = encodeValue(schema.get(), vertex.props_, value);
    if (!encodeRet) {
      LOG(ERROR) << "Encode failed, tagid %d" << tagId;
      return Status::Error("Encode failed, tagid %d", tagId);
    }
    log.log_str_ref() = encodeMultiValues(kvstore::OP_PUT, key, value);
    logStrVec.emplace_back(std::move(log));
    count++;
  }
  req.log_str_list_ref() = std::move(logStrVec);
  req.last_log_id_to_send_ref() = count;

  auto result = sourceDataSize >= count;
  EXPECT_TRUE(result);
  return req;
}

// recv.log(last_log_id_recv)
StatusOr<LogID> readRecvLogFile(const std::string& recvLogFile) {
  if (access(recvLogFile.c_str(), 0) != 0) {
    // file not exists
    return Status::Error("%s file not exists.", recvLogFile.c_str());
  }
  int32_t fd = open(recvLogFile.c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(ERROR) << "Failed to open the file " << recvLogFile << " error(" << errno
               << "): " << strerror(errno);
    return Status::Error("Open %s failed.", recvLogFile.c_str());
  }

  LogID lastLogIdRecv;
  CHECK_EQ(pread(fd, reinterpret_cast<char*>(&lastLogIdRecv), sizeof(LogID), 0),
           static_cast<ssize_t>(sizeof(LogID)));
  close(fd);

  return lastLogIdRecv;
}

// recv.log(last_log_id_recv)
StatusOr<LogID> readInterverLogFile(const std::string& intervalLogFile) {
  if (access(intervalLogFile.c_str(), 0) != 0) {
    // file not exists
    return Status::Error("%s file not exists.", intervalLogFile.c_str());
  }
  int32_t fd = open(intervalLogFile.c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(ERROR) << "Failed to open the file " << intervalLogFile << " error(" << errno
               << "): " << strerror(errno);
    return Status::Error("Open %s failed.", intervalLogFile.c_str());
  }

  LogID logInterval;
  CHECK_EQ(pread(fd, reinterpret_cast<char*>(&logInterval), sizeof(LogID), 0),
           static_cast<ssize_t>(sizeof(LogID)));
  close(fd);

  return logInterval;
}

}  // namespace drainer
}  // namespace nebula

#endif  // DRAINER_TEST_TESTUTILS_H_
