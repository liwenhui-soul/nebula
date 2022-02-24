/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "drainer/CommonUtils.h"

namespace nebula {
namespace drainer {

Status DrainerCommon::writeSpaceMeta(const std::string& path,
                                     ClusterID fromClusterId,
                                     GraphSpaceID fromSpaceId,
                                     int32_t fromPartNum,
                                     ClusterID toClusterId) {
  int32_t fd = open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    return Status::Error("Failed to open the file %s, error: %s", path.c_str(), strerror(errno));
  }

  std::string val;
  val.reserve(sizeof(ClusterID) * 2 + sizeof(GraphSpaceID) + sizeof(int32_t));
  val.append(reinterpret_cast<const char*>(&fromClusterId), sizeof(ClusterID))
      .append(reinterpret_cast<const char*>(&fromSpaceId), sizeof(GraphSpaceID))
      .append(reinterpret_cast<const char*>(&fromPartNum), sizeof(int32_t))
      .append(reinterpret_cast<const char*>(&toClusterId), sizeof(ClusterID));
  ssize_t written = write(fd, val.c_str(), val.size());
  if (written != (ssize_t)val.size()) {
    close(fd);
    return Status::Error(
        "Bytes written: %zd, expected: %luerror: %s", written, val.size(), strerror(errno));
  }
  close(fd);
  return Status::OK();
}

StatusOr<std::tuple<ClusterID, GraphSpaceID, int32_t, ClusterID>> DrainerCommon::readSpaceMeta(
    const std::string& path) {
  ClusterID fromClusterId;
  GraphSpaceID fromSpaceId;
  int32_t fromPartNum;
  ClusterID toClusterId;

  int32_t fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return Status::Error("Failed to open the file %s, error: %s", path.c_str(), strerror(errno));
  }

  auto ret = pread(fd, reinterpret_cast<char*>(&fromClusterId), sizeof(ClusterID), 0);
  if (ret != static_cast<ssize_t>(sizeof(ClusterID))) {
    close(fd);
    return Status::Error("Failed to read the file %s.", path.c_str());
  }

  ret = pread(fd, reinterpret_cast<char*>(&fromSpaceId), sizeof(GraphSpaceID), sizeof(ClusterID));
  if (ret != static_cast<ssize_t>(sizeof(GraphSpaceID))) {
    close(fd);
    return Status::Error("Failed to read the file %s.", path.c_str());
  }

  // partNum
  auto offset = sizeof(ClusterID) + sizeof(GraphSpaceID);
  ret = pread(fd, reinterpret_cast<char*>(&fromPartNum), sizeof(int32_t), offset);
  if (ret != static_cast<ssize_t>(sizeof(int32_t))) {
    close(fd);
    return Status::Error("Failed to read the file %s.", path.c_str());
  }

  offset = sizeof(ClusterID) + sizeof(GraphSpaceID) + sizeof(int32_t);
  ret = pread(fd, reinterpret_cast<char*>(&toClusterId), sizeof(ClusterID), offset);
  if (ret != static_cast<ssize_t>(sizeof(ClusterID))) {
    close(fd);
    return Status::Error("Failed to read the file %s.", path.c_str());
  }
  close(fd);

  std::tuple<ClusterID, GraphSpaceID, int32_t, ClusterID> result;
  std::get<0>(result) = fromClusterId;
  std::get<1>(result) = fromSpaceId;
  std::get<2>(result) = fromPartNum;
  std::get<3>(result) = toClusterId;
  return result;
}

}  // namespace drainer
}  // namespace nebula
