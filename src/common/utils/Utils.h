/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_UTILS_UTILS_H_
#define COMMON_UTILS_UTILS_H_

#include "common/base/Base.h"
#include "common/datatypes/HostAddr.h"

namespace nebula {
class Utils final {
 public:
  // Calculate the admin service address based on the storage service address
  static HostAddr getAdminAddrFromStoreAddr(HostAddr storeAddr) {
    if (storeAddr == HostAddr("", 0)) {
      return storeAddr;
    }
    return HostAddr(storeAddr.host, storeAddr.port - 1);
  }

  static HostAddr getStoreAddrFromAdminAddr(HostAddr adminAddr) {
    if (adminAddr == HostAddr("", 0)) {
      return adminAddr;
    }
    return HostAddr(adminAddr.host, adminAddr.port + 1);
  }

  // Calculate the raft service address based on the storage service address
  static HostAddr getRaftAddrFromStoreAddr(const HostAddr& srvcAddr) {
    if (srvcAddr == HostAddr("", 0)) {
      return srvcAddr;
    }
    return HostAddr(srvcAddr.host, srvcAddr.port + 1);
  }

  static HostAddr getStoreAddrFromRaftAddr(const HostAddr& raftAddr) {
    if (raftAddr == HostAddr("", 0)) {
      return raftAddr;
    }
    return HostAddr(raftAddr.host, raftAddr.port - 1);
  }

  static HostAddr getInternalAddrFromStoreAddr(HostAddr adminAddr) {
    if (adminAddr == HostAddr("", 0)) {
      return adminAddr;
    }
    return HostAddr(adminAddr.host, adminAddr.port - 2);
  }

  static std::string getLocalTime() {
    time_t now = std::time(NULL);
    struct tm p;
    localtime_r(&now, &p);
    char tmp_buff[50];
    strftime(tmp_buff, sizeof(tmp_buff), "%Y-%m-%d %H:%M:%S", &p);
    return std::string(tmp_buff);
  }

  static int runCommand(const std::string& command, int& exitCode, std::string& out) {
    FILE* f = popen(command.c_str(), "r");
    if (f == nullptr) {
      return -1;
    }

    char buf[1025];
    size_t len;
    std::string tempOut = "";
    do {
      len = fread(buf, 1, 1024, f);
      if (len > 0) {
        buf[len] = '\0';
        tempOut += buf;
      }
    } while (len == 1024);

    if (ferror(f)) {
      // Something is wrong
      fclose(f);
      return -1;
    }

    out = tempOut;
    exitCode = pclose(f);
    return 0;
  }

  // transform ` to '
  static std::string processSpecChar(const std::string& in) {
    std::string out;
    out.reserve(in.size());
    for (auto& c : in) {
      if (c == '`') {
        out.push_back('\'');
      } else {
        out.push_back(c);
      }
    }
    return out;
  }
};
}  // namespace nebula
#endif  // COMMON_UTILS_UTILS_H_
