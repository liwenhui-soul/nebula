/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Memory.h"

#include <errno.h>

#include "common/base/Logging.h"

namespace nebula {

StatusOr<std::unique_ptr<MemInfo>> MemInfo::make() {
  std::unique_ptr<MemInfo> mem(new MemInfo);
  NG_RETURN_IF_ERROR(mem->init());
  return mem;
}

MemInfo::MemInfo() noexcept {
#ifndef PLATFORM_MACOS
  info_ = std::make_unique<struct sysinfo>();
#else
  vmStat_ = std::make_unique<vm_statistics64_data_t>();
#endif
}

Status MemInfo::init() {
#ifndef PLATFORM_MACOS
  if (sysinfo(info_.get()) == -1) {
    auto err = errno;
    return Status::Error("Fail to call sysinfo to get memory info, errno: %d", err);
  }
#else
  machPort_ = mach_host_self();
  count_ = HOST_VM_INFO64_COUNT;
  if (KERN_SUCCESS != host_page_size(machPort_, &pageSize_) ||
      KERN_SUCCESS !=
          host_statistics64(machPort_, HOST_VM_INFO64, (host_info64_t)vmStat_.get(), &count_)) {
    auto err = errno;
    return Status::Error("Fail to call sysinfo to get memory info, errno: %d", err);
  }
#endif
  return Status::OK();
}

}  // namespace nebula
