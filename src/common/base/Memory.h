/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_MEMORY_H_
#define COMMON_BASE_MEMORY_H_
#include "common/base/CommonMacro.h"
#ifndef PLATFORM_MACOS
#include <sys/sysinfo.h>
#else
#include <mach/mach.h>
#endif

#include <cstdint>
#include <memory>

#include "common/base/StatusOr.h"
#include "common/cpp/helpers.h"

namespace nebula {

class MemInfo final : protected cpp::NonCopyable, protected cpp::NonMovable {
 public:
  static StatusOr<std::unique_ptr<MemInfo>> make();

  uint64_t totalInKB() const {
#ifndef PLATFORM_MACOS
    return (info_->totalram * info_->mem_unit) >> 10;
#else
    return ((vmStat_->free_count + vmStat_->active_count + vmStat_->inactive_count +
             vmStat_->speculative_count + vmStat_->wire_count + vmStat_->compressor_page_count) *
            pageSize_) >>
           10;
#endif
  }

  uint64_t freeInKB() const {
#ifndef PLATFORM_MACOS
    return (info_->freeram * info_->mem_unit) >> 10;
#else
    return (vmStat_->free_count * pageSize_) >> 10;
#endif
  }

  uint64_t bufferInKB() const {
#ifndef PLATFORM_MACOS
    return (info_->bufferram * info_->mem_unit) >> 10;
#else
    return ((vmStat_->purgeable_count + vmStat_->external_page_count) * pageSize_) >> 10;
#endif
  }

  uint64_t usedInKB() const {
#ifndef PLATFORM_MACOS
    return totalInKB() - freeInKB() - bufferInKB();
#else
    return ((vmStat_->active_count + vmStat_->inactive_count + vmStat_->speculative_count +
             vmStat_->wire_count + vmStat_->compressor_page_count - vmStat_->purgeable_count -
             vmStat_->external_page_count) *
            pageSize_) >>
           10;
#endif
  }

  bool hitsHighWatermark(float ratio = 0.8f) const { return usedInKB() > totalInKB() * ratio; }

 private:
  MemInfo() noexcept;

  Status init();

#ifndef PLATFORM_MACOS
  std::unique_ptr<struct sysinfo> info_;
#else
  vm_size_t pageSize_;
  mach_port_t machPort_;
  mach_msg_type_number_t count_;
  std::unique_ptr<vm_statistics64_data_t> vmStat_;
#endif
};

}  // namespace nebula

#endif  // COMMON_BASE_MEMORY_H_
