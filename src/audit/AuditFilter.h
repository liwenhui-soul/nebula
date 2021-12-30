/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <gtest/gtest_prod.h>

#include <set>
#include <unordered_map>

#include "audit/AuditMessage.h"

namespace nebula {
namespace audit {

/**
 * AuditFilter is used to filter operations that do not need to be tracked.
 */
class AuditFilter {
  FRIEND_TEST(AuditFilter, init);
  FRIEND_TEST(AuditFilter, filter);

 public:
  AuditFilter() {}
  ~AuditFilter() {}

  bool init(const std::unordered_map<std::string, std::string>& config);
  bool filter(const AuditMessage& msg);

 private:
  std::set<std::string> excludeSpaces_;
  std::set<std::string> categories_;           // which categories to track
  static std::set<std::string> kAllCategory_;  // save all categories for checking
};

}  // namespace audit
}  // namespace nebula
