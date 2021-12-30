/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include <unordered_map>

#include "audit/AuditFilter.h"
#include "common/base/Logging.h"

namespace nebula {
namespace audit {

TEST(AuditFilter, init) {
  std::unordered_map<std::string, std::string> config;
  AuditFilter filter;
  EXPECT_EQ(true, filter.init(config));

  config["audit_log_exclude_spaces"] = "abc, efg,aaa,,";
  EXPECT_EQ(true, filter.init(config));
  EXPECT_EQ(3, filter.excludeSpaces_.size());
  EXPECT_EQ(0, filter.categories_.size());
  filter.excludeSpaces_.clear();

  config["audit_log_categories"] = "unknown";
  EXPECT_EQ(true, filter.init(config));
  EXPECT_EQ(1, filter.categories_.size());
  std::string out = std::string("unknown");
  EXPECT_EQ(out, *filter.categories_.begin());
}

TEST(AuditFilter, filter) {
  AuditFilter filter;
  AuditMessage msg;
  EXPECT_EQ(true, filter.filter(msg));

  filter.categories_.insert("abc");
  msg.category_ = "abc";
  EXPECT_EQ(false, filter.filter(msg));
  EXPECT_EQ(true, msg.space_.empty());

  msg.space_ = "nebula";
  filter.excludeSpaces_.insert("nebula");
  EXPECT_EQ(true, filter.filter(msg));
  filter.excludeSpaces_.clear();
}

}  // namespace audit
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
