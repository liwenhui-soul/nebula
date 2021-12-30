/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "parser/Sentence.h"

namespace nebula {
namespace graph {

extern std::string auditCategory(Sentence::Kind kind);

TEST(AuditCategory, test) {
  std::string category = "ddl";
  EXPECT_EQ(category, auditCategory(Sentence::Kind::kCreateTag));

  category = "dql";
  EXPECT_EQ(category, auditCategory(Sentence::Kind::kLimit));

  category = "dml";
  EXPECT_EQ(category, auditCategory(Sentence::Kind::kInsertEdges));

  category = "dcl";
  EXPECT_EQ(category, auditCategory(Sentence::Kind::kCreateSnapshot));

  category = "util";
  EXPECT_EQ(category, auditCategory(Sentence::Kind::kShowQueries));

  category = "unknown";
  EXPECT_EQ(category, auditCategory(Sentence::Kind::kSet));
}

}  // namespace graph
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
