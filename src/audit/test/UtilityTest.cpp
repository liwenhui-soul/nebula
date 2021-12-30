/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Random.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "common/utils/Utils.h"

/**
 * This file is mainly used to test some related utility functions used by audit.
 * Check if these functions meet expectations.
 */
namespace nebula {
namespace audit {

TEST(folly, splitAndTrim) {
  std::string s = "abc, ef g,aaa ,, bbb ,,,";
  std::vector<std::string> vecOrigin;
  folly::split(",", s, vecOrigin, true);
  std::vector<std::string> vec;
  for (auto& v : vecOrigin) {
    vec.push_back(folly::trimWhitespace(v).str());
  }
  ASSERT_EQ(4, vec.size());
  std::string out = std::string("abc");
  EXPECT_EQ(out, vec[0]);
  out = std::string("ef g");
  EXPECT_EQ(out, vec[1]);
  out = std::string(" ef g");
  EXPECT_NE(out, vec[1]);
  out = std::string("aaa");
  EXPECT_EQ(out, vec[2]);
  out = std::string("bbb");
  EXPECT_EQ(out, vec[3]);
  out = std::string(" bbb ");
  EXPECT_NE(out, vec[3]);

  vec.clear();
  vecOrigin.clear();
  folly::split(",", s, vecOrigin, false);
  for (auto& v : vecOrigin) {
    vec.push_back(folly::trimWhitespace(v).str());
  }
  ASSERT_EQ(8, vec.size());
  out = std::string("abc");
  EXPECT_EQ(out, vec[0]);
  out = std::string("ef g");
  EXPECT_EQ(out, vec[1]);
  out = std::string("aaa");
  EXPECT_EQ(out, vec[2]);
  out = std::string("aaa ");
  EXPECT_NE(out, vec[2]);
  out = std::string("");
  EXPECT_EQ(out, vec[3]);
  out = std::string("bbb");
  EXPECT_EQ(out, vec[4]);
  out = std::string("");
  EXPECT_EQ(out, vec[5]);
  out = std::string("");
  EXPECT_EQ(out, vec[6]);
  out = std::string("");
  EXPECT_EQ(out, vec[7]);
}

TEST(folly, parseJson) {
  std::string in = "{\"errors\":false, \"success\":\"false\", \"key\":{\"subkey\":\"value\"}}";
  auto out = folly::parseJson(in);
  auto res = out.find("errors");
  EXPECT_NE(res, out.items().end());
  EXPECT_EQ(true, res->second.isBool());
  EXPECT_EQ(false, res->second.getBool());

  auto res2 = out.find("success");
  EXPECT_NE(res2, out.items().end());
  EXPECT_EQ(false, res2->second.isBool());

  auto res3 = out.find("key");
  EXPECT_NE(res3, out.items().end());

  auto res4 = out.find("subkey");
  EXPECT_EQ(res4, out.items().end());
}

TEST(folly, rand32) {
  int res = folly::Random::rand32(3);
  EXPECT_GT(3, res);

  res = folly::Random::rand32(1);
  EXPECT_GT(1, res);

  res = folly::Random::rand32(0);
  EXPECT_EQ(0, res);
}

TEST(Utils, getLocalTime) {
  std::string s = "";
  s = Utils::getLocalTime();
  std::string out = std::string("");
  EXPECT_NE(out, s);
}

TEST(Utils, runCommand) {
  std::string command = "";
  int exitCode = 0;
  std::string out = "";
  EXPECT_EQ(0, Utils::runCommand(command, exitCode, out));
  EXPECT_EQ(0, exitCode);
  std::string res = std::string("");
  EXPECT_EQ(res, out);

  command = "ls -l";
  EXPECT_EQ(0, Utils::runCommand(command, exitCode, out));
  EXPECT_EQ(0, exitCode);
  res = std::string("");
  EXPECT_NE(res, out);

  command = "curl 256.257.289.300:-10";
  EXPECT_EQ(0, Utils::runCommand(command, exitCode, out));
  EXPECT_NE(0, exitCode);
  res = std::string("");
  EXPECT_EQ(res, out);
}

TEST(Utils, processSpecChar) {
  std::string in = "`a12\"HG'bc`d``bv";
  std::string out = "'a12\"HG'bc'd''bv";
  EXPECT_EQ(out, Utils::processSpecChar(in));

  in = "";
  out = "";
  EXPECT_EQ(out, Utils::processSpecChar(in));
}

}  // namespace audit
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
