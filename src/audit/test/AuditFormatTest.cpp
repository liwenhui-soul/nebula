/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include <unordered_map>

#include "audit/AuditFormat.h"
#include "common/base/Logging.h"

namespace nebula {
namespace audit {

TEST(AuditFormat, init) {
  std::unordered_map<std::string, std::string> config;
  AuditFormat format;

  // miss audit_log_format option.
  EXPECT_EQ(false, format.init(config));

  config["audit_log_format"] = "xml";
  EXPECT_EQ(true, format.init(config));
}

TEST(AuditFormat, format) {
  std::unordered_map<std::string, std::string> config;
  AuditFormat format;
  AuditMessage msg;
  msg.category_ = "dql";
  msg.timestamp_ = "2021-12-20 16:48:00";
  msg.connectionId_ = 0;
  msg.connectionStatus_ = 0;
  msg.connectionErrMessage_ = "";
  msg.user_ = "xiao\"ming";
  msg.clientHost_ = "192.168.0.1";
  msg.host_ = "local\\host";
  msg.space_ = "nebula";
  msg.query_ = "go from 'v1' over 'follow'";
  msg.queryStatus_ = 0;
  msg.queryErrMessage_ = "unexpected `error,";

  config["audit_log_format"] = "xml";
  EXPECT_EQ(true, format.init(config));
  std::string out = "";
  EXPECT_EQ(true, format.format(msg, out));

  std::string res =
      "<AUDIT_RECORD\n"
      "  CATEGORY=\"dql\"\n"
      "  TIMESTAMP=\"2021-12-20 16:48:00\"\n"
      "  TERMINAL=\"\"\n"
      "  CONNECTION_ID=\"0\"\n"
      "  CONNECT_STATUS=\"0\"\n"
      "  CONNECT_MESSAGE=\"\"\n"
      "  USER=\"xiao\"ming\"\n"
      "  CLIENT_HOST=\"192.168.0.1\"\n"
      "  HOST=\"local\\host\"\n"
      "  SPACE=\"nebula\"\n"
      "  QUERY=\"go from 'v1' over 'follow'\"\n"
      "  QUERY_STATUS=\"0\"\n"
      "  QUERY_MESSAGE=\"unexpected `error,\"\n"
      "/>";
  EXPECT_EQ(res, out);

  config["audit_log_format"] = "json";
  EXPECT_EQ(true, format.init(config));
  out = "";
  EXPECT_EQ(true, format.format(msg, out));
  res =
      "{\n"
      "  \"category\":\"dql\",\n"
      "  \"timestamp\":\"2021-12-20 16:48:00\",\n"
      "  \"terminal\":\"\",\n"
      "  \"connection_id\":\"0\",\n"
      "  \"connection_status\":\"0\",\n"
      "  \"connection_message\":\"\",\n"
      "  \"user\":\"xiao\"ming\",\n"
      "  \"client_host\":\"192.168.0.1\",\n"
      "  \"host\":\"local\\host\",\n"
      "  \"space\":\"nebula\",\n"
      "  \"query\":\"go from 'v1' over 'follow'\",\n"
      "  \"query_status\":\"0\",\n"
      "  \"query_message\":\"unexpected `error,\"\n"
      "}";
  EXPECT_EQ(res, out);

  config["audit_log_format"] = "csv";
  EXPECT_EQ(true, format.init(config));
  out = "";
  EXPECT_EQ(true, format.format(msg, out));
  res =
      "dql,2021-12-20 16:48:00,,0,0,,xiao\"ming,192.168.0.1,local\\host,nebula,"
      "go from 'v1' over 'follow',0,unexpected `error,";
  EXPECT_EQ(res, out);

  config["audit_log_format"] = "other";
  EXPECT_EQ(true, format.init(config));
  out = "";
  EXPECT_EQ(false, format.format(msg, out));
}

}  // namespace audit
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
