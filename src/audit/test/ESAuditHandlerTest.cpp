/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include <unordered_map>

#include "audit/ESAuditHandler.h"

namespace nebula {
namespace audit {

TEST(HttpSession, toString) {
  HttpSession s("localhost:9200", "u", "p");
  std::string res = " -u u:p  http://localhost:9200";
  EXPECT_EQ(res, s.toString());
}

TEST(ESClient, init) {
  std::unordered_map<std::string, std::string> config;
  ESClient client;
  // audit_log_es_address is not specified
  EXPECT_EQ(false, client.init(config));

  config["audit_log_es_address"] = "";
  // audit_log_es_address can't be null
  EXPECT_EQ(false, client.init(config));

  config["audit_log_es_address"] = "192.168.0.1:9200";
  EXPECT_EQ(true, client.init(config));
}

TEST(ESClient, bulk) {
  std::unordered_map<std::string, std::string> config;
  ESClient client;
  // port -9200 must not exist
  config["audit_log_es_address"] = "256:987:1000:01:-5";
  EXPECT_EQ(true, client.init(config));

  std::vector<std::string> docs{"{\"k1\":\"v1\"}", "{\"k2\":\"v2\"}"};
  std::string header = client.bulkHeader(client.esSessions_[0]);
  std::string headerCompared =
      "/usr/bin/curl  -sS  -XPOST  -H \"Content-Type: application/json; charset=utf-8\"  "
      "http://256:987:1000:01:-5/_bulk";
  EXPECT_EQ(headerCompared, header);

  std::string body = client.bulkBody(docs);
  std::string bodyCompared =
      "{\"create\":{\"_index\":\"audit_log_nebula\"}}\n"
      "{\"k1\":\"v1\"}\n"
      "{\"create\":{\"_index\":\"audit_log_nebula\"}}\n"
      "{\"k2\":\"v2\"}\n";
  EXPECT_EQ(bodyCompared, body);

  std::string cmd = client.bulkCmd(client.esSessions_[0], docs);
  std::string cmdCompared = headerCompared + " -d '" + bodyCompared + "'";
  EXPECT_EQ(cmd, cmdCompared);

  std::string out = "";
  EXPECT_EQ(false, client.execQuery(cmd, out));  // es not found
}

TEST(ESClient, execQuery) {
  ESClient client;
  std::string cmd = "curl 256:987:1000:01:-5";
  std::string out = "";
  EXPECT_EQ(false, client.execQuery(cmd, out));

  cmd = "ls -l";
  EXPECT_EQ(true, client.execQuery(cmd, out));
}

TEST(ESAuditHandler, handleMessage) {
  ESAuditHandler handler;
  std::unordered_map<std::string, std::string> config{{"audit_log_categories", "unknown"}};
  handler.filter_.init(config);
  handler.batchSize_ = 1000;

  AuditMessage msg;
  std::string msgStr = handler.messageToDoc(msg);
  std::string msgCompared =
      "{\"category\":\"unknown\","
      "\"timestamp\":\"\","
      "\"terminal\":\"\","
      "\"connection_id\":\"0\","
      "\"connection_status\":\"0\","
      "\"connection_message\":\"\","
      "\"user\":\"\","
      "\"client_host\":\"\","
      "\"host\":\"\","
      "\"space\":\"\","
      "\"query\":\"\","
      "\"query_status\":\"0\","
      "\"query_message\":\"\"}";
  EXPECT_EQ(msgStr, msgCompared);

  EXPECT_EQ(true, handler.handleMessage(msg));
  EXPECT_EQ(1, handler.docs_.size());
}

}  // namespace audit
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
