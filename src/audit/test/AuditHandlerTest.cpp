/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>
#include <folly/logging/LoggerDB.h>
#include <folly/logging/StreamHandlerFactory.h>
#include <gtest/gtest.h>

#include <unordered_map>

#include "audit/AuditHandler.h"
#include "common/base/Logging.h"

namespace nebula {
namespace audit {

TEST(FileAuditHandler, init) {
  std::unordered_map<std::string, std::string> config;
  FileAuditHandler handler;

  // failed to init AuditHandler
  EXPECT_EQ(false, handler.init(config));
  config["audit_log_exclude_spaces"] = "null";

  // failed to init AuditFormat
  EXPECT_EQ(false, handler.init(config));
  config["audit_log_format"] = "csv";

  // failed to init xlog
  EXPECT_EQ(false, handler.init(config));
  config["audit_log_file"] = "./log";
  folly::LoggerDB::get().registerHandlerFactory(std::make_unique<folly::StreamHandlerFactory>());
  folly::LoggerDB::get().unregisterHandlerFactory("file");

  // failed to init xlog
  EXPECT_EQ(false, handler.init(config));
  config["audit_log_strategy"] = "asynchronous";
  folly::LoggerDB::get().registerHandlerFactory(std::make_unique<folly::StreamHandlerFactory>());
  folly::LoggerDB::get().unregisterHandlerFactory("file");

  EXPECT_EQ(true, handler.init(config));

  // In order not to affect the next TEST case, cleanup all handlers here.
  folly::LoggerDB::get().cleanupHandlers();
  folly::LoggerDB::get().registerHandlerFactory(std::make_unique<folly::StreamHandlerFactory>());
}

TEST(FileAuditHandler, initXlog) {
  std::unordered_map<std::string, std::string> config;
  FileAuditHandler handler;

  // Miss audit_log_file option.
  EXPECT_EQ(false, handler.initXlog(config));
  config["audit_log_file"] = "./log";
  folly::LoggerDB::get().registerHandlerFactory(std::make_unique<folly::StreamHandlerFactory>());
  folly::LoggerDB::get().unregisterHandlerFactory("file");

  // Miss audit_log_strategy option.
  EXPECT_EQ(false, handler.initXlog(config));
  config["audit_log_strategy"] = "unknown";
  folly::LoggerDB::get().registerHandlerFactory(std::make_unique<folly::StreamHandlerFactory>());
  folly::LoggerDB::get().unregisterHandlerFactory("file");

  // Wrong param in audit_log_strategy option.
  EXPECT_EQ(false, handler.initXlog(config));
  config["audit_log_strategy"] = "asynchronous";
  config["audit_log_max_buffer_size"] = "1000";
  folly::LoggerDB::get().registerHandlerFactory(std::make_unique<folly::StreamHandlerFactory>());
  folly::LoggerDB::get().unregisterHandlerFactory("file");

  EXPECT_EQ(true, handler.initXlog(config));

  folly::LoggerDB::get().registerHandlerFactory(std::make_unique<folly::StreamHandlerFactory>());
  folly::LoggerDB::get().unregisterHandlerFactory("file");
}

TEST(FileAuditHandler, handleMessage) {
  std::unordered_map<std::string, std::string> config;
  config["audit_log_file"] = "./log";
  config["audit_log_strategy"] = "synchronous";
  config["audit_log_max_buffer_size"] = "10";
  config["audit_log_format"] = "json";
  config["audit_log_exclude_spaces"] = "null";
  config["audit_log_categories"] = "exit";

  FileAuditHandler handler;
  EXPECT_EQ(true, handler.init(config));

  AuditMessage msg;
  msg.category_ = "login";
  // filter
  EXPECT_EQ(true, handler.handleMessage(msg));

  msg.category_ = "exit";
  // normal
  EXPECT_EQ(true, handler.handleMessage(msg));

  folly::LoggerDB::get().registerHandlerFactory(std::make_unique<folly::StreamHandlerFactory>());
  folly::LoggerDB::get().unregisterHandlerFactory("file");
}

}  // namespace audit
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
