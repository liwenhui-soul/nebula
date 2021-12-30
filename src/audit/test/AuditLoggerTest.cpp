/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>
#include <folly/logging/LoggerDB.h>
#include <folly/logging/StreamHandlerFactory.h>
#include <gtest/gtest.h>

#include "audit/AuditHandler.h"
#include "audit/AuditLogger.h"

namespace nebula {
namespace audit {

TEST(AuditLogger, createAuditHandler) {
  AuditLogger::getInstance()->auditHandler_ = std::make_shared<FileAuditHandler>();
  // audit handler has been registered
  EXPECT_EQ(false, AuditLogger::getInstance()->createAuditHandler());
  AuditLogger::getInstance()->auditHandler_ = nullptr;

  std::unordered_map<std::string, std::string> config;
  AuditLogger::getInstance()->setConfig(config);

  // audit_log_handler is not specified
  EXPECT_EQ(false, AuditLogger::getInstance()->createAuditHandler());

  AuditLogger::getInstance()->config_["audit_log_handler"] = "file";
  AuditLogger::getInstance()->config_["audit_log_file"] = "./log";
  AuditLogger::getInstance()->config_["audit_log_strategy"] = "synchronous";
  AuditLogger::getInstance()->config_["audit_log_format"] = "xml";
  AuditLogger::getInstance()->config_["audit_log_categories"] = "login";

  // normal
  EXPECT_EQ(true, AuditLogger::getInstance()->createAuditHandler());
  AuditLogger::getInstance()->auditHandler_ = nullptr;

  AuditLogger::getInstance()->config_["audit_log_handler"] = "es";

  // failed to init auditHandler.
  EXPECT_EQ(false, AuditLogger::getInstance()->createAuditHandler());
  AuditLogger::getInstance()->config_["audit_log_es_address"] = "localhost:9200";

  // normal
  EXPECT_EQ(true, AuditLogger::getInstance()->createAuditHandler());
  AuditLogger::getInstance()->auditHandler_ = nullptr;

  AuditLogger::getInstance()->config_["audit_log_handler"] = "other";
  // unsupported handler type
  EXPECT_EQ(false, AuditLogger::getInstance()->createAuditHandler());

  // In order not to affect the next TEST case, cleanup all handlers here.
  folly::LoggerDB::get().cleanupHandlers();
  folly::LoggerDB::get().registerHandlerFactory(std::make_unique<folly::StreamHandlerFactory>());
}

TEST(AuditLogger, auditEvent) {
  AuditMessage msg;
  // No audit handler registered
  EXPECT_EQ(false, AuditLogger::getInstance()->auditEvent(msg));

  AuditLogger::getInstance()->config_.clear();
  AuditLogger::getInstance()->config_["audit_log_handler"] = "file";
  AuditLogger::getInstance()->config_["audit_log_file"] = "./log";
  AuditLogger::getInstance()->config_["audit_log_strategy"] = "asynchronous";
  AuditLogger::getInstance()->config_["audit_log_format"] = "xml";
  AuditLogger::getInstance()->config_["audit_log_exclude_spaces"] = "abc";
  AuditLogger::getInstance()->config_["audit_log_categories"] = "ddl";
  EXPECT_EQ(true, AuditLogger::getInstance()->createAuditHandler());
  EXPECT_EQ(true, AuditLogger::getInstance()->auditEvent(msg));
}

}  // namespace audit
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
