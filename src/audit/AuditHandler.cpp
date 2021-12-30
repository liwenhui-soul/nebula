/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "audit/AuditHandler.h"

#include <folly/logging/FileHandlerFactory.h>
#include <folly/logging/LogConfig.h>
#include <folly/logging/LogHandlerConfig.h>
#include <folly/logging/LoggerDB.h>
#include <folly/logging/xlog.h>

namespace nebula {
namespace audit {

bool AuditHandler::init(const std::unordered_map<std::string, std::string>& config) {
  return filter_.init(config);
}

bool FileAuditHandler::init(const std::unordered_map<std::string, std::string>& config) {
  if (!AuditHandler::init(config)) {
    LOG(ERROR) << "failed to init AuditHandler";
    return false;
  }
  if (!format_.init(config)) {
    LOG(ERROR) << "failed to init AuditFormat";
    return false;
  }
  if (!initXlog(config)) {
    LOG(ERROR) << "failed to init xlog";
    return false;
  }
  return true;
}

bool FileAuditHandler::initXlog(const std::unordered_map<std::string, std::string>& config) {
  // first step : register file factory and unregister old stream factory.
  folly::LoggerDB::get().registerHandlerFactory(std::make_unique<folly::FileHandlerFactory>());
  folly::LoggerDB::get().unregisterHandlerFactory("stream");

  // second step : construct xlog config.
  std::unordered_map<std::string, std::string> xlogOptions;
  auto opt = config.find("audit_log_file");
  if (opt == config.end()) {
    LOG(ERROR) << "Miss audit_log_file option.";
    return false;
  }
  xlogOptions["path"] = opt->second;
  opt = config.find("audit_log_strategy");
  if (opt == config.end()) {
    LOG(ERROR) << "Miss audit_log_strategy option.";
    return false;
  }
  if (opt->second == "synchronous") {
    xlogOptions["async"] = "false";
  } else if (opt->second == "asynchronous") {
    xlogOptions["async"] = "true";
    opt = config.find("audit_log_max_buffer_size");
    if (opt != config.end()) {
      xlogOptions["max_buffer_size"] = opt->second;
    }
  } else {
    LOG(ERROR) << "Wrong param in audit_log_strategy option: " << opt->second;
    return false;
  }
  xlogOptions["formatter"] = "custom";
  xlogOptions["log_format"] = "";
  auto xlogHandlerConfig = folly::LogHandlerConfig("file", xlogOptions);
  folly::LogConfig xlogConfig({{"default", xlogHandlerConfig}}, {});

  // third step : update xlog.
  folly::LoggerDB::get().updateConfig(xlogConfig);
  return true;
}

bool FileAuditHandler::handleMessage(const AuditMessage& msg) {
  if (filter_.filter(msg)) {
    return true;
  }
  std::string formatedMsg = "";
  if (!format_.format(msg, formatedMsg)) {
    LOG(ERROR) << "failed to format msg";
    return false;
  }
  XLOG(INFO) << formatedMsg;
  return true;
}

}  // namespace audit
}  // namespace nebula
