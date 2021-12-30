/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "audit/AuditLogger.h"

#include "audit/AuditHandler.h"
#include "audit/ESAuditHandler.h"

namespace nebula {
namespace audit {

void AuditLogger::setConfig(const std::unordered_map<std::string, std::string>& config) {
  folly::RWSpinLock::WriteHolder holder(lock_);
  config_ = config;
  return;
}

bool AuditLogger::createAuditHandler() {
  folly::RWSpinLock::WriteHolder holder(lock_);
  if (auditHandler_ != nullptr) {
    LOG(ERROR) << "audit handler has been registered";
    return false;
  }
  auto handlerType = config_.find("audit_log_handler");
  if (handlerType == config_.end()) {
    LOG(ERROR) << "audit_log_handler is not specified";
    return false;
  }
  if (handlerType->second == "file") {
    auto auditHandler = std::make_shared<FileAuditHandler>();
    if (!auditHandler->init(config_)) {
      LOG(ERROR) << "failed to init auditHandler";
      return false;
    }
    auditHandler_ = auditHandler;
    return true;
  } else if (handlerType->second == "es") {
    auto auditHandler = std::make_shared<ESAuditHandler>();
    if (!auditHandler->init(config_)) {
      LOG(ERROR) << "failed to init auditHandler";
      return false;
    }
    auditHandler_ = auditHandler;
    return true;
  } else {
    LOG(ERROR) << "unsupported handler type:" << handlerType->second;
    return false;
  }
  return false;
}

bool AuditLogger::auditEvent(const AuditMessage& msg) {
  folly::RWSpinLock::ReadHolder holder(lock_);
  if (auditHandler_ != nullptr) {
    return auditHandler_->handleMessage(msg);
  } else {
    LOG(ERROR) << "No audit handler registered";
    return false;
  }
}

}  // namespace audit
}  // namespace nebula
