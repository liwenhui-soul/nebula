/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "audit/AuditLogging.h"

#include "audit/AuditLogger.h"
#include "audit/AuditMessage.h"
#include "common/utils/Utils.h"
#include "graph/service/GraphFlags.h"

using nebula::audit::AuditLogger;
using nebula::audit::AuditMessage;

bool initAuditLog(const std::unordered_map<std::string, std::string>& config) {
  AuditLogger::getInstance()->setConfig(config);
  return AuditLogger::getInstance()->createAuditHandler();
}

void auditLogin(const AuditContext& context, ErrorCode status, const std::string& errMsg) {
  if (FLAGS_enable_audit) {
    AuditMessage msg;
    msg.category_ = context.category_;
    msg.timestamp_ = nebula::Utils::getLocalTime();
    msg.connectionId_ = context.connectionId_;
    msg.connectionStatus_ = static_cast<int>(status);
    msg.connectionErrMessage_ = errMsg;
    msg.user_ = context.user_;
    msg.clientHost_ = context.clientHost_;
    msg.host_ = FLAGS_local_ip;
    if (!AuditLogger::getInstance()->auditEvent(msg)) {
      LOG(WARNING) << "Failed to record message: [" << msg.toString() << "]";
    }
  }
}

void auditExit(const AuditContext& context) {
  if (FLAGS_enable_audit) {
    AuditMessage msg;
    msg.category_ = context.category_;
    msg.timestamp_ = nebula::Utils::getLocalTime();
    msg.connectionId_ = context.connectionId_;
    msg.user_ = context.user_;
    msg.clientHost_ = context.clientHost_;
    msg.host_ = FLAGS_local_ip;
    if (!AuditLogger::getInstance()->auditEvent(msg)) {
      LOG(WARNING) << "Failed to record message: [" << msg.toString() << "]";
    }
  }
}

void auditQuery(const AuditContext& context, ErrorCode status, const std::string& errMsg) {
  if (FLAGS_enable_audit) {
    AuditMessage msg;
    msg.category_ = context.category_;
    msg.timestamp_ = nebula::Utils::getLocalTime();
    msg.connectionId_ = context.connectionId_;
    msg.user_ = context.user_;
    msg.clientHost_ = context.clientHost_;
    msg.host_ = FLAGS_local_ip;
    msg.space_ = context.space_;
    msg.query_ = context.query_;
    msg.queryStatus_ = static_cast<int>(status);
    msg.queryErrMessage_ = errMsg;
    if (!AuditLogger::getInstance()->auditEvent(msg)) {
      LOG(WARNING) << "Failed to record message: [" << msg.toString() << "]";
    }
  }
}
