/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <string>
#include <unordered_map>

#include "common/graph/Response.h"

/**
 * AuditLogging.h is the interface of the audit log.
 */

using nebula::ErrorCode;

struct AuditContext {
  std::string category_{"unknown"};
  int64_t connectionId_{0};
  std::string user_;
  std::string clientHost_;
  std::string space_;
  std::string query_;
};

bool initAuditLog(const std::unordered_map<std::string, std::string>& config);

void auditLogin(const AuditContext& context, ErrorCode status, const std::string& errMsg);

void auditExit(const AuditContext& context);

void auditQuery(const AuditContext& context, ErrorCode status, const std::string& errMsg);
