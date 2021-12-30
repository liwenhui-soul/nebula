/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <algorithm>

#include "audit/AuditLogging.h"
#include "common/base/Status.h"
#include "common/fs/FileUtils.h"
#include "graph/service/GraphFlags.h"

using nebula::Status;
using nebula::fs::FileUtils;

void collectAuditOptions(std::unordered_map<std::string, std::string>& config) {
  std::string in = FLAGS_audit_log_handler;
  std::transform(in.begin(), in.end(), in.begin(), ::tolower);
  config["audit_log_handler"] = in;

  config["audit_log_file"] = FLAGS_audit_log_file;

  in = FLAGS_audit_log_strategy;
  std::transform(in.begin(), in.end(), in.begin(), ::tolower);
  config["audit_log_strategy"] = in;

  config["audit_log_max_buffer_size"] = std::to_string(FLAGS_audit_log_max_buffer_size);

  in = FLAGS_audit_log_format;
  std::transform(in.begin(), in.end(), in.begin(), ::tolower);
  config["audit_log_format"] = in;

  config["audit_log_es_address"] = FLAGS_audit_log_es_address;
  config["audit_log_es_user"] = FLAGS_audit_log_es_user;
  config["audit_log_es_password"] = FLAGS_audit_log_es_password;
  config["audit_log_es_batch_size"] = std::to_string(FLAGS_audit_log_es_batch_size);
  config["audit_log_exclude_spaces"] = FLAGS_audit_log_exclude_spaces;

  in = FLAGS_audit_log_categories;
  std::transform(in.begin(), in.end(), in.begin(), ::tolower);
  config["audit_log_categories"] = in;
}

Status setupAuditLog() {
  std::string logPath = FileUtils::dirname(FLAGS_audit_log_file.c_str());
  if (!FileUtils::exist(logPath)) {
    if (!FileUtils::makeDir(logPath)) {
      return Status::Error("Failed to create log directory `%s'", logPath.c_str());
    }
  }

  std::unordered_map<std::string, std::string> config;
  collectAuditOptions(config);
  if (!initAuditLog(config)) {
    return Status::Error("Failed to init audit log");
  }
  return Status::OK();
}
