/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <folly/RWSpinLock.h>
#include <gtest/gtest_prod.h>

#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/base/Logging.h"

namespace nebula {
namespace audit {

class AuditHandler;
struct AuditMessage;

/**
 * AuditLogger is used to record various operations of the database.
 * AuditLogger is a main class of the audit, it should be a singleton.
 */
class AuditLogger {
  FRIEND_TEST(AuditLogger, createAuditHandler);
  FRIEND_TEST(AuditLogger, auditEvent);

 public:
  static AuditLogger* getInstance() {
    static std::unique_ptr<AuditLogger> auditInstance(new AuditLogger());
    return auditInstance.get();
  }

  void setConfig(const std::unordered_map<std::string, std::string>& config);
  bool createAuditHandler();
  bool auditEvent(const AuditMessage& msg);

 private:
  AuditLogger() {}
  AuditLogger(const AuditLogger& other) = delete;
  AuditLogger& operator=(const AuditLogger& other) = delete;

  folly::RWSpinLock lock_;
  std::unordered_map<std::string, std::string> config_;
  std::shared_ptr<AuditHandler> auditHandler_{nullptr};
};

}  // namespace audit
}  // namespace nebula
