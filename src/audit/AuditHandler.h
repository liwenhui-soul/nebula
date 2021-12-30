/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <gtest/gtest_prod.h>

#include "audit/AuditFilter.h"
#include "audit/AuditFormat.h"
#include "audit/AuditMessage.h"

namespace nebula {
namespace audit {

/**
 * AuditHandler is used to handle log message. It is a base class,
 * and it has provided two implementations: FileAuditHandler and ESAuditHandler.
 * Users can also implement their own subclass based on it.
 */
class AuditHandler {
 public:
  AuditHandler() {}
  virtual ~AuditHandler() {}

  bool init(const std::unordered_map<std::string, std::string>& config);
  virtual bool handleMessage(const AuditMessage& msg) = 0;

 protected:
  AuditFilter filter_;
};

/**
 * FileAuditHandler uses folly::xlog to write files.
 */
class FileAuditHandler : public AuditHandler {
  FRIEND_TEST(FileAuditHandler, initXlog);

 public:
  FileAuditHandler() {}
  ~FileAuditHandler() {}

  bool init(const std::unordered_map<std::string, std::string>& config);
  bool handleMessage(const AuditMessage& msg) override;

 private:
  bool initXlog(const std::unordered_map<std::string, std::string>& config);

  AuditFormat format_;
};

}  // namespace audit
}  // namespace nebula
