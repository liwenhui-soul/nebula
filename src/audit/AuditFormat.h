/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <string>
#include <unordered_map>

#include "audit/AuditMessage.h"

namespace nebula {
namespace audit {

class AuditFormat {
 public:
  AuditFormat() {}
  ~AuditFormat() {}

  bool init(const std::unordered_map<std::string, std::string> &config);
  bool format(const AuditMessage &msg, std::string &out);

 private:
  std::string formatXML(const AuditMessage &msg);
  std::string formatJSON(const AuditMessage &msg);
  std::string formatCSV(const AuditMessage &msg);

  std::string formatType_;
};

}  // namespace audit
}  // namespace nebula
