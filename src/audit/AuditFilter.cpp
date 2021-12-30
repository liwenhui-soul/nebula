/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "audit/AuditFilter.h"

#include <folly/String.h>

#include "common/base/Logging.h"

namespace nebula {
namespace audit {

std::set<std::string> AuditFilter::kAllCategory_ = {
    "login", "exit", "ddl", "dql", "dml", "dcl", "util", "unknown"};

bool AuditFilter::init(const std::unordered_map<std::string, std::string> &config) {
  auto option = config.find("audit_log_exclude_spaces");
  if (option != config.end()) {
    std::vector<std::string> excludeSpaces;
    folly::split(",", option->second, excludeSpaces, true);
    for (auto &s : excludeSpaces) {
      excludeSpaces_.insert(folly::trimWhitespace(s).str());
    }
  }

  option = config.find("audit_log_categories");
  if (option != config.end()) {
    std::vector<std::string> categories;
    folly::split(",", option->second, categories, true);
    for (auto &s : categories) {
      std::string category = folly::trimWhitespace(s).str();
      if (!kAllCategory_.count(category)) {
        LOG(ERROR) << "unexpected category: " << category;
        return false;
      }
      categories_.insert(category);
    }
  }

  return true;
}

bool AuditFilter::filter(const AuditMessage &msg) {
  if (!categories_.count(msg.category_)) {
    return true;
  }
  if (!msg.space_.empty()) {
    if (excludeSpaces_.count(msg.space_)) {
      return true;
    }
  }
  return false;
}

}  // namespace audit
}  // namespace nebula
