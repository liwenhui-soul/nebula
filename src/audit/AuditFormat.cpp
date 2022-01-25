/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "audit/AuditFormat.h"

#include <folly/Format.h>

#include "common/base/Logging.h"

namespace nebula {
namespace audit {

constexpr char kXmlFormat[] =
    "<AUDIT_RECORD\n"
    "  CATEGORY=\"{}\"\n"
    "  TIMESTAMP=\"{}\"\n"
    "  TERMINAL=\"{}\"\n"
    "  CONNECTION_ID=\"{}\"\n"
    "  CONNECTION_STATUS=\"{}\"\n"
    "  CONNECTION_MESSAGE=\"{}\"\n"
    "  USER=\"{}\"\n"
    "  CLIENT_HOST=\"{}\"\n"
    "  HOST=\"{}\"\n"
    "  SPACE=\"{}\"\n"
    "  QUERY=\"{}\"\n"
    "  QUERY_STATUS=\"{}\"\n"
    "  QUERY_MESSAGE=\"{}\"\n"
    "/>";

constexpr char kJsonformat[] =
    "{{\n"
    "  \"category\":\"{}\",\n"
    "  \"timestamp\":\"{}\",\n"
    "  \"terminal\":\"{}\",\n"
    "  \"connection_id\":\"{}\",\n"
    "  \"connection_status\":\"{}\",\n"
    "  \"connection_message\":\"{}\",\n"
    "  \"user\":\"{}\",\n"
    "  \"client_host\":\"{}\",\n"
    "  \"host\":\"{}\",\n"
    "  \"space\":\"{}\",\n"
    "  \"query\":\"{}\",\n"
    "  \"query_status\":\"{}\",\n"
    "  \"query_message\":\"{}\"\n"
    "}}";

constexpr char kCsvformat[] = "{},{},{},{},{},{},{},{},{},{},{},{},{}";

bool AuditFormat::init(const std::unordered_map<std::string, std::string> &config) {
  auto option = config.find("audit_log_format");
  if (option == config.end()) {
    LOG(ERROR) << "miss audit_log_format option.";
    return false;
  }
  if (option->second != "xml" && option->second != "json" && option->second != "csv") {
    LOG(ERROR) << "unsupported format type:" << option->second;
    return false;
  }
  formatType_ = option->second;
  return true;
}

bool AuditFormat::format(const AuditMessage &msg, std::string &out) {
  if (formatType_ == "xml") {
    out = formatXML(msg);
    return true;
  } else if (formatType_ == "json") {
    out = formatJSON(msg);
    return true;
  } else if (formatType_ == "csv") {
    out = formatCSV(msg);
    return true;
  } else {
    LOG(ERROR) << "unsupported format type:" << formatType_;
    return false;
  }
  return false;
}

// @TODO(zhaohaifei): escape string
std::string AuditFormat::formatXML(const AuditMessage &msg) {
  return folly::sformat(kXmlFormat,
                        msg.category_,
                        msg.timestamp_,
                        msg.terminal_,
                        msg.connectionId_,
                        msg.connectionStatus_,
                        msg.connectionErrMessage_,
                        msg.user_,
                        msg.clientHost_,
                        msg.host_,
                        msg.space_,
                        msg.query_,
                        msg.queryStatus_,
                        msg.queryErrMessage_);
}

// @TODO(zhaohaifei): escape string
std::string AuditFormat::formatJSON(const AuditMessage &msg) {
  return folly::sformat(kJsonformat,
                        msg.category_,
                        msg.timestamp_,
                        msg.terminal_,
                        msg.connectionId_,
                        msg.connectionStatus_,
                        msg.connectionErrMessage_,
                        msg.user_,
                        msg.clientHost_,
                        msg.host_,
                        msg.space_,
                        msg.query_,
                        msg.queryStatus_,
                        msg.queryErrMessage_);
}

// @TODO(zhaohaifei): escape string
std::string AuditFormat::formatCSV(const AuditMessage &msg) {
  return folly::sformat(kCsvformat,
                        msg.category_,
                        msg.timestamp_,
                        msg.terminal_,
                        msg.connectionId_,
                        msg.connectionStatus_,
                        msg.connectionErrMessage_,
                        msg.user_,
                        msg.clientHost_,
                        msg.host_,
                        msg.space_,
                        msg.query_,
                        msg.queryStatus_,
                        msg.queryErrMessage_);
}

}  // namespace audit
}  // namespace nebula
