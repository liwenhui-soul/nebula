/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <sstream>
#include <string>

namespace nebula {
namespace audit {

// One operation corresponds to a AuditMessage.
struct AuditMessage {
  std::string category_{"unknown"};
  std::string timestamp_;
  std::string terminal_;  // where does the query come from. Follow-up will support.
  int64_t connectionId_{0};
  int connectionStatus_{0};
  // When recording a connection operation, if the connection fails,
  // connectionErrMessage_ is used to record the error message.
  std::string connectionErrMessage_;
  std::string user_;
  std::string clientHost_;  // client ip
  std::string host_;        // server ip
  std::string space_;
  std::string query_;  // ngql statement.
  int queryStatus_{0};
  // If the query fails, queryErrMessage_ will record the failure information.
  std::string queryErrMessage_;

  std::string toString() {
    std::stringstream os;
    os << "category = " << category_ << ", "
       << "timestamp = " << timestamp_ << ", "
       << "terminal = " << terminal_ << ", "
       << "connectionId = " << connectionId_ << ", "
       << "connectionStatus = " << connectionStatus_ << ", "
       << "connectionErrMessage = " << connectionErrMessage_ << ", "
       << "user = " << user_ << ", "
       << "clientHost = " << clientHost_ << ", "
       << "host = " << host_ << ", "
       << "space = " << space_ << ", "
       << "query = " << query_ << ", "
       << "queryStatus = " << queryStatus_ << ", "
       << "queryErrMessage = " << queryErrMessage_;
    return os.str();
  }
};

}  // namespace audit
}  // namespace nebula
