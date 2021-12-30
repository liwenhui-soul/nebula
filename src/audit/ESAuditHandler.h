/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <folly/RWSpinLock.h>
#include <gtest/gtest_prod.h>

#include "audit/AuditHandler.h"
#include "common/thread/GenericThreadPool.h"

namespace nebula {
namespace audit {

struct HttpSession {
  std::string address_;  // ip:port
  std::string user_;
  std::string password_;

  HttpSession(const std::string& address, const std::string& user, const std::string& password)
      : address_(address), user_(user), password_(password) {}
  ~HttpSession() {}

  std::string toString();
};

class ESClient {
  FRIEND_TEST(ESClient, bulk);
  FRIEND_TEST(ESClient, execQuery);

 public:
  ESClient() {}
  ~ESClient() {}

  bool init(const std::unordered_map<std::string, std::string>& config);
  bool bulk(const std::vector<std::string>& docs);

 private:
  bool execQuery(const std::string& query, std::string& out);

  std::string bulkCmd(HttpSession& session, const std::vector<std::string>& docs);
  std::string bulkHeader(HttpSession& session);
  std::string bulkBody(const std::vector<std::string>& docs);

  std::vector<HttpSession> esSessions_;
};

class ESAuditHandler : public AuditHandler {
  FRIEND_TEST(ESAuditHandler, handleMessage);

 public:
  ESAuditHandler() {}
  ~ESAuditHandler() {
    if (threadPool_ != nullptr) {
      threadPool_->purgeTimerTask(tid_);
      threadPool_->stop();
      threadPool_->wait();
    }
  }

  bool init(const std::unordered_map<std::string, std::string>& config);
  bool handleMessage(const AuditMessage& msg) override;

 private:
  std::string messageToDoc(const AuditMessage& msg);

  ESClient esClient_;
  uint64_t batchSize_{0};
  folly::RWSpinLock docsLock_;
  std::vector<std::string> docs_;
  uint64_t tid_;
  std::shared_ptr<nebula::thread::GenericThreadPool> threadPool_;
};

}  // namespace audit
}  // namespace nebula
