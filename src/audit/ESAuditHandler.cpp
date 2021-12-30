/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "audit/ESAuditHandler.h"

#include <folly/String.h>

#include <sstream>

#include "common/utils/Utils.h"

constexpr char kCurl[] = "/usr/bin/curl ";
constexpr char kXPOST[] = " -XPOST ";
constexpr char kSilentMode[] = " -sS ";
constexpr char kUser[] = " -u ";
constexpr char kHttp[] = " http://";
constexpr char kContentJson[] = " -H \"Content-Type: application/json; charset=utf-8\" ";

// The reason why 'nebula_*' is not used is to avoid conflicts with fulltext index.
constexpr char kAuditIndex[] = "audit_log_nebula";

namespace nebula {
namespace audit {

std::string HttpSession::toString() {
  std::stringstream os;
  if (!user_.empty()) {
    os << kUser << user_ << ":" << password_ << " ";
  }
  os << kHttp << address_;
  return os.str();
}

bool ESClient::init(const std::unordered_map<std::string, std::string>& config) {
  auto option = config.find("audit_log_es_address");
  if (option == config.end()) {
    LOG(ERROR) << "audit_log_es_address is not specified";
    return false;
  }
  std::vector<std::string> esAddressesVec;
  folly::split(",", option->second, esAddressesVec, true);
  std::vector<std::string> esAddresses;
  for (auto& addr : esAddressesVec) {
    esAddresses.push_back(folly::trimWhitespace(addr).str());
  }
  if (esAddresses.empty()) {
    LOG(ERROR) << "audit_log_es_address can't be null";
    return false;
  }

  std::string user = "";
  option = config.find("audit_log_es_user");
  if (option != config.end()) {
    user = option->second;
  }

  std::string password = "";
  option = config.find("audit_log_es_password");
  if (option != config.end()) {
    password = option->second;
  }

  for (auto& address : esAddresses) {
    HttpSession esSession(address, user, password);
    esSessions_.push_back(esSession);
  }
  if (esSessions_.size() == 0) {
    LOG(ERROR) << "No es session available";
    return false;
  }
  return true;
}

bool ESClient::bulk(const std::vector<std::string>& docs) {
  int retryCnt = 3;
  while (retryCnt-- > 0) {
    auto index = folly::Random::rand32(esSessions_.size());
    std::string query = bulkCmd(esSessions_[index], docs);
    std::string out;
    if (!execQuery(query, out)) {
      LOG(WARNING) << "Bulk failed. retry : " << retryCnt;
      continue;
    } else {
      auto outJson = folly::parseJson(out);
      auto res = outJson.find("errors");
      if (res != outJson.items().end() && res->second.isBool() && !res->second.getBool()) {
        return true;
      } else {
        LOG(WARNING) << "Bulk failed. reason: " << out;
        // @TODO(zhaohaifei): record failed docs
        return false;
      }
    }
  }
  return false;
}

bool ESClient::execQuery(const std::string& query, std::string& out) {
  int exitCode = 0;
  if (Utils::runCommand(query, exitCode, out) < 0) {
    LOG(ERROR) << "Failed to execQuery: " << query;
    return false;
  }
  if (exitCode != 0) {
    LOG(WARNING) << "Failed to communicate with es";
    // try again
    if (Utils::runCommand(query, exitCode, out) < 0) {
      LOG(ERROR) << "Failed to execQuery: " << query;
      return false;
    } else if (exitCode != 0) {
      LOG(ERROR) << "Failed to communicate with es";
      return false;
    } else {
      return true;
    }
  }
  return true;
}

std::string ESClient::bulkCmd(HttpSession& session, const std::vector<std::string>& docs) {
  std::stringstream os;
  os << bulkHeader(session) << " -d '" << bulkBody(docs) << "'";
  return os.str();
}

std::string ESClient::bulkHeader(HttpSession& session) {
  std::stringstream os;
  os << kCurl << kSilentMode << kXPOST << kContentJson << session.toString() << "/_bulk";
  return os.str();
}

std::string ESClient::bulkBody(const std::vector<std::string>& docs) {
  std::stringstream os;
  for (auto& doc : docs) {
    os << "{\"create\":{\"_index\":\"" << kAuditIndex << "\"}}\n";
    os << doc << "\n";
  }
  return os.str();
}

bool ESAuditHandler::init(const std::unordered_map<std::string, std::string>& config) {
  if (!AuditHandler::init(config)) {
    LOG(ERROR) << "failed to init AuditHandler";
    return false;
  }
  if (!esClient_.init(config)) {
    LOG(ERROR) << "failed to init ESClient";
    return false;
  }
  auto option = config.find("audit_log_es_batch_size");
  if (option != config.end()) {
    uint64_t batchSize = std::stoull(option->second);
    batchSize_ = batchSize > 0 ? batchSize : 1000;
  } else {
    batchSize_ = 1000;
  }

  // @TODO(zhaohaifei): Adjust to more suitable parameters
  threadPool_ = std::make_shared<nebula::thread::GenericThreadPool>();
  if (!threadPool_->start(10, "audit-log")) {
    LOG(ERROR) << "failed to start thread pool";
    return false;
  }
  auto task = [this]() {
    std::vector<std::string> docs;
    {
      folly::RWSpinLock::WriteHolder holder(docsLock_);
      if (docs_.size() > 0) {
        docs_.swap(docs);
      }
    }
    if (docs.size() > 0) {
      this->esClient_.bulk(docs);
    }
  };
  tid_ = threadPool_->addRepeatTask(1000, task);
  return true;
}

bool ESAuditHandler::handleMessage(const AuditMessage& msg) {
  if (filter_.filter(msg)) {
    return true;
  }
  std::string doc = messageToDoc(msg);
  std::vector<std::string> docs;
  {
    folly::RWSpinLock::WriteHolder holder(docsLock_);
    if (docs_.size() < batchSize_) {
      docs_.push_back(doc);
    } else {
      docs_.swap(docs);
      docs_.push_back(doc);
    }
  }
  if (docs.size() > 0) {
    auto task = [this, docs = std::move(docs)]() { this->esClient_.bulk(docs); };
    threadPool_->addTask(task);
  }
  return true;
}

std::string ESAuditHandler::messageToDoc(const AuditMessage& msg) {
  const char* format_string =
      "{{\"category\":\"{}\","
      "\"timestamp\":\"{}\","
      "\"terminal\":\"{}\","
      "\"connection_id\":\"{}\","
      "\"connection_status\":\"{}\","
      "\"connection_message\":\"{}\","
      "\"user\":\"{}\","
      "\"client_host\":\"{}\","
      "\"host\":\"{}\","
      "\"space\":\"{}\","
      "\"query\":\"{}\","
      "\"query_status\":\"{}\","
      "\"query_message\":\"{}\"}}";
  return folly::sformat(format_string,
                        msg.category_,
                        msg.timestamp_,
                        msg.terminal_,
                        msg.connectionId_,
                        msg.connectionStatus_,
                        Utils::processSpecChar(msg.connectionErrMessage_),
                        msg.user_,
                        msg.clientHost_,
                        msg.host_,
                        msg.space_,
                        msg.query_,
                        msg.queryStatus_,
                        Utils::processSpecChar(msg.queryErrMessage_));
}

}  // namespace audit
}  // namespace nebula
