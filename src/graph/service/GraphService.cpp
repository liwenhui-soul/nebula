/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/service/GraphService.h"

#include <proxygen/lib/utils/CryptUtil.h>

#include <boost/filesystem.hpp>

#include "audit/AuditLogging.h"
#include "clients/storage/StorageClient.h"
#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/stats/StatsManager.h"
#include "common/time/Duration.h"
#include "common/time/TimezoneInfo.h"
#include "common/utils/Utils.h"
#include "graph/service/CloudAuthenticator.h"
#include "graph/service/GraphFlags.h"
#include "graph/service/IpWhitelistCheck.h"
#include "graph/service/LdapAuthenticator.h"
#include "graph/service/PasswordAuthenticator.h"
#include "graph/service/RequestContext.h"
#include "graph/stats/GraphStats.h"
#include "version/Version.h"

namespace nebula {
namespace graph {

Status GraphService::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor,
                          const HostAddr& hostAddr) {
  auto addrs = network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
  if (!addrs.ok()) {
    return addrs.status();
  }

  meta::MetaClientOptions options;
  options.serviceName_ = "graph";
  options.skipConfig_ = FLAGS_local_config;
  options.role_ = meta::cpp2::HostRole::GRAPH;
  options.localHost_ = hostAddr;
  options.gitInfoSHA_ = gitInfoSha();
  options.rootPath_ = boost::filesystem::current_path().string();

  metaClient_ = std::make_unique<meta::MetaClient>(ioExecutor, std::move(addrs.value()), options);

  // Load data try 3 time
  bool loadDataOk = metaClient_->waitForMetadReady(3);
  if (!loadDataOk) {
    // Resort to retrying in the background
    LOG(ERROR) << "Failed to wait for meta service ready synchronously.";
    return Status::Error("Failed to wait for meta service ready synchronously.");
  }

  sessionManager_ = std::make_unique<GraphSessionManager>(metaClient_.get(), hostAddr);
  auto initSessionMgrStatus = sessionManager_->init();
  if (!initSessionMgrStatus.ok()) {
    LOG(ERROR) << "Failed to initialize session manager: " << initSessionMgrStatus.toString();
    return Status::Error("Failed to initialize session manager: %s",
                         initSessionMgrStatus.toString().c_str());
  }

  queryEngine_ = std::make_unique<QueryEngine>();
  return queryEngine_->init(std::move(ioExecutor), metaClient_.get());
}

folly::Future<AuthResponse> GraphService::future_authenticate(const std::string& username,
                                                              const std::string& password) {
  auto* peer = getRequestContext()->getPeerAddress();
  auto clientIp = peer->getAddressStr();
  LOG(INFO) << "Authenticating user " << username << " from " << peer->describe();

  if (peer->isIPv4Mapped()) {
    folly::IPAddress v6map(clientIp);
    clientIp = folly::IPAddress::createIPv4(v6map).str();
  }

  auto ctx = std::make_unique<RequestContext<AuthResponse>>();
  auto future = ctx->future();

  if (FLAGS_enable_audit) {
    ctx->auditContext().category_ = "login";
    ctx->auditContext().user_ = username;
    ctx->auditContext().clientHost_ = clientIp;
  }
  // Check username and password failed
  auto clientAddr = HostAddr(peer->getAddressStr(), peer->getPort());
  auto authResult = auth(username, password, clientAddr);

  if (!authResult.ok()) {
    ctx->resp().errorCode = ErrorCode::E_BAD_USERNAME_PASSWORD;
    ctx->resp().errorMsg.reset(new std::string(authResult.toString()));
    auditLogin(ctx->auditContext(), ctx->resp().errorCode, *(ctx->resp().errorMsg));
    ctx->finish();
    stats::StatsManager::addValue(kNumAuthFailedSessions);
    stats::StatsManager::addValue(kNumAuthFailedSessionsBadUserNamePassword);
    return future;
  }

  if (sessionManager_->isOutOfConnections()) {
    ctx->resp().errorCode = ErrorCode::E_TOO_MANY_CONNECTIONS;
    ctx->resp().errorMsg.reset(new std::string("Too many connections in the cluster"));
    auditLogin(ctx->auditContext(), ctx->resp().errorCode, *(ctx->resp().errorMsg));
    ctx->finish();
    stats::StatsManager::addValue(kNumAuthFailedSessions);
    stats::StatsManager::addValue(kNumAuthFailedSessionsOutOfMaxAllowed);
    return future;
  }

  auto cb = [user = username, cIp = clientIp, ctx = std::move(ctx)](
                StatusOr<std::shared_ptr<ClientSession>> ret) mutable {
    VLOG(2) << "Create session doFinish";
    if (!ret.ok()) {
      LOG(ERROR) << "Create session for userName: " << user << ", ip: " << cIp
                 << " failed: " << ret.status();
      ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
      ctx->resp().errorMsg.reset(new std::string(ret.status().toString()));
      auditLogin(ctx->auditContext(), ctx->resp().errorCode, *(ctx->resp().errorMsg));
      return ctx->finish();
    }
    auto sessionPtr = std::move(ret).value();
    if (sessionPtr == nullptr) {
      LOG(ERROR) << "Get session for sessionId is nullptr";
      ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
      ctx->resp().errorMsg.reset(new std::string("Get session for sessionId is nullptr"));
      auditLogin(ctx->auditContext(), ctx->resp().errorCode, *(ctx->resp().errorMsg));
      return ctx->finish();
    }
    stats::StatsManager::addValue(kNumOpenedSessions);
    stats::StatsManager::addValue(kNumActiveSessions);
    ctx->setSession(sessionPtr);
    ctx->resp().sessionId.reset(new int64_t(ctx->session()->id()));
    ctx->resp().timeZoneOffsetSeconds.reset(
        new int32_t(time::Timezone::getGlobalTimezone().utcOffsetSecs()));
    ctx->resp().timeZoneName.reset(
        new std::string(time::Timezone::getGlobalTimezone().stdZoneName()));
    ctx->auditContext().connectionId_ = ctx->session()->id();
    auditLogin(ctx->auditContext(), ErrorCode::SUCCEEDED, "");
    return ctx->finish();
  };

  sessionManager_->createSession(username, clientIp, getThreadManager()).thenValue(std::move(cb));
  return future;
}

void GraphService::signout(int64_t sessionId) {
  VLOG(2) << "Sign out session " << sessionId;
  if (FLAGS_enable_audit) {
    AuditContext auditCtx;
    auditCtx.category_ = "exit";
    auditCtx.connectionId_ = sessionId;
    // In order not to affect the main thread, findSessionFromCache() is called here
    // instead of findSession(). If session is not obtained, audit would not record user,
    // client ip. In that case, users can distinguish different records by sessionId.
    std::shared_ptr<ClientSession> clientSessionPtr =
        sessionManager_->findSessionFromCache(sessionId);
    if (clientSessionPtr != nullptr) {
      auditCtx.user_ = clientSessionPtr->user();
      auditCtx.clientHost_ = clientSessionPtr->clientIp();
    }
    auditExit(auditCtx);
  }
  sessionManager_->removeSession(sessionId);
}

folly::Future<ExecutionResponse> GraphService::future_executeWithParameter(
    int64_t sessionId,
    const std::string& query,
    const std::unordered_map<std::string, Value>& parameterMap) {
  auto ctx = std::make_unique<RequestContext<ExecutionResponse>>();
  ctx->setQuery(query);
  ctx->setRunner(getThreadManager());
  ctx->setSessionMgr(sessionManager_.get());
  auto future = ctx->future();
  if (FLAGS_enable_audit) {
    ctx->auditContext().connectionId_ = sessionId;
    ctx->auditContext().query_ = query;
    // In order not to affect the main thread, findSessionFromCache() is called here
    // instead of findSession(). If session is not obtained, audit would not record user,
    // client ip and space. In that case, users can distinguish different records by sessionId.
    std::shared_ptr<ClientSession> clientSessionPtr =
        sessionManager_->findSessionFromCache(sessionId);
    if (clientSessionPtr != nullptr) {
      ctx->auditContext().user_ = clientSessionPtr->user();
      ctx->auditContext().clientHost_ = clientSessionPtr->clientIp();
      ctx->auditContext().space_ = clientSessionPtr->spaceName();
    }
  }
  // When the sessionId is 0, it means the clients to ping the connection is ok
  if (sessionId == 0) {
    ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
    ctx->resp().errorMsg = std::make_unique<std::string>("Invalid session id");
    ctx->finish();
    // Before connecting to graphd, clients will ping whether the network is ok.
    // Therefore, these ping operations are no need to audite.
    return future;
  }
  auto cb = [this, sessionId, ctx = std::move(ctx), parameterMap = std::move(parameterMap)](
                StatusOr<std::shared_ptr<ClientSession>> ret) mutable {
    if (!ret.ok()) {
      LOG(ERROR) << "Get session for sessionId: " << sessionId << " failed: " << ret.status();
      ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
      ctx->resp().errorMsg.reset(new std::string(folly::stringPrintf(
          "Get sessionId[%ld] failed: %s", sessionId, ret.status().toString().c_str())));
      auditQuery(ctx->auditContext(), ctx->resp().errorCode, *(ctx->resp().errorMsg));
      return ctx->finish();
    }
    auto sessionPtr = std::move(ret).value();
    if (sessionPtr == nullptr) {
      LOG(ERROR) << "Get session for sessionId: " << sessionId << " is nullptr";
      ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
      ctx->resp().errorMsg.reset(
          new std::string(folly::stringPrintf("SessionId[%ld] does not exist", sessionId)));
      auditQuery(ctx->auditContext(), ctx->resp().errorCode, *(ctx->resp().errorMsg));
      return ctx->finish();
    }
    stats::StatsManager::addValue(kNumQueries);
    stats::StatsManager::addValue(kNumActiveQueries);
    if (FLAGS_enable_space_level_metrics && sessionPtr->space().name != "") {
      stats::StatsManager::addValue(stats::StatsManager::counterWithLabels(
          kNumQueries, {{"space", sessionPtr->space().name}}));
      stats::StatsManager::addValue(stats::StatsManager::counterWithLabels(
          kNumActiveQueries, {{"space", sessionPtr->space().name}}));
    }
    auto& spaceName = sessionPtr->space().name;
    ctx->setSession(std::move(sessionPtr));
    ctx->setParameterMap(parameterMap);
    queryEngine_->execute(std::move(ctx));
    stats::StatsManager::decValue(kNumActiveQueries);
    if (FLAGS_enable_space_level_metrics && spaceName != "") {
      stats::StatsManager::decValue(
          stats::StatsManager::counterWithLabels(kNumActiveQueries, {{"space", spaceName}}));
    }
  };
  sessionManager_->findSession(sessionId, getThreadManager()).thenValue(std::move(cb));
  return future;
}

folly::Future<ExecutionResponse> GraphService::future_execute(int64_t sessionId,
                                                              const std::string& query) {
  return future_executeWithParameter(sessionId, query, std::unordered_map<std::string, Value>{});
}

folly::Future<std::string> GraphService::future_executeJson(int64_t sessionId,
                                                            const std::string& query) {
  return future_executeJsonWithParameter(
      sessionId, query, std::unordered_map<std::string, Value>{});
}

folly::Future<std::string> GraphService::future_executeJsonWithParameter(
    int64_t sessionId,
    const std::string& query,
    const std::unordered_map<std::string, Value>& parameterMap) {
  return future_executeWithParameter(sessionId, query, parameterMap).thenValue([](auto&& resp) {
    return folly::toJson(resp.toJson());
  });
}

Status GraphService::auth(const std::string& username,
                          const std::string& password,
                          const HostAddr& hostAddr) {
  auto metaClient = queryEngine_->metaClient();

  // Skip authentication if FLAGS_enable_authorize is false
  if (!FLAGS_enable_authorize) {
    return Status::OK();
  }

  // Check IP white list
  auto ipWhiteListChecker = std::make_unique<IpWhitelistCheck>(metaClient);
  NG_RETURN_IF_ERROR(ipWhiteListChecker->check(username, hostAddr.host));

  // Authenticate via diffrent auth types
  if (FLAGS_auth_type == "password") {
    // Auth with PasswordAuthenticator
    auto authenticator = std::make_unique<PasswordAuthenticator>(metaClient);
    return authenticator->auth(username, proxygen::md5Encode(folly::StringPiece(password)));
  } else if (FLAGS_auth_type == "cloud") {
    // Cloud user and native user will be mixed.
    // Since cloud user and native user has the same transport protocol,
    // There is no way to identify which one is in the graph layer，
    // let's check the native user's password first, then cloud user.
    auto pwdAuth = std::make_unique<PasswordAuthenticator>(metaClient);
    return pwdAuth->auth(username, proxygen::md5Encode(folly::StringPiece(password)));
    auto cloudAuth = std::make_unique<CloudAuthenticator>(metaClient);
    return cloudAuth->auth(username, password);
  } else if (FLAGS_auth_type == "ldap") {
    auto authenticator = std::make_unique<LdapAuthenticator>(metaClient);
    return authenticator->auth(username, password);
  }

  LOG(WARNING) << "Unknown auth type: " << FLAGS_auth_type;
  return Status::Error("Unknown auth type: %s", FLAGS_auth_type.c_str());
}

folly::Future<cpp2::VerifyClientVersionResp> GraphService::future_verifyClientVersion(
    const cpp2::VerifyClientVersionReq& req) {
  std::unordered_set<std::string> whiteList;
  folly::splitTo<std::string>(
      ":", FLAGS_client_white_list, std::inserter(whiteList, whiteList.begin()));
  cpp2::VerifyClientVersionResp resp;

  if (FLAGS_enable_client_white_list && whiteList.find(req.get_version()) == whiteList.end()) {
    std::string uniqueWhiteList;
    std::for_each(whiteList.begin(), whiteList.end(), [&uniqueWhiteList](auto& version) {
      uniqueWhiteList.append(version);
    });
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_CLIENT_SERVER_INCOMPATIBLE;
    resp.error_msg_ref() = folly::stringPrintf(
        "Graph client version(%s) is not accepted, current graph client white list: %s.",
        req.get_version().c_str(),
        uniqueWhiteList.c_str());
  } else {
    resp.error_code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  return folly::makeFuture<cpp2::VerifyClientVersionResp>(std::move(resp));
}
}  // namespace graph
}  // namespace nebula
