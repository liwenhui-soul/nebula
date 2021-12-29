/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/service/IpWhitelistCheck.h"

namespace nebula {
namespace graph {

IpWhitelistCheck::IpWhitelistCheck(meta::MetaClient* client) { metaClient_ = client; }

bool IpWhitelistCheck::check(const std::string& user, const std::string& clientIp) {
  return metaClient_->checkIpWhitelistFromCache(user, clientIp);
}
}  // namespace graph
}  // namespace nebula
