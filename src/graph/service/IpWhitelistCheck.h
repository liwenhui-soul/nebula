/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_SERVICE_IPWHITELISTCHECK_H_
#define GRAPH_SERVICE_IPWHITELISTCHECK_H_

#include "clients/meta/MetaClient.h"

namespace nebula {
namespace graph {

class IpWhitelistCheck final {
 public:
  explicit IpWhitelistCheck(meta::MetaClient* client);

  Status check(const std::string& user, const std::string& clientIp);

 private:
  meta::MetaClient* metaClient_{nullptr};
};
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_WHITELISTCHECK_H_
