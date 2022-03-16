/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef WEBSERVICE_GETLICENSEHANDLER_H_
#define WEBSERVICE_GETLICENSEHANDLER_H_

#include <folly/dynamic.h>
#include <proxygen/httpserver/RequestHandler.h>

#include "common/base/Base.h"
#include "webservice/Common.h"

namespace nebula {

class GetLicenseHandler : public proxygen::RequestHandler {
 public:
  GetLicenseHandler() = default;

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;

 protected:
  const folly::dynamic getLicense() const;

 protected:
  HttpCode err_{HttpCode::SUCCEEDED};
  bool returnJson_{false};
  std::vector<std::string> statNames_;
};

}  // namespace nebula
#endif  // WEBSERVICE_GETLICENSEHANDLER_H_
