/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "webservice/GetLicenseHandler.h"

#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>

#include "common/encryption/License.h"

namespace nebula {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::ResponseBuilder;
using proxygen::UpgradeProtocol;

void GetLicenseHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
  if (headers->getMethod().value() != HTTPMethod::GET) {
    // Unsupported method
    err_ = HttpCode::E_UNSUPPORTED_METHOD;
    return;
  }
}

void GetLicenseHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
  // Do nothing, we only support GET
}

void GetLicenseHandler::onEOM() noexcept {
  switch (err_) {
    case HttpCode::E_UNSUPPORTED_METHOD:
      ResponseBuilder(downstream_)
          .status(WebServiceUtils::to(HttpStatusCode::METHOD_NOT_ALLOWED),
                  WebServiceUtils::toString(HttpStatusCode::METHOD_NOT_ALLOWED))
          .sendWithEOM();
      return;
    default:
      break;
  }

  folly::dynamic vals = getLicense();
  ResponseBuilder(downstream_)
      .status(WebServiceUtils::to(HttpStatusCode::OK),
              WebServiceUtils::toString(HttpStatusCode::OK))
      .body(folly::toJson(vals))
      .sendWithEOM();
}

void GetLicenseHandler::onUpgrade(UpgradeProtocol) noexcept {
  // Do nothing
}

void GetLicenseHandler::requestComplete() noexcept {
  delete this;
}

void GetLicenseHandler::onError(ProxygenError error) noexcept {
  LOG(ERROR) << "Web service StorageHttpHandler got error: " << proxygen::getErrorString(error);
}

const folly::dynamic GetLicenseHandler::getLicense() const {
  auto licenseIns = encryption::License::getInstance();
  auto licenseContent = licenseIns->getContent();
  return licenseContent;
}

}  // namespace nebula
