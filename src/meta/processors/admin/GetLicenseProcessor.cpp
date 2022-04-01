/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/GetLicenseProcessor.h"

#include "common/encryption/License.h"

namespace nebula {
namespace meta {
void GetLicenseProcessor::process(const cpp2::GetLicenseReq& req) {
  UNUSED(req);
  LOG(INFO) << "Received GetLicense request";
  std::string licenseContent = "";
  std::string licenseKey = "";

  // Get singleton
  auto licenseIns = encryption::License::getInstance();

  // Parse license file to get license content
  auto contentCheckStatus = encryption::License::parseLicenseContent(licenseIns->getLicensePath());
  if (!contentCheckStatus.ok()) {
    LOG(ERROR) << "Failed to parse the license file: " << contentCheckStatus.status();
  } else {
    licenseContent = std::move(contentCheckStatus).value();
  }

  // Parse license file to get license key
  auto parseLicenseKeyStatus = encryption::License::parseLicenseKey(licenseIns->getLicensePath());
  if (!parseLicenseKeyStatus.ok()) {
    LOG(ERROR) << "Failed to parse the license file: " << parseLicenseKeyStatus.status();
  } else {
    licenseKey = parseLicenseKeyStatus.value();
  }

  // Fill license into response
  if (licenseContent.empty() || licenseKey.empty()) {
    // Return error if parsing failed
    resp_.code_ref() = nebula::cpp2::ErrorCode::E_PARSING_LICENSE_FAILURE;
    resp_.error_msg() = "Failed to parse the license file";
  } else {
    resp_.licenseContent_ref() = licenseContent;
    resp_.licenseKey_ref() = licenseKey;
    resp_.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  onFinished();
}
}  // namespace meta
}  // namespace nebula
