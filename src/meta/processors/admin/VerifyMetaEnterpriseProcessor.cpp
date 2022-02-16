/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/VerifyMetaEnterpriseProcessor.h"

namespace nebula {
namespace meta {
void VerifyMetaEnterpriseProcessor::process(const cpp2::VerifyMetaEnterpriseReq& req) {
  UNUSED(req);

  resp_.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  onFinished();
}
}  // namespace meta
}  // namespace nebula
