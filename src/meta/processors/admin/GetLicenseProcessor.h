/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETLICENSEPROCESSOR_H_
#define META_GETLICENSEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {
class GetLicenseProcessor final : public BaseProcessor<cpp2::GetLicenseResp> {
 public:
  static GetLicenseProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetLicenseProcessor(kvstore);
  }

  // This interface is enterprise exclusive
  // This processor should return the license content and key.
  // The parameter is unused.
  void process(const cpp2::GetLicenseReq& req);

 private:
  explicit GetLicenseProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetLicenseResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula
#endif  // META_GETLICENSEPROCESSOR_H_
