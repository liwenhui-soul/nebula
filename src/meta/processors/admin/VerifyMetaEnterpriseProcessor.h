/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_VERIFYMETAENTERPRISEPROCESSOR_H_
#define META_VERIFYMETAENTERPRISEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {
class VerifyMetaEnterpriseProcessor final : public BaseProcessor<cpp2::VerifyMetaEnterpriseResp> {
 public:
  static VerifyMetaEnterpriseProcessor* instance(kvstore::KVStore* kvstore) {
    return new VerifyMetaEnterpriseProcessor(kvstore);
  }

  // This interface is enterprise exclusive, this processor should return true as long as the meta
  // service is enterprise version.
  // The parameter is unused.
  void process(const cpp2::VerifyMetaEnterpriseReq& req);

 private:
  explicit VerifyMetaEnterpriseProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::VerifyMetaEnterpriseResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula
#endif  // META_VERIFYMETAENTERPRISEPROCESSOR_H_
