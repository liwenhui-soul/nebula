/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/encryption/Base64.h"

#include <openssl/evp.h>
#include <proxygen/lib/utils/Base64.h>

namespace nebula {
namespace encryption {

std::string Base64::encode(const std::string &toEncodeStr) {
  return proxygen::base64Encode(folly::ByteRange(
      reinterpret_cast<const unsigned char *>(toEncodeStr.c_str()), toEncodeStr.length()));
}

std::string Base64::decode(const std::string &toDecodeStr) {
  auto padding = std::count(toDecodeStr.begin(), toDecodeStr.end(), '=');
  return proxygen::Base64::decode(toDecodeStr, padding);
}

}  // namespace encryption
}  // namespace nebula
