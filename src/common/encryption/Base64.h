/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_ENCRYPTION_BASE64_H_
#define COMMON_ENCRYPTION_BASE64_H_

#include <proxygen/lib/utils/CryptUtil.h>

namespace nebula {
namespace encryption {

class Base64 final {
 public:
  Base64() = delete;

  static std::string encode(const std::string& toEncodeStr);
  static std::string decode(const std::string& toDecodeStr);
};

}  // namespace encryption
}  // namespace nebula
#endif  // COMMON_ENCRYPTION_BASE64_H_
