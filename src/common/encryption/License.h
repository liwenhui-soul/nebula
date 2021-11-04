/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <openssl/err.h>
#include <openssl/pem.h>

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/encryption/Base64.h"
#include "common/fs/FileUtils.h"

using nebula::Status;
using nebula::StatusOr;

namespace nebula {
namespace encryption {
// AES key/block size
extern const unsigned int kKeySize;
extern const unsigned int kBlockSize;
extern const char kAesKeyBase64[];
extern const char kAesIvBase64[];
// RSA public key
extern const char kPubKeyBase64[];
// Length of base64 encoded signature
extern const size_t kSigSize;
// License headers/footers
extern const char kContentHeader[];
extern const char kContentFooter[];
extern const char kKeyHeader[];
extern const char kKeyFooter[];
// Epoch secs
extern const std::time_t kSecsInWeek;
extern const std::time_t kSecsInDay;

class License final {
 private:
  License() = default;
  ~License() = default;
  License(const License& license);
  const License operator=(const License& license);

 public:
  static License* getInstance();

  // Read License and validate
  static Status validateLicense(const std::string& licensePath);

  // Sign a RSA signature
  static Status generateRsaSign(const std::string& digest,
                                const std::string& prikey,
                                std::vector<char>& outBuf);

  // Verify RSA signature
  static Status VerifyRsaSign(char* rsaSig,
                              uint32_t rsaSigLen,
                              const std::string& pubkey,
                              const std::string& digest);

  // Compute the message digest using sha256
  static Status computeSha256Digest(const std::string& message, std::string& outBuf);

  static Status aes256Encrypt(const unsigned char* key,
                              const unsigned char* iv,
                              const std::string& ptext,
                              std::string& ctext);

  static Status aes256Decrypt(const unsigned char* key,
                              const unsigned char* iv,
                              const std::string& ctext,
                              std::string& rtext);

  // Check the expiration of the license
  static Status checkExpiration(const std::string datetime);

  // Parse license file to get license body
  static Status parseLicenseContent(const std::string& licensePath, std::string& licenseContent);

  // Parse license file to get license key
  static Status parseLicenseKey(const std::string& licensePath, std::string& licenseKey);

  static void logErrors() {
    char errBuf[1024];
    ERR_error_string(ERR_get_error(), errBuf);
    LOG(ERROR) << errBuf;
  }

 public:
  // Dynamic object representing the license info
  folly::dynamic content;
};

}  // namespace encryption
}  // namespace nebula
