/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/encryption/License.h"

#include <folly/ScopeGuard.h>
#include <folly/json.h>
#include <proxygen/lib/utils/Base64.h>
#include <proxygen/lib/utils/CryptUtil.h>
#include <sys/inotify.h>

#include <bitset>
#include <chrono>
#include <csignal>
#include <iomanip>
#include <string>
#include <string_view>

#include "common/base/Status.h"
#include "common/base/StatusOr.h"
#include "common/time/TimeUtils.h"

namespace nebula {
namespace encryption {

// AES key/block size
const unsigned int kKeySize = 32;
const unsigned int kBlockSize = 16;
constexpr std::string_view kAesKeyBase64 = "241IYjd0+MKVhiXc0PWFetV7RhmsjTCJpZslOCPC5n8=";
constexpr std::string_view kAesIvBase64 = "rjJJOkaaueQmwFTVtzBAxw==";

// RSA public key
constexpr std::string_view kPubKeyBase64 =
    "LS0tLS1CRUdJTiBSU0EgUFVCTElDIEtFWS0tLS0tCk1JSUJDZ0tDQVFFQTNGR0NvSW44VHZvYVNEYmx4RmJvL0l1VitxTF"
    "l6U0IwU2QrdGkvYzJZWm9SWkJ3c0ZuTTkKNUhWYXN6UlJ5cmZw"
    "ZlFTdFdMdThUcFlkc1l4ZkxUbmo1eWlYenlRMXluZzNnbytsZmozMXlidFNKVHNVU2pmRAo2RVNTRHlET3hvT0tRUlp1Wm"
    "k5b0NCTDRCU2o3UXpGaFpoZ3pySHZLMXJCVXUzN0ovM0ZLVnp2"
    "Tk8zSjFFbkI3CmlMODduRVJRdkRoUUxXVWQ0VDF4K3B6RXpPQkFvWnRMaE5GQTBjRjczdnZkWit1Y1UxZ3FCNU9RYitCR2"
    "VQREcKUjQwL3VWWC9zdnZXempXRDBabVI4QkQ1SC9rb3hLUGth"
    "Vm13c3ZOTCsrNk5qdENoUlV1NVBwVmFPclgwSUxMcgpKQzI5VzJuT0gvNWo3eVR4eUlsMU1uaWcyS3d6eUx0Ylh3SURBUU"
    "FCCi0tLS0tRU5EIFJTQSBQVUJMSUMgS0VZLS0tLS0K";

// Length of base64 encoded
const size_t kSigSize = 344;

// License headers/footers
const char kContentHeader[] = "----------License Content Start----------";
const char kContentFooter[] = "----------License Content End----------";
const char kKeyHeader[] = "----------License Key Start----------";
const char kKeyFooter[] = "----------License Key End----------";

// Epoch secs
const std::time_t kSecsInDay = 86400;

// Contact info
const char contactInfo[] = "Please contact Vesoft.Inc at inquiry@vesoft.com to renew the license.";

// Inotify
const size_t EVENT_SIZE(sizeof(struct inotify_event));
const size_t BUF_LEN(1024 * (EVENT_SIZE + 16));

License* License::getInstance() {
  static License instance;
  return &instance;
}

Status License::validateLicense(const std::string& licensePath) {
  LOG(INFO) << "[License] Validation started";

  // Check existence of license file
  if (!nebula::fs::FileUtils::exist(licensePath)) {
    return Status::Error("Failed to find the license file: %s", licensePath.c_str());
  }
  LOG(INFO) << "[License] File detected";

  // Parse license file to get license content
  auto contentCheckStatus = parseLicenseContent(licensePath);
  NG_RETURN_IF_ERROR(contentCheckStatus);
  auto licenseContent = contentCheckStatus.value();

  // Parse license file to get license key
  auto parseLicenseKeyStatus = parseLicenseKey(licensePath);
  NG_RETURN_IF_ERROR(parseLicenseKeyStatus);
  auto licenseKey = parseLicenseKeyStatus.value();

  // Validate the content
  NG_RETURN_IF_ERROR(checkContent(licenseContent, licenseKey));

  // Save the content to the instance
  VLOG(1) << "[License] Loading license into program";
  rawContent_ = licenseContent;
  key_ = licenseKey;
  content_ = folly::parseJson(rawContent_);
  licensePath_ = licensePath;
  VLOG(1) << "[License] Loading finished";

  // Check expiration
  NG_RETURN_IF_ERROR(checkExpiration(folly::parseJson(rawContent_)));
  LOG(INFO) << "[License] Validation succeed";
  return Status::OK();
}

Status License::generateRsaSign(const std::string& digest,
                                const std::string& prikey,
                                std::vector<char>& outBuf) {
  BIO* in = BIO_new_mem_buf(reinterpret_cast<const void*>(prikey.c_str()), -1);
  if (in == nullptr) {
    return Status::Error("BIO_new_mem_buf failed");
  }

  RSA* rsa = PEM_read_bio_RSAPrivateKey(in, nullptr, nullptr, nullptr);
  BIO_free(in);
  if (rsa == nullptr) {
    return Status::Error("PEM_read_bio_RSAPrivateKey failed");
  }

  unsigned int size = RSA_size(rsa);
  std::vector<char> sign;
  sign.resize(size);

  int ret = RSA_sign(NID_sha256,
                     reinterpret_cast<const unsigned char*>(digest.c_str()),
                     digest.length(),
                     reinterpret_cast<unsigned char*>(sign.data()),
                     &size,
                     rsa);
  RSA_free(rsa);
  if (ret != 1) {
    return Status::Error("RSA_sign failed");
  }
  outBuf = sign;
  return Status::OK();
}

Status License::VerifyRsaSign(char* rsaSig,
                              uint32_t rsaSigLen,
                              const std::string& pubkey,
                              const std::string& digest) {
  LOG(INFO) << "[License] Signature validation started";

  BIO* in = BIO_new_mem_buf(reinterpret_cast<const void*>(pubkey.c_str()), -1);
  if (in == nullptr) {
    return Status::Error("BIO_new_mem_buf failed");
  }

  // Load RSA public key
  RSA* rsa = PEM_read_bio_RSAPublicKey(in, nullptr, nullptr, nullptr);
  BIO_free(in);
  if (rsa == nullptr) {
    return Status::Error("Failed to load public key from the given string");
  }

  int ret = RSA_verify(NID_sha256,
                       reinterpret_cast<const unsigned char*>(digest.c_str()),
                       digest.length(),
                       reinterpret_cast<unsigned char*>(rsaSig),
                       rsaSigLen,
                       rsa);
  RSA_free(rsa);
  if (ret != 1) {
    logErrors();
    return Status::Error("Failed to verify the signature");
  }

  CRYPTO_cleanup_all_ex_data();
  LOG(INFO) << "[License] Signature validation succeed";
  return Status::OK();
}

Status License::computeSha256Digest(const std::string& message, std::string& outBuf) {
  // Buffer to hold the calculated digest
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int lengthOfDigest = 0;
  int res = -1;

  // Compute message digest
  EVP_MD_CTX* context = EVP_MD_CTX_new();

  res = EVP_DigestInit_ex(context, EVP_sha256(), nullptr);
  if (res != 1) {
    logErrors();
    return Status::Error("EVP_DigestInit_ex failed");
  }

  res = EVP_DigestUpdate(context, message.c_str(), message.length());
  if (res != 1) {
    logErrors();
    return Status::Error("EVP_DigestUpdate failed");
  }

  res = EVP_DigestFinal_ex(context, digest, &lengthOfDigest);
  if (res != 1) {
    logErrors();
    return Status::Error("EVP_DigestFinal_ex failed");
  }

  // Write result into buf
  outBuf = std::string(digest, digest + lengthOfDigest);
  EVP_MD_CTX_free(context);
  return Status::OK();
}

Status License::aes256Encrypt(const unsigned char* key,
                              const unsigned char* iv,
                              const std::string& ptext,
                              std::string& ctext) {
  EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
  int rc = EVP_EncryptInit_ex(ctx, EVP_aes_256_cbc(), nullptr, key, iv);
  if (rc != 1) return Status::Error("EVP_EncryptInit_ex failed");

  // Recovered text expands upto kBlockSize
  ctext.resize(ptext.size() + kBlockSize);
  int out_len1 = static_cast<int>(ctext.size());

  rc = EVP_EncryptUpdate(ctx,
                         reinterpret_cast<unsigned char*>(&ctext[0]),
                         &out_len1,
                         reinterpret_cast<const unsigned char*>(&ptext[0]),
                         static_cast<int>(ptext.size()));
  if (rc != 1) return Status::Error("EVP_EncryptUpdate failed");

  int out_len2 = static_cast<int>(ctext.size()) - out_len1;
  rc = EVP_EncryptFinal_ex(ctx, reinterpret_cast<unsigned char*>(&ctext[0]) + out_len1, &out_len2);
  if (rc != 1) return Status::Error("EVP_EncryptFinal_ex failed");

  EVP_CIPHER_CTX_free(ctx);
  // Set cipher text size now that we know it
  ctext.resize(out_len1 + out_len2);
  return Status::OK();
}

Status License::aes256Decrypt(const unsigned char* key,
                              const unsigned char* iv,
                              const std::string& ctext,
                              std::string& rtext) {
  EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
  int rc = EVP_DecryptInit_ex(ctx, EVP_aes_256_cbc(), nullptr, key, iv);
  if (rc != 1) return Status::Error("EVP_DecryptInit_ex failed");

  // Recovered text contracts upto kBlockSize
  rtext.resize(ctext.size());
  int out_len1 = static_cast<int>(rtext.size());

  rc = EVP_DecryptUpdate(ctx,
                         reinterpret_cast<unsigned char*>(&rtext[0]),
                         &out_len1,
                         reinterpret_cast<const unsigned char*>(&ctext[0]),
                         static_cast<int>(ctext.size()));
  if (rc != 1) return Status::Error("EVP_DecryptUpdate failed");

  int out_len2 = static_cast<int>(rtext.size()) - out_len1;
  rc = EVP_DecryptFinal_ex(ctx, reinterpret_cast<unsigned char*>(&rtext[0]) + out_len1, &out_len2);
  if (rc != 1) {
    logErrors();
    return Status::Error("EVP_DecryptFinal_ex failed");
  }

  EVP_CIPHER_CTX_free(ctx);
  // Set recovered text size now that we know it
  rtext.resize(out_len1 + out_len2);
  return Status::OK();
}

Status License::checkContent(const std::string& licenseContent, const std::string& licenseKey) {
  // Extract AES cipher from licenseKey
  const size_t licenseKeySize = licenseKey.size();
  std::string aesCipherBase64 = licenseKey.substr(0, licenseKeySize - kSigSize);
  auto aesCipherBase64paddingSz =
      aesCipherBase64.size() - (aesCipherBase64.find_last_not_of('=') + 1);
  auto aesCipherText = proxygen::Base64::decode(aesCipherBase64, aesCipherBase64paddingSz);

  // Extract RSA signature from licenseKey
  std::string rsaSigBase64 = licenseKey.substr(licenseKeySize - kSigSize);
  auto rsaSigBase64paddingSz = rsaSigBase64.size() - (rsaSigBase64.find_last_not_of('=') + 1);
  auto rsaSig = proxygen::Base64::decode(rsaSigBase64, rsaSigBase64paddingSz);

  // Calculate message digest of AES256 encrypted license content
  auto kAesKeyBase64paddingSz = kAesKeyBase64.size() - (kAesKeyBase64.find_last_not_of('=') + 1);
  const std::string aesKey =
      proxygen::Base64::decode(std::string(kAesKeyBase64), kAesKeyBase64paddingSz);
  auto kAesIvBase64paddingSz = kAesIvBase64.size() - (kAesIvBase64.find_last_not_of('=') + 1);
  const std::string aesIv =
      proxygen::Base64::decode(std::string(kAesIvBase64), kAesIvBase64paddingSz);
  std::string encryptedBody = "";

  NG_RETURN_IF_ERROR(aes256Encrypt(reinterpret_cast<const unsigned char*>(aesKey.c_str()),
                                   reinterpret_cast<const unsigned char*>(aesIv.c_str()),
                                   licenseContent,
                                   encryptedBody));
  std::string digestBuf = "";
  NG_RETURN_IF_ERROR(computeSha256Digest(encryptedBody, digestBuf));

  // Validate rsa signature
  auto kPubKeyBase64paddingSz = kPubKeyBase64.size() - (kPubKeyBase64.find_last_not_of('=') + 1);
  const std::string pubKey =
      proxygen::Base64::decode(std::string(kPubKeyBase64), kPubKeyBase64paddingSz);
  NG_RETURN_IF_ERROR(VerifyRsaSign(const_cast<char*>(rsaSig.c_str()), 256, pubKey, digestBuf));

  // Decrypt license content
  std::string rtext = "";
  NG_RETURN_IF_ERROR(aes256Decrypt(reinterpret_cast<const unsigned char*>(aesKey.c_str()),
                                   reinterpret_cast<const unsigned char*>(aesIv.c_str()),
                                   aesCipherText,
                                   rtext));

  auto contentJson = folly::parseJson(rtext);
  VLOG(1) << "[License] content JSON: " << folly::toPrettyJson(contentJson);

  // Clean key string
  OPENSSL_cleanse(reinterpret_cast<void*>(const_cast<char*>(aesKey.c_str())), kKeySize);
  OPENSSL_cleanse(reinterpret_cast<void*>(const_cast<char*>(aesIv.c_str())), kBlockSize);

  return Status::OK();
}

Status License::checkExpiration(const folly::dynamic& content) {
  // Get expiration date from content
  auto expiration = content["expirationDate"].asString();
  // Get current time and covert to timestamp
  auto currentTime = std::chrono::system_clock::now();
  std::time_t currentTimestamp = std::chrono::system_clock::to_time_t(currentTime);

  // Convert expiration datetime to timestamp
  std::tm t{};
  std::istringstream ss(expiration);
  ss >> std::get_time(&t, "%Y-%m-%dT%H:%M:%S");
  if (ss.fail()) {
    return Status::Error("failed to parse time string");
  }
  std::time_t expirationTimestamp = mktime(&t);

  std::time_t gracePeriod = 0;
  std::time_t warningAhead = 0;
  // Check if the license has the attribute "gracePeriod", it is a license from v3.0.0 or earlier
  // If the license does not contain "gracePeriod", set the grace period to 14 days as the default
  if (content.find("gracePeriod") == content.items().end()) {
    gracePeriod = 14 * kSecsInDay;
    warningAhead = 30 * kSecsInDay;
  } else {
    // Determine if the license is a trial license, for trial license, no grace period is given
    if (content["gracePeriod"].asInt() != 0) {
      gracePeriod = content["gracePeriod"].asInt() * kSecsInDay;
      warningAhead = 30 * kSecsInDay;
    } else {  // Trial license
      gracePeriod = 0;
      warningAhead = 7 * kSecsInDay;
    }
  }

  // Give the user a grace period before disable the launch of meta service
  // The length of the grace period is specified in the license
  if (currentTimestamp > expirationTimestamp + gracePeriod) {
    return Status::Error("The license has expired on %s. %s", expiration.c_str(), contactInfo);
  } else if (currentTimestamp > expirationTimestamp) {
    LOG(WARNING) << folly::sformat(
        "[License] The license has expired on {}. You can still use Nebula service for {} days. {}",
        expiration.c_str(),
        gracePeriod,
        contactInfo);

  } else if (currentTimestamp > expirationTimestamp - warningAhead) {
    // Warning about the expiration 30 days ahead
    LOG(WARNING) << folly::sformat(
        "[License] The license will be expired in {} days. The expiration date is {}. {}",
        warningAhead,
        expiration.c_str(),
        contactInfo);
  }

  VLOG(2) << "[License] Expiration check passed";
  return Status::OK();
}

StatusOr<std::string> License::parseLicenseContent(const std::string& licensePath) {
  nebula::fs::FileUtils::FileLineIterator iter(licensePath);
  if (!iter.valid()) {
    return Status::Error("Failed to parse license content, file path: %s, errno: %s",
                         licensePath.c_str(),
                         ::strerror(errno));
  }

  // Check license header
  if (iter.entry() != kContentHeader) {
    return Status::Error("Invalid license header");
  }

  std::string licenseContent = "";
  bool parsing = false;
  // Parse license content
  while (iter.valid()) {
    if (iter.entry() == kContentHeader) {
      parsing = true;
      ++iter;
    }
    if (iter.entry() == kContentFooter) {
      parsing = false;
    }
    if (parsing) {
      licenseContent.append(iter.entry() + '\n');
    }
    ++iter;
  }
  // TODO(Aiee) remove this part after intergrating license generation/authentication in the server
  // side. Delete newline literal at the end
  licenseContent.erase(licenseContent.end() - 1, licenseContent.end());

  VLOG(2) << "[License] Content parsing passed";
  return licenseContent;
}

StatusOr<std::string> License::parseLicenseKey(const std::string& licensePath) {
  nebula::fs::FileUtils::FileLineIterator iter(licensePath);
  if (!iter.valid()) {
    return Status::Error("Failed to parse license key, file path: %s, errno: %s",
                         licensePath.c_str(),
                         ::strerror(errno));
  }

  std::string licenseKey = "";
  bool parsing = false;
  // Parse license key
  while (iter.valid()) {
    if (iter.entry() == kKeyHeader) {
      parsing = true;
      ++iter;
    }
    if (iter.entry() == kKeyFooter) {
      parsing = false;
    }
    if (parsing) {
      licenseKey.append(iter.entry() + '\n');
    }
    ++iter;
  }
  // TODO(Aiee) remove this part after intergrating license generation/authentication in the server
  // side. Delete newline literal at the end
  licenseKey.erase(licenseKey.end() - 1, licenseKey.end());

  VLOG(2) << "[License] Key check passed";
  return licenseKey;
}

void License::setLicenseMonitor(const std::string& licensePath, int& inotifyFd) {
  LOG(INFO) << "[License Monitor] File monitor started";

  int inotifyLength, inotifyI = 0;
  char inotifyBuffer[BUF_LEN];

  inotifyLength = read(inotifyFd, inotifyBuffer, BUF_LEN);

  if (inotifyLength < 0) {
    LOG(ERROR) << "read error";
    return;
  }
  VLOG(3) << "inotifyI: " << inotifyI << " inotifyLength: " << inotifyLength;
  while (inotifyI < inotifyLength) {
    struct inotify_event* event = reinterpret_cast<struct inotify_event*>(&inotifyBuffer[inotifyI]);
    VLOG(3) << "Event name: " << event->name;

    // Ignore other file changes
    if (std::strcmp(event->name, "nebula.license") != 0) {
      return;
    }

    // Check event
    if (event->len) {
      if (event->mask & IN_CLOSE_WRITE) {
        if (event->mask & IN_ISDIR) {
          // Ignore directory change
        } else {
          LOG(INFO) << folly::sformat("[License Monitor] The license file {} was modified.",
                                      event->name);
          auto ret = validateLicense(licensePath);
          if (!ret.ok()) {
            LOG(ERROR) << "Failed to validate license: " << ret;
          }
        }
      }
    }
    inotifyI += EVENT_SIZE + event->len;
  }
}

const folly::dynamic& License::getContent() const {
  if (content_.empty()) {
    LOG(ERROR) << "[License] data was not loaded";
  }
  return content_;
}

void License::setContent(const folly::dynamic& content) {
  content_ = content;
}

const std::string& License::getRawContent() const {
  if (rawContent_.empty()) {
    LOG(ERROR) << "[License] data was not loaded";
  }
  return rawContent_;
}

const std::string& License::getkey() const {
  if (key_.empty()) {
    LOG(ERROR) << "[License] data was not loaded";
  }
  return key_;
}

const std::string& License::getLicensePath() const {
  if (licensePath_.empty()) {
    LOG(ERROR) << "[License] data was not loaded";
  }
  return licensePath_;
}

std::string License::getLicenseDirPath() const {
  auto licensePath = getLicensePath();
  auto siz = licensePath.find_last_of('/');
  auto licenseDirPath = licensePath.substr(0, siz);

  return licenseDirPath;
}

}  // namespace encryption
}  // namespace nebula
