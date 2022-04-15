/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <folly/dynamic.h>
#include <gtest/gtest_prod.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <rocksdb/filter_policy.h>

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/fs/FileUtils.h"

namespace nebula {
namespace encryption {

class License final {
  FRIEND_TEST(LicenseGeneratorTool, BloomFilterTest);
  FRIEND_TEST(LicenseGeneratorTool, HardwareCheckTest);

 private:
  License() = default;
  ~License() = default;
  License(const License& license);
  const License operator=(const License& license);

 public:
  // Gets the singleton
  static License* getInstance();

  // Reads and validates License, save the parsed license content into content_
  // This method is called when the meta process starts
  Status validateLicense(const std::string& licensePath);

  // Signs a RSA signature
  static Status generateRsaSign(const std::string& digest,
                                const std::string& prikey,
                                std::string& outBuf);

  // Verifies RSA signature
  static Status VerifyRsaSign(char* rsaSig,
                              uint32_t rsaSigLen,
                              const std::string& pubkey,
                              const std::string& digest);

  // Computes the message digest using sha256
  static Status computeSha256Digest(const std::string& message, std::string& outBuf);

  static Status aes256Encrypt(const unsigned char* key,
                              const unsigned char* iv,
                              const std::string& ptext,
                              std::string& ctext);

  static Status aes256Decrypt(const unsigned char* key,
                              const unsigned char* iv,
                              const std::string& ctext,
                              std::string& rtext);

  // Checks the expiration of the license
  // For trial license, warn the user 7 days before the expiration and no grace period is given
  // For official license, warn the user 30 days before the expiration and a grace period is given
  static Status checkExpiration(const folly::dynamic& content);

  // Checks the expiration of the license
  static Status checkContent(const std::string& licenseContent, const std::string& licenseKey);

  // Parses license file to get license body as a folly::dynamic object
  static StatusOr<std::string> parseLicenseContent(const std::string& licensePath);

  // Parses license file to get license key as a string
  static StatusOr<std::string> parseLicenseKey(const std::string& licensePath);

  static void logErrors() {
    char errBuf[1024];
    ERR_error_string(ERR_get_error(), errBuf);
    LOG(ERROR) << errBuf;
  }

  // Get the default ase key
  static std::string getAesKey();

  // Get the default ase IV
  static std::string getAesIV();

  // Sets a dynamic object representing the license info
  void setContent(const folly::dynamic& content);

  // Returns a dynamic object representing the license info
  const folly::dynamic& getContent() const;

  // Returns a string representing the license content
  const std::string& getRawContent() const;

  // Returns a string representing the license key
  const std::string& getkey() const;

  // Returns a the license path
  const std::string& getLicensePath() const;

  // Returns a the license dir path
  std::string getLicenseDirPath() const;

  // **************** File Monitor ****************
  // Sets up license monitor using inotify
  // This function will be called in metaDaemon with a function scheduler to execute repeatedly
  void setLicenseMonitor(const std::string& licensePath, int& inotifyFd);

  // **************** Hardware Registration ****************
  // Generates a string as an unique id of the machine, return an empty string on failure.
  static std::string genMachineCode();

  // Generates a string as an unique id of the CLUSTER, return an empty string on failure.
  // The input is a vector of strings representing machine codes.
  static std::string genClusterCode(const std::vector<std::string>& keyVec);

  // Validates the machine code of the current machine with license
  static Status checkMachineCode(const std::string& machineCode, const std::string& clusterCode);

  // Checks if the machine is regirsted in the license or not.
  // This check should only be executed once when the meta service launches.
  Status checkHardware();

 private:
  // Generates a bit vector for all elements in the keyVec using a bloom filter.
  static std::string genBloomFilter(const std::vector<std::string>& keyVec);

  // Checks if the given key is in the bit vector.
  // We can use this method to determin if the machine is in the cluster.
  static bool lookupKey(const std::string& key, const std::string& bitVec);

  // Checks the field of the license to determine if the license is valid or not.
  // This check includes product type check and license version check.
  Status checkFields();

 private:
  // Dynamic object representing the license info
  folly::dynamic content_ = "";
  // String representing the license content
  std::string rawContent_ = "";
  // String representing the license key
  std::string key_ = "";
  // The license file path
  std::string licensePath_ = "";
};

}  // namespace encryption
}  // namespace nebula
