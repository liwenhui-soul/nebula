/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Random.h>
#include <gtest/gtest.h>
#include <net/if.h>
#include <proxygen/lib/utils/Base64.h>
#include <proxygen/lib/utils/CryptUtil.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "common/base/Base.h"
#include "common/base/Logging.h"
#include "common/encryption/License.h"
#include "common/network/NetworkUtils.h"

namespace nebula {
namespace encryption {
// using encryption::License;
TEST(LicenseGeneratorTool, BloomFilterTest) {
  folly::Random::secureRand32(12, 42);
  std::vector<std::string> keyVec{"adasdadasdadasd",
                                  "fsafljfiogerklio9)nf",
                                  "akdodsgjiowoo87*&7hb3eb fsdf 3",
                                  "wqqq",
                                  "nebula::encryption::License::genBloomFil(keyVec);",
                                  "ne231bula::encryption::License::genBloomter(keyVec);",
                                  "ne231bu2314la::encryption::License::genBmFilter(keyVec);",
                                  "ne231bu2314la::encryption::License::genBmFilter;",
                                  "qwefjgpwejp"};

  LOG(INFO) << "Building bit vector...";
  auto bitVec = License::genBloomFilter(keyVec);

  LOG(INFO) << "bitVec binary: " << bitVec << " size: " << bitVec.size();

  auto base64 = proxygen::base64Encode(folly::StringPiece((bitVec)));
  LOG(INFO) << "bitVec base64: " << base64 << " length: " << base64.size();

  LOG(INFO) << "Looking for existing keys...";
  for (auto &ele : keyVec) {
    EXPECT_TRUE(License::lookupKey(ele, bitVec));
  }

  LOG(INFO) << "Looking for non-existing keys...";
  int falsePositiveCount = 0;
  for (auto i = 0; i < 100; i++) {
    if (License::lookupKey(std::string(i, 'b'), bitVec)) {
      ++falsePositiveCount;
    }
  }
  double falsePositiveRate = falsePositiveCount / 100.0;
  LOG(INFO) << "False positive rate: " << falsePositiveRate;
  EXPECT_LT(falsePositiveRate, 0.15);
}

TEST(LicenseGeneratorTool, HardwareCheckTest) {
  // Dummy machine codes
  std::string machineCode1 = "d6QqwqCHJK0j0LtIxHpP2leSNiCaXu3ucVP6rJKFT2A=%";
  std::string machineCode2 = "d6QqwqCHJK0j0LtIwqeiugfuweSNiCaXu3ucVP6rJKF213A=%";

  // A dummy cluster code that is generated using multiple machine codes
  const std::string bitVec = License::genBloomFilter({machineCode1, machineCode2});
  auto clusterCode = proxygen::base64Encode(folly::StringPiece((bitVec)));
  EXPECT_EQ(Status::OK(), License::checkMachineCode(machineCode1, clusterCode));
  EXPECT_EQ(Status::OK(), License::checkMachineCode(machineCode2, clusterCode));

  // Unregistered machine code
  std::string machineCode3 = "d6QqwqCHJK0j0LasdugfuweSNiCaXu3rdfy6rJKF213A=%";
  EXPECT_EQ(Status::Error(), License::checkMachineCode("machineCode", clusterCode));
}
}  // namespace encryption
}  // namespace nebula

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
