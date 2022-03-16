/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/json.h>
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/encryption/License.h"
#include "common/fs/TempDir.h"
#include "common/http/HttpClient.h"
#include "version/Version.h"
#include "webservice/WebService.h"

namespace nebula {

using nebula::fs::TempDir;

class LicenseHandlerTestEnv : public ::testing::Environment {
 public:
  void SetUp() override {
    FLAGS_ws_http_port = 0;
    VLOG(1) << "Starting web service...";

    webSvc_ = std::make_unique<WebService>();
    auto status = webSvc_->start();
    getDummyLicense();
    ASSERT_TRUE(status.ok()) << status;
  }

  void getDummyLicense() {
    auto licenseIns = encryption::License::getInstance();
    folly::dynamic dummyContent = folly::dynamic::object();
    dummyContent["vendor"] = "vesoft";
    dummyContent["graphdSpec"] = folly::dynamic::object("nodes", 100);
    dummyContent["storagedSpec"] = folly::dynamic::object("nodes", 100);
    dummyContent["storagedSpec"] = folly::dynamic::object("nodes", 100);
    dummyContent["expirationDate"] = "2021-11-30T15:59:59.000Z";
    licenseIns->setContent(dummyContent);
  }

  void TearDown() override {
    webSvc_.reset();
    VLOG(1) << "Web service stopped";
  }

 protected:
  std::unique_ptr<WebService> webSvc_;
};

TEST(LicenseHandlerTest, SimpleTest) {
  {
    auto url = "/license";
    auto request =
        folly::stringPrintf("http://%s:%d%s", FLAGS_ws_ip.c_str(), FLAGS_ws_http_port, url);
    auto resp = http::HttpClient::get(request);
    ASSERT_TRUE(resp.ok());
    auto json = folly::parseJson(resp.value());
    LOG(INFO) << folly::toPrettyJson(json);

    ASSERT_EQ("vesoft", json["vendor"]);
    ASSERT_EQ("{\"nodes\":100}", folly::toJson(json["graphdSpec"]));
    ASSERT_EQ(100, json["storagedSpec"]["nodes"]);
    ASSERT_EQ("2021-11-30T15:59:59.000Z", json["expirationDate"].asString());
  }
}

}  // namespace nebula

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  ::testing::AddGlobalTestEnvironment(new nebula::LicenseHandlerTestEnv());
  return RUN_ALL_TESTS();
}
