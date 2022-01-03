/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/processors/drainer/DrainerProcessor.h"
#include "meta/processors/parts/CreateSpaceProcessor.h"
#include "meta/processors/zone/AddHostsProcessor.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

TEST(DrainerTest, DrainerTest) {
  fs::TempDir rootPath("/tmp/DrainerTest.XXXXXX");

  // Prepare
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));

  {
    cpp2::AddHostsReq req;
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    req.hosts_ref() = std::move(hosts);
    auto* processor = AddHostsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    // Register drainer machine
    std::vector<HostAddr> addresses;
    for (int32_t i = 0; i < 3; i++) {
      addresses.emplace_back(std::to_string(i), i);
    }
    TestUtils::registerHB(kv.get(), addresses, cpp2::HostRole::DRAINER);
  }
  {
    // Register storage machine
    std::vector<HostAddr> addresses;
    for (int32_t i = 3; i < 6; i++) {
      addresses.emplace_back(std::to_string(i), i);
    }
    TestUtils::registerHB(kv.get(), addresses, cpp2::HostRole::STORAGE);
  }
  {
    // Add Drainer, space not exist, failed
    cpp2::AddDrainerReq req;
    req.space_id_ref() = 1;

    std::vector<HostAddr> hosts;
    for (int32_t i = 0; i < 3; i++) {
      hosts.emplace_back(std::to_string(i), i);
    }
    req.hosts_ref() = std::move(hosts);
    auto* processor = AddDrainerProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    // Create space, succeeded
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "first_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 1;
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);

    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(1, resp.get_id().get_space_id());
  }
  {
    // Add Drainer, space exists, succeeded
    cpp2::AddDrainerReq req;
    req.space_id_ref() = 1;
    std::vector<HostAddr> hosts;
    for (int32_t i = 0; i < 3; i++) {
      hosts.emplace_back(std::to_string(i), i);
    }
    req.hosts_ref() = std::move(hosts);

    auto* processor = AddDrainerProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    // Add Drainer, drainer exists in space, failed
    cpp2::AddDrainerReq req;
    req.space_id_ref() = 1;
    std::vector<HostAddr> hosts;
    for (int32_t i = 0; i < 3; i++) {
      hosts.emplace_back(std::to_string(i), i);
    }
    req.hosts_ref() = std::move(hosts);

    auto* processor = AddDrainerProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    // List drainer
    cpp2::ListDrainersReq req;
    req.space_id_ref() = 1;
    auto* processor = ListDrainersProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto drainers = resp.get_drainers();
    ASSERT_EQ(3, drainers.size());

    for (auto i = 0; i < 3; i++) {
      auto drainer = drainers[i];
      ASSERT_EQ(HostAddr(std::to_string(i), i), drainer.get_host());
      ASSERT_EQ(cpp2::HostStatus::ONLINE, drainer.get_status());
    }
  }
  {
    // remove drainer
    cpp2::RemoveDrainerReq req;
    req.space_id_ref() = 1;
    auto* processor = RemoveDrainerProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    // List drainer, succeed, drainers is empty
    cpp2::ListDrainersReq req;
    req.space_id_ref() = 1;
    auto* processor = ListDrainersProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto drainers = resp.get_drainers();
    ASSERT_EQ(0, drainers.size());
  }
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
