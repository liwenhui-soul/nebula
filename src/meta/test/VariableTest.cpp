/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/processors/admin/VariableProcessor.h"
#include "meta/processors/parts/CreateSpaceProcessor.h"
#include "meta/processors/zone/AddHostsProcessor.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

TEST(VariableTest, VariableTest) {
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
    // Register storage machine
    std::vector<HostAddr> addresses;
    for (int32_t i = 0; i < 3; i++) {
      addresses.emplace_back(std::to_string(i), i);
    }
    TestUtils::registerHB(kv.get(), addresses, cpp2::HostRole::STORAGE);
  }
  {
    // get variable, space not exist, failed
    cpp2::GetVariableReq req;
    req.space_id_ref() = 1;
    cpp2::VariableItem item;
    item.name_ref() = "read_only";
    req.item_ref() = std::move(item);

    auto* processor = GetVariableProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    // set variable, space not exist, failed
    cpp2::SetVariableReq req;
    req.space_id_ref() = 1;
    cpp2::VariableItem item;
    item.name_ref() = "read_only";
    item.value_ref() = Value(true);
    req.item_ref() = std::move(item);

    auto* processor = SetVariableProcessor::instance(kv.get());
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
    // get variable default value
    cpp2::GetVariableReq req;
    req.space_id_ref() = 1;
    cpp2::VariableItem item;
    item.name_ref() = "read_only";
    req.item_ref() = std::move(item);

    auto* processor = GetVariableProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ("read_only", resp.get_item().get_name());
    ASSERT_EQ("false", resp.get_item().get_value().toString());
  }
  {
    // set variable, success
    cpp2::SetVariableReq req;
    req.space_id_ref() = 1;
    cpp2::VariableItem item;
    item.name_ref() = "read_only";
    item.value_ref() = Value(true);
    req.item_ref() = std::move(item);

    auto* processor = SetVariableProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    // get variable value
    cpp2::GetVariableReq req;
    req.space_id_ref() = 1;
    cpp2::VariableItem item;
    item.name_ref() = "read_only";
    req.item_ref() = std::move(item);

    auto* processor = GetVariableProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ("read_only", resp.get_item().get_name());
    ASSERT_EQ("true", resp.get_item().get_value().toString());
  }
  {
    // list variables
    cpp2::ListVariablesReq req;
    req.space_id_ref() = 1;

    auto* processor = ListVariablesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto variables = resp.get_variables();
    ASSERT_EQ("true", variables["read_only"].toString());
  }
  {
    // get not exist variable
    cpp2::GetVariableReq req;
    req.space_id_ref() = 1;
    cpp2::VariableItem item;
    item.name_ref() = "read";
    req.item_ref() = std::move(item);

    auto* processor = GetVariableProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
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
