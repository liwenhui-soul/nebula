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
    req.set_hosts(std::move(hosts));
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
    req.set_space_id(1);
    cpp2::VariableItem item;
    item.set_name("read_only");
    req.set_item(std::move(item));

    auto* processor = GetVariableProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    // set variable, space not exist, failed
    cpp2::SetVariableReq req;
    req.set_space_id(1);
    cpp2::VariableItem item;
    item.set_name("read_only");
    item.set_value(Value(true));
    req.set_item(std::move(item));

    auto* processor = SetVariableProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    // Create space, succeeded
    cpp2::SpaceDesc properties;
    properties.set_space_name("first_space");
    properties.set_partition_num(9);
    properties.set_replica_factor(1);
    cpp2::CreateSpaceReq req;
    req.set_properties(std::move(properties));

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
    req.set_space_id(1);
    cpp2::VariableItem item;
    item.set_name("read_only");
    req.set_item(std::move(item));

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
    req.set_space_id(1);
    cpp2::VariableItem item;
    item.set_name("read_only");
    item.set_value(Value(true));
    req.set_item(std::move(item));

    auto* processor = SetVariableProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    // get variable value
    cpp2::GetVariableReq req;
    req.set_space_id(1);
    cpp2::VariableItem item;
    item.set_name("read_only");
    req.set_item(std::move(item));

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
    req.set_space_id(1);

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
    req.set_space_id(1);
    cpp2::VariableItem item;
    item.set_name("read");
    req.set_item(std::move(item));

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
