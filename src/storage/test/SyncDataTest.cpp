/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "clients/storage/InternalStorageClient.h"
#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/network/NetworkUtils.h"
#include "kvstore/LogEncoder.h"
#include "meta/test/TestUtils.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/kv/SyncDataProcessor.h"
#include "storage/test/TestUtils.h"

namespace nebula {
namespace storage {

bool encodeValue(const meta::NebulaSchemaProvider* schema,
                 const std::vector<Value>& props,
                 std::string& val) {
  RowWriterV2 writer(schema);
  for (size_t i = 0; i < props.size(); i++) {
    auto r = writer.setValue(i, props[i]);
    if (r != WriteResult::SUCCEEDED) {
      LOG(ERROR) << "Invalid prop " << i;
      return false;
    }
  }
  auto ret = writer.finish();
  if (ret != WriteResult::SUCCEEDED) {
    LOG(ERROR) << "Failed to write data";
    return false;
  }
  val = std::move(writer).moveEncodedStr();
  return true;
}

StatusOr<cpp2::SyncDataRequest> mockSyncDataReq(meta::SchemaManager* schemaMan,
                                                ClusterID cluster,
                                                GraphSpaceID space,
                                                int32_t partNum,
                                                PartitionID partId,
                                                int32_t vidLen) {
  cpp2::SyncDataRequest req;
  req.cluster_ref() = cluster;
  req.space_id_ref() = space;

  auto verticesPart = mock::MockData::mockVerticesofPart(partNum);
  std::vector<nebula::mock::VertexData> data = verticesPart[partId];

  LOG(INFO) << "Build SyncDataRequest...";
  LOG(INFO) << "Vertex data size " << data.size();

  auto sourceDataSize = data.size();
  int count = 0;

  for (auto& vertex : data) {
    std::string log;
    TagID tagId = vertex.tId_;
    std::string value;
    auto key = NebulaKeyUtils::tagKey(vidLen, partId, vertex.vId_, tagId);
    auto schema = schemaMan->getTagSchema(space, tagId);
    if (!schema) {
      LOG(ERROR) << "Invalid tagId " << tagId;
      return Status::Error("Invalid tagId %d", tagId);
    }
    auto encodeRet = encodeValue(schema.get(), vertex.props_, value);
    if (!encodeRet) {
      LOG(ERROR) << "Encode failed, tagid %d" << tagId;
      return Status::Error("Encode failed, tagid %d", tagId);
    }
    log = encodeMultiValues(kvstore::OP_PUT, key, value);
    (*req.parts_ref())[partId].emplace_back(std::move(log));
    count++;
  }

  EXPECT_EQ(sourceDataSize, count);
  return req;
}

void checkAddVerticesData(StorageEnv* env, GraphSpaceID space, PartitionID partId, int expectNum) {
  auto ret = env->schemaMan_->getSpaceVidLen(space);
  EXPECT_TRUE(ret.ok());

  int totalCount = 0;
  auto prefix = NebulaKeyUtils::tagPrefix(partId);
  std::unique_ptr<kvstore::KVIterator> iter;
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            env->kvstore_->prefix(space, partId, prefix, &iter));

  while (iter && iter->valid()) {
    totalCount++;
    iter->next();
  }
  EXPECT_EQ(expectNum, totalCount);
}

TEST(SyncDataTest, SimpleTest) {
  fs::TempDir rootPath("/tmp/SyncDataTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  GraphSpaceID space = 1;
  auto partNum = cluster.getTotalParts();

  PartitionID partId = 1;
  auto status = env->schemaMan_->getSpaceVidLen(space);
  ASSERT_TRUE(status.ok());
  auto spaceVidLen = status.value();

  LOG(INFO) << "Build SyncDataRequest...";
  auto reqRet =
      mockSyncDataReq(env->schemaMan_, FLAGS_cluster_id, space, partNum, partId, spaceVidLen);
  ASSERT_TRUE(reqRet.ok());
  auto req = reqRet.value();

  auto partCount = (*req.parts_ref())[partId].size();

  LOG(INFO) << "Test SyncDataProcessor...";
  auto* processor = SyncDataProcessor::instance(env, nullptr);
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  LOG(INFO) << "Check data in kv store...";
  checkAddVerticesData(env, space, partId, partCount);
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
