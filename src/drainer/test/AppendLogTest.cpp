/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "drainer/processor/AppendLogProcessor.h"
#include "drainer/test/TestUtils.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/drainer_types.h"
#include "kvstore/LogEncoder.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"

namespace nebula {
namespace drainer {

TEST(AppendLogTest, SimpleTest) {
  fs::TempDir rootPath("/tmp/AppendLogTest.XXXXXX");
  mock::MockCluster cluster;
  std::string path = rootPath.path();
  cluster.initStorageKV(path.c_str());
  auto* storageEnv = cluster.storageEnv_.get();

  auto drainerEnv = std::make_unique<nebula::drainer::DrainerEnv>();
  auto datapath = path + "/data/drainer";
  drainerEnv->drainerPath_ = datapath;
  drainerEnv->schemaMan_ = storageEnv->schemaMan_;
  drainerEnv->indexMan_ = storageEnv->indexMan_;
  drainerEnv->metaClient_ = storageEnv->metaClient_;

  GraphSpaceID space = 1;
  // The space information(vidLen, vidType) of the master cluster must be consistent
  // with the information of the slave cluster space.
  // Here the information(vidLen, vidType) of the slave cluster space is used as
  // the information of the master cluster space
  auto toSpaceNameRet = drainerEnv->schemaMan_->toGraphSpaceName(space);
  ASSERT_TRUE(toSpaceNameRet.ok());
  auto toSpaceName = toSpaceNameRet.value();

  auto toSpaceVidLenRet = drainerEnv->schemaMan_->getSpaceVidLen(space);
  ASSERT_TRUE(toSpaceVidLenRet.ok());
  auto toSpaceVidLen = toSpaceVidLenRet.value();

  auto toSpaceVidTypeRet = drainerEnv->schemaMan_->getSpaceVidType(space);
  ASSERT_TRUE(toSpaceVidTypeRet.ok());
  auto toSpaceVidType = toSpaceVidTypeRet.value();

  auto partNumRet = drainerEnv->schemaMan_->getPartsNum(space);
  ASSERT_TRUE(partNumRet.ok());
  auto partNum = partNumRet.value();

  auto* processor = AppendLogProcessor::instance(drainerEnv.get(), nullptr);

  // only handle partId  1
  PartitionID partId = 1;
  LOG(INFO) << "Build AppendLogRequest...";
  auto reqRet = mockAppendLogReq(
      drainerEnv->schemaMan_, space, toSpaceName, partNum, partId, toSpaceVidLen, toSpaceVidType);
  ASSERT_TRUE(reqRet.ok());
  auto req = reqRet.value();

  auto lastRecvLogId = *req.last_log_id_to_send_ref();

  LOG(INFO) << "Test AppendLogProcessor...";
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, *resp.error_code_ref());

  LOG(INFO) << "Check recv.log file ...";
  // build recv log file path
  auto recvLogFile =
      folly::stringPrintf("%s/nebula/%d/%d/recv.log", datapath.c_str(), space, partId);
  auto recvLogIdRet = readRecvLogFile(recvLogFile);
  EXPECT_TRUE(recvLogIdRet.ok());
  auto recvLogId = recvLogIdRet.value();
  EXPECT_EQ(lastRecvLogId, recvLogId);
}

}  // namespace drainer
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
