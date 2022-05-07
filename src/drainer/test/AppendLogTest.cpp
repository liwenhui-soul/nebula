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

// Suppose that data is received from wal at the beginning
TEST(AppendLogTest, ReceivingFromWalTest) {
  fs::TempDir rootPath("/tmp/ReceivingFromWalTest.XXXXXX");
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

  // intervalLogFile_ not exists
  auto intervalLogFile =
      folly::stringPrintf("%s/nebula/%d/%d/interval.log", datapath.c_str(), space, partId);
  EXPECT_NE(access(intervalLogFile.c_str(), 0), 0);
}

// Suppose that data is received from snapshot at the beginning
TEST(AppendLogTest, ReceivingFromSnapshotTest) {
  fs::TempDir rootPath("/tmp/ReceivingFromSnapshotTest.XXXXXX");
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

  // snapshot data and snapshot finished
  auto lastRecvLogId = *req.last_log_id_to_send_ref();
  req.need_cleanup_ref() = true;
  req.is_snapshot_ref() = true;
  req.snapshot_finished_ref() = true;
  req.snapshot_commitLogId_ref() = lastRecvLogId;
  req.last_log_id_to_send_ref() = 0;

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

  // intervalLogFile_ exists
  auto intervalLogFile =
      folly::stringPrintf("%s/nebula/%d/%d/interval.log", datapath.c_str(), space, partId);
  auto intervalLogIdRet = readInterverLogFile(intervalLogFile);
  EXPECT_TRUE(intervalLogIdRet.ok());
  auto intervalLogId = intervalLogIdRet.value();
  EXPECT_EQ(0, intervalLogId);
}

// Suppose that data is received from snapshot at the beginning
// then receiving wal data
// But snapshot_commitLogId is equal to the wal id written in drainer.
// logInterval_ is equal to 0.
TEST(AppendLogTest, LogIntervalEqualZeroTest) {
  fs::TempDir rootPath("/tmp/LogIntervalEqualZeroTest.XXXXXX");
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

  // only handle partId  1
  PartitionID partId = 1;
  {
    // receive snapshot data
    auto* processor = AppendLogProcessor::instance(drainerEnv.get(), nullptr);
    LOG(INFO) << "Build AppendLogRequest...";
    auto reqRet = mockAppendLogReq(
        drainerEnv->schemaMan_, space, toSpaceName, partNum, partId, toSpaceVidLen, toSpaceVidType);
    ASSERT_TRUE(reqRet.ok());
    auto req = reqRet.value();

    // snapshot data and snapshot finished
    auto lastRecvLogId = *req.last_log_id_to_send_ref();
    req.need_cleanup_ref() = true;
    req.is_snapshot_ref() = true;
    req.snapshot_finished_ref() = true;
    req.snapshot_commitLogId_ref() = lastRecvLogId;
    req.last_log_id_to_send_ref() = 0;

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

    // intervalLogFile_ exists, but logInterval = 0
    auto intervalLogFile =
        folly::stringPrintf("%s/nebula/%d/%d/interval.log", datapath.c_str(), space, partId);
    auto intervalLogIdRet = readInterverLogFile(intervalLogFile);
    EXPECT_TRUE(intervalLogIdRet.ok());
    auto intervalLogId = intervalLogIdRet.value();
    EXPECT_EQ(0, intervalLogId);
  }
  {
    // receive wal data
    auto* processor = AppendLogProcessor::instance(drainerEnv.get(), nullptr);
    LOG(INFO) << "Build AppendLogRequest...";
    auto reqRet = mockAppendLogReq(
        drainerEnv->schemaMan_, space, toSpaceName, partNum, partId, toSpaceVidLen, toSpaceVidType);
    ASSERT_TRUE(reqRet.ok());
    auto req = reqRet.value();

    auto lastRecvLogId = *req.last_log_id_to_send_ref();
    req.last_log_id_sent_ref() = lastRecvLogId;
    req.last_log_id_to_send_ref() = lastRecvLogId * 2;

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
    EXPECT_EQ(lastRecvLogId * 2, recvLogId);
  }
}

// Suppose that data is received from snapshot at the beginning
// then receiving wal data
// But snapshot_commitLogId is less than the wal id written in drainer.
// logInterval_ is greater than zero.
TEST(AppendLogTest, LogIntervalGreaterZeroTest) {
  fs::TempDir rootPath("/tmp/LogIntervalGreaterZeroTest.XXXXXX");
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

  // only handle partId  1
  PartitionID partId = 1;
  LogID interverExpect = 5;
  {
    // receive snapshot data
    auto* processor = AppendLogProcessor::instance(drainerEnv.get(), nullptr);
    LOG(INFO) << "Build AppendLogRequest...";
    auto reqRet = mockAppendLogReq(
        drainerEnv->schemaMan_, space, toSpaceName, partNum, partId, toSpaceVidLen, toSpaceVidType);
    ASSERT_TRUE(reqRet.ok());
    auto req = reqRet.value();

    // snapshot data and snapshot finished
    auto lastRecvLogId = *req.last_log_id_to_send_ref();
    req.need_cleanup_ref() = true;
    req.is_snapshot_ref() = true;
    req.snapshot_finished_ref() = true;
    req.snapshot_commitLogId_ref() = lastRecvLogId - interverExpect;
    req.last_log_id_to_send_ref() = 0;

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

    // intervalLogFile_ exists, but logInterval = 0
    auto intervalLogFile =
        folly::stringPrintf("%s/nebula/%d/%d/interval.log", datapath.c_str(), space, partId);
    auto intervalLogIdRet = readInterverLogFile(intervalLogFile);
    EXPECT_TRUE(intervalLogIdRet.ok());
    auto intervalLogId = intervalLogIdRet.value();
    EXPECT_EQ(interverExpect, intervalLogId);
  }
  {
    // receive wal data
    auto* processor = AppendLogProcessor::instance(drainerEnv.get(), nullptr);
    LOG(INFO) << "Build AppendLogRequest...";
    auto reqRet = mockAppendLogReq(
        drainerEnv->schemaMan_, space, toSpaceName, partNum, partId, toSpaceVidLen, toSpaceVidType);
    ASSERT_TRUE(reqRet.ok());
    auto req = reqRet.value();

    auto lastRecvLogId = *req.last_log_id_to_send_ref();
    req.last_log_id_sent_ref() = lastRecvLogId - interverExpect;
    req.last_log_id_to_send_ref() = lastRecvLogId * 2 - interverExpect;

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
    EXPECT_EQ(lastRecvLogId * 2, recvLogId);
  }
}

// Suppose that data is received from snapshot at the beginning
// then receiving wal data
// But snapshot_commitLogId is greater than the wal id written in drainer.
// logInterval_ is less than zero.
TEST(AppendLogTest, LogIntervalLessZeroTest) {
  fs::TempDir rootPath("/tmp/LogIntervalLessZeroTest.XXXXXX");
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

  // only handle partId  1
  PartitionID partId = 1;
  LogID interverExpect = -5;
  {
    // receive snapshot data
    auto* processor = AppendLogProcessor::instance(drainerEnv.get(), nullptr);
    LOG(INFO) << "Build AppendLogRequest...";
    auto reqRet = mockAppendLogReq(
        drainerEnv->schemaMan_, space, toSpaceName, partNum, partId, toSpaceVidLen, toSpaceVidType);
    ASSERT_TRUE(reqRet.ok());
    auto req = reqRet.value();

    // snapshot data and snapshot finished
    auto lastRecvLogId = *req.last_log_id_to_send_ref();
    req.need_cleanup_ref() = true;
    req.is_snapshot_ref() = true;
    req.snapshot_finished_ref() = true;
    req.snapshot_commitLogId_ref() = lastRecvLogId + interverExpect;
    req.last_log_id_to_send_ref() = 0;

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

    // intervalLogFile_ exists, but logInterval = 0
    auto intervalLogFile =
        folly::stringPrintf("%s/nebula/%d/%d/interval.log", datapath.c_str(), space, partId);
    auto intervalLogIdRet = readInterverLogFile(intervalLogFile);
    EXPECT_TRUE(intervalLogIdRet.ok());
    auto intervalLogId = intervalLogIdRet.value();
    EXPECT_EQ(-interverExpect, intervalLogId);
  }
  {
    // receive wal data
    auto* processor = AppendLogProcessor::instance(drainerEnv.get(), nullptr);
    LOG(INFO) << "Build AppendLogRequest...";
    auto reqRet = mockAppendLogReq(
        drainerEnv->schemaMan_, space, toSpaceName, partNum, partId, toSpaceVidLen, toSpaceVidType);
    ASSERT_TRUE(reqRet.ok());
    auto req = reqRet.value();

    auto lastRecvLogId = *req.last_log_id_to_send_ref();
    req.last_log_id_sent_ref() = lastRecvLogId + interverExpect;
    req.last_log_id_to_send_ref() = lastRecvLogId * 2 + interverExpect;

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
    EXPECT_EQ(lastRecvLogId * 2, recvLogId);
  }
}

}  // namespace drainer
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
