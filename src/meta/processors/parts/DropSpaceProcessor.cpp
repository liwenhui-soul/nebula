/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/parts/DropSpaceProcessor.h"

#include "kvstore/LogEncoder.h"

namespace nebula {
namespace meta {

void DropSpaceProcessor::process(const cpp2::DropSpaceReq& req) {
  folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  const auto& spaceName = req.get_space_name();
  auto spaceRet = getSpaceId(spaceName);

  if (!nebula::ok(spaceRet)) {
    auto retCode = nebula::error(spaceRet);
    if (retCode == nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND) {
      if (req.get_if_exists()) {
        retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
      } else {
        LOG(INFO) << "Drop space Failed, space " << spaceName << " not existed.";
      }
    } else {
      LOG(INFO) << "Drop space Failed, space " << spaceName
                << " error: " << apache::thrift::util::enumNameSafe(retCode);
    }
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto spaceId = nebula::value(spaceRet);
  auto batchHolder = std::make_unique<kvstore::BatchHolder>();

  // 1. Delete related part meta data.
  auto prefix = MetaKeyUtils::partPrefix(spaceId);
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "Drop space Failed, space " << spaceName
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    auto key = iter->key();
    batchHolder->remove(key.str());
    iter->next();
  }

  // 2. Delete this space data
  batchHolder->remove(MetaKeyUtils::indexSpaceKey(spaceName));
  batchHolder->remove(MetaKeyUtils::spaceKey(spaceId));

  // 3. Delete related role data.
  auto rolePrefix = MetaKeyUtils::roleSpacePrefix(spaceId);
  auto roleRet = doPrefix(rolePrefix);
  if (!nebula::ok(roleRet)) {
    auto retCode = nebula::error(roleRet);
    LOG(INFO) << "Drop space Failed, space " << spaceName
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto roleIter = nebula::value(roleRet).get();
  while (roleIter->valid()) {
    VLOG(3) << "Revoke role " << MetaKeyUtils::parseRoleStr(roleIter->val()) << " for user "
            << MetaKeyUtils::parseRoleUser(roleIter->key());
    auto key = roleIter->key();
    batchHolder->remove(key.str());
    roleIter->next();
  }

  // 4. Delete listener meta data
  auto lstPrefix = MetaKeyUtils::listenerPrefix(spaceId);
  auto lstRet = doPrefix(rolePrefix);
  if (!nebula::ok(lstRet)) {
    auto retCode = nebula::error(lstRet);
    LOG(INFO) << "Drop space Failed, space " << spaceName
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto lstIter = nebula::value(lstRet).get();
  while (lstIter->valid()) {
    auto key = lstIter->key();
    batchHolder->remove(key.str());
    lstIter->next();
  }

  // 5. Delete listener drainer meta data
  auto lstDrainerPrefix = MetaKeyUtils::listenerDrainerPrefix(spaceId);
  auto lstDrainerRet = doPrefix(lstDrainerPrefix);
  if (!nebula::ok(lstDrainerRet)) {
    auto retCode = nebula::error(lstDrainerRet);
    LOG(ERROR) << "Drop space Failed, space " << spaceName
               << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto lstDrainerIter = nebula::value(lstDrainerRet).get();
  while (lstDrainerIter->valid()) {
    auto key = lstDrainerIter->key();
    batchHolder->remove(key.str());
    lstDrainerIter->next();
  }

  // 6.Delete related drainer meta data
  auto drainerKey = MetaKeyUtils::drainerKey(spaceId);
  batchHolder->remove(std::move(drainerKey));

  // 7. Delete related statis data
  auto statskey = MetaKeyUtils::statsKey(spaceId);
  batchHolder->remove(std::move(statskey));

  // 8. Delete related fulltext index meta data
  auto ftPrefix = MetaKeyUtils::fulltextIndexPrefix();
  auto ftRet = doPrefix(ftPrefix);
  if (!nebula::ok(ftRet)) {
    auto retCode = nebula::error(ftRet);
    LOG(INFO) << "Drop space Failed, space " << spaceName
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto ftIter = nebula::value(ftRet).get();
  while (ftIter->valid()) {
    auto index = MetaKeyUtils::parsefulltextIndex(ftIter->val());
    if (index.get_space_id() == spaceId) {
      auto key = ftIter->key();
      batchHolder->remove(key.str());
    }
    ftIter->next();
  }

  // 9. Delete local_id meta data
  auto localIdkey = MetaKeyUtils::localIdKey(spaceId);
  batchHolder->remove(std::move(localIdkey));

  // 10. Delete space leval external service data
  auto spaceServicePre = MetaKeyUtils::spaceServicePrefix(spaceId);
  auto spaceServiceRet = doPrefix(spaceServicePre);
  if (!nebula::ok(spaceServiceRet)) {
    auto retCode = nebula::error(spaceServiceRet);
    LOG(ERROR) << "Drop space Failed, space " << spaceName
               << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto spaceServiceIter = nebula::value(spaceServiceRet).get();
  while (spaceServiceIter->valid()) {
    auto key = spaceServiceIter->key();
    batchHolder->remove(key.str());
    spaceServiceIter->next();
  }

  // 11. Delete space leval variable data
  auto variablePre = MetaKeyUtils::variablePrefix(spaceId);
  auto variableRet = doPrefix(variablePre);
  if (!nebula::ok(variableRet)) {
    auto retCode = nebula::error(variableRet);
    LOG(ERROR) << "Drop space Failed, space " << spaceName
               << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto variableIter = nebula::value(variableRet).get();
  while (variableIter->valid()) {
    auto key = ftIter->key();
    batchHolder->remove(key.str());
    variableIter->next();
  }

  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(batchHolder.get(), timeInMilliSec);
  auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
  doBatchOperation(std::move(batch));
  LOG(INFO) << "Drop space " << spaceName << ", id " << spaceId;
}

}  // namespace meta
}  // namespace nebula
