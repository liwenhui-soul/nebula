/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "kvstore/plugins/sync/SyncListener.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/fs/FileUtils.h"
#include "common/time/WallClock.h"
#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace kvstore {

void SyncListener::init() {}

bool SyncListener::apply(const std::vector<KV>&) { return true; }

std::pair<LogID, TermID> SyncListener::lastCommittedLogId() { return {0, 0}; }

LogID SyncListener::lastApplyLogId() { return 0; }

}  // namespace kvstore
}  // namespace nebula
