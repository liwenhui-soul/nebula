/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "drainer/processor/AppendLogProcessor.h"

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"
#include "kvstore/wal/FileBasedWal.h"

DECLARE_int32(wal_ttl);
DECLARE_int64(wal_file_size);
DECLARE_int32(wal_buffer_size);
DECLARE_bool(wal_sync);
DECLARE_int32(cluster_id);

namespace nebula {
namespace drainer {

using nebula::fs::FileType;
using nebula::fs::FileUtils;

ProcessorCounters kAppendLogCounters;

void AppendLogProcessor::process(const cpp2::AppendLogRequest&) {
  onFinished();
  return;
}

nebula::cpp2::ErrorCode AppendLogProcessor::checkAndBuildContexts(const cpp2::AppendLogRequest&) {
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void AppendLogProcessor::onProcessFinished() {}

}  // namespace drainer
}  // namespace nebula
