/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "drainer/DrainerServiceHandler.h"

#include "drainer/processor/AppendLogProcessor.h"

#define RETURN_FUTURE(processor)   \
  auto f = processor->getFuture(); \
  processor->process(req);         \
  return f;

namespace nebula {
namespace drainer {

DrainerServiceHandler::DrainerServiceHandler(DrainerEnv* env) : env_(env) {
  // Initialize all counters
  kAppendLogCounters.init("appendLogs");
}

// Vertice section
folly::Future<drainer::cpp2::AppendLogResponse> DrainerServiceHandler::future_appendLog(
    const cpp2::AppendLogRequest& req) {
  auto* processor = AppendLogProcessor::instance(env_, &kAppendLogCounters);
  RETURN_FUTURE(processor);
}

}  // namespace drainer
}  // namespace nebula
