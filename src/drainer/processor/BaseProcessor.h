/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef DRAINER_BASEPROCESSOR_H_
#define DRAINER_BASEPROCESSOR_H_

#include <folly/SpinLock.h>
#include <folly/futures/Future.h>
#include <folly/futures/Promise.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/base/Base.h"
#include "common/stats/StatsManager.h"
#include "common/time/Duration.h"
#include "drainer/CommonUtils.h"

namespace nebula {
namespace drainer {

template <typename REQ, typename RESP>
class BaseProcessor {
 public:
  explicit BaseProcessor(DrainerEnv* env, const ProcessorCounters* counters = nullptr)
      : env_(env), counters_(counters) {}

  virtual ~BaseProcessor() = default;

  virtual void process(const REQ& req) = 0;

  folly::Future<RESP> getFuture() { return promise_.getFuture(); }

 protected:
  virtual void onFinished() {
    if (counters_) {
      stats::StatsManager::addValue(counters_->numCalls_);
      if (this->resp_.get_error_code() != nebula::cpp2::ErrorCode::SUCCEEDED) {
        stats::StatsManager::addValue(counters_->numErrors_);
      }
    }

    this->promise_.setValue(std::move(this->resp_));

    if (counters_) {
      stats::StatsManager::addValue(counters_->latency_, this->duration_.elapsedInUSec());
    }

    delete this;
  }

  void pushResultCode(nebula::cpp2::ErrorCode code) { resp_.set_error_code(std::move(code)); }

  virtual nebula::cpp2::ErrorCode checkAndBuildContexts(const REQ& req) = 0;

  virtual void onProcessFinished() = 0;

 protected:
  DrainerEnv* env_{nullptr};
  const ProcessorCounters* counters_;

  RESP resp_;
  folly::Promise<RESP> promise_;

  time::Duration duration_;
};

}  // namespace drainer
}  // namespace nebula

#endif  // DRAINER_BASEPROCESSOR_H_
