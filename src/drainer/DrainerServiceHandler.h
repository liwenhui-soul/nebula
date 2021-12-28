/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef DRAINER_DRAINERSERVICEHANDLER_H_
#define DRAINER_DRAINERSERVICEHANDLER_H_

#include "common/base/Base.h"
#include "drainer/CommonUtils.h"
#include "drainer/DrainerFlags.h"
#include "interface/gen-cpp2/DrainerService.h"

namespace nebula {
namespace drainer {

class DrainerEnv;

class DrainerServiceHandler final : public cpp2::DrainerServiceSvIf {
 public:
  explicit DrainerServiceHandler(DrainerEnv* env);

  folly::Future<drainer::cpp2::AppendLogResponse> future_appendLog(
      const drainer::cpp2::AppendLogRequest& req) override;

 private:
  DrainerEnv* env_{nullptr};
};

}  // namespace drainer
}  // namespace nebula

#endif  // DRAINER_DRAINERSERVICEHANDLER_H_
