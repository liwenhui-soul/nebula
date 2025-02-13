/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LISTEDGEINDEXSTATUSPROCESSOR_H
#define META_LISTEDGEINDEXSTATUSPROCESSOR_H

#include "meta/processors/BaseProcessor.h"
#include "meta/processors/job/JobDescription.h"
#include "meta/processors/job/JobUtils.h"

namespace nebula {
namespace meta {

class ListEdgeIndexStatusProcessor : public BaseProcessor<cpp2::ListIndexStatusResp> {
 public:
  static ListEdgeIndexStatusProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListEdgeIndexStatusProcessor(kvstore);
  }

  void process(const cpp2::ListIndexStatusReq& req);

 private:
  explicit ListEdgeIndexStatusProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListIndexStatusResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTEDGEINDEXSTATUSPROCESSOR_H
