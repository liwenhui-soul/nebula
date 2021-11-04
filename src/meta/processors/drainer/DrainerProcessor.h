/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DRAINERROCESSOR_H_
#define META_DRAINERROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class AddDrainerProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static AddDrainerProcessor* instance(kvstore::KVStore* kvstore) {
    return new AddDrainerProcessor(kvstore);
  }

  void process(const cpp2::AddDrainerReq& req);

 private:
  explicit AddDrainerProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class RemoveDrainerProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static RemoveDrainerProcessor* instance(kvstore::KVStore* kvstore) {
    return new RemoveDrainerProcessor(kvstore);
  }

  void process(const cpp2::RemoveDrainerReq& req);

 private:
  explicit RemoveDrainerProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class ListDrainersProcessor : public BaseProcessor<cpp2::ListDrainersResp> {
 public:
  static ListDrainersProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListDrainersProcessor(kvstore);
  }

  void process(const cpp2::ListDrainersReq& req);

 private:
  explicit ListDrainersProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListDrainersResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DRAINERROCESSOR_H_
