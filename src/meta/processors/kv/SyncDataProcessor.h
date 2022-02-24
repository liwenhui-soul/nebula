/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_SYNCDATAPROCESSOR_H_
#define META_SYNCDATAPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Sync meta data. Receive meta data from drainer and write to meta leader.
 *
 */
class SyncDataProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static SyncDataProcessor* instance(kvstore::KVStore* kvstore) {
    return new SyncDataProcessor(kvstore);
  }

  void process(const cpp2::SyncDataReq& req);

 private:
  explicit SyncDataProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SYNCDATAPROCESSOR_H_
