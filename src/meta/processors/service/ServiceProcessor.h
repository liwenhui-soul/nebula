/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_SERVICEPROCESSOR_H_
#define META_SERVICEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class SignInServiceProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static SignInServiceProcessor* instance(kvstore::KVStore* kvstore) {
    return new SignInServiceProcessor(kvstore);
  }

  void process(const cpp2::SignInServiceReq& req);

 private:
  explicit SignInServiceProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class SignOutServiceProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static SignOutServiceProcessor* instance(kvstore::KVStore* kvstore) {
    return new SignOutServiceProcessor(kvstore);
  }

  void process(const cpp2::SignOutServiceReq& req);

 private:
  explicit SignOutServiceProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class ListServiceClientsProcessor : public BaseProcessor<cpp2::ListServiceClientsResp> {
 public:
  static ListServiceClientsProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListServiceClientsProcessor(kvstore);
  }

  void process(const cpp2::ListServiceClientsReq& req);

 private:
  explicit ListServiceClientsProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListServiceClientsResp>(kvstore) {}
};

class SignInSpaceServiceProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static SignInSpaceServiceProcessor* instance(kvstore::KVStore* kvstore) {
    return new SignInSpaceServiceProcessor(kvstore);
  }

  void process(const cpp2::SignInSpaceServiceReq& req);

 private:
  explicit SignInSpaceServiceProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class SignOutSpaceServiceProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static SignOutSpaceServiceProcessor* instance(kvstore::KVStore* kvstore) {
    return new SignOutSpaceServiceProcessor(kvstore);
  }

  void process(const cpp2::SignOutSpaceServiceReq& req);

 private:
  explicit SignOutSpaceServiceProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class ListSpaceServiceClientsProcessor : public BaseProcessor<cpp2::ListSpaceServiceClientsResp> {
 public:
  static ListSpaceServiceClientsProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListSpaceServiceClientsProcessor(kvstore);
  }

  void process(const cpp2::ListSpaceServiceClientsReq& req);

 private:
  explicit ListSpaceServiceClientsProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListSpaceServiceClientsResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SERVICEPROCESSOR_H_
