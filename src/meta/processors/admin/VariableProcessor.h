/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_VARIABLEPROCESSOR_H
#define META_VARIABLEPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class GetVariableProcessor : public BaseProcessor<cpp2::GetVariableResp> {
 public:
  static GetVariableProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetVariableProcessor(kvstore);
  }

  void process(const cpp2::GetVariableReq& req);

 private:
  explicit GetVariableProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetVariableResp>(kvstore) {}
};

class SetVariableProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static SetVariableProcessor* instance(kvstore::KVStore* kvstore) {
    return new SetVariableProcessor(kvstore);
  }

  void process(const cpp2::SetVariableReq& req);

  ErrorOr<nebula::cpp2::ErrorCode, std::string> checkVariableNameAndValue(const std::string& name,
                                                                          Value val);

 private:
  explicit SetVariableProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class ListVariablesProcessor : public BaseProcessor<cpp2::ListVariablesResp> {
 public:
  static ListVariablesProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListVariablesProcessor(kvstore);
  }

  void process(const cpp2::ListVariablesReq& req);

 private:
  explicit ListVariablesProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListVariablesResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_VARIABLEPROCESSOR_H
