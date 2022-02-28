/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/VariableProcessor.h"

#include <folly/String.h>

#include "common/utils/MetaKeyUtils.h"

namespace nebula {
namespace meta {

void GetVariableProcessor::process(const cpp2::GetVariableReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);

  auto varName = req.get_item().get_name();
  auto ret = MetaKeyUtils::getVariableTypeAndDefaultValue(varName);
  if (ret.first.empty()) {
    LOG(ERROR) << "Variable " << varName << " is invalid in space " << space;
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_VARIABLE);
    onFinished();
    return;
  }

  auto type = ret.first;
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());

  cpp2::VariableItem item;
  std::string value;

  const auto& variableKey = MetaKeyUtils::variableKey(space, varName);
  auto result = doGet(variableKey);
  if (!nebula::ok(result)) {
    auto errCode = nebula::error(result);
    if (errCode != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      LOG(ERROR) << "Get variable " << varName
                 << " failed, error: " << apache::thrift::util::enumNameSafe(errCode);
      handleErrorCode(errCode);
      onFinished();
      return;
    } else {
      // return default value
      value = ret.second;
    }
  } else {
    value = nebula::value(result);
  }

  auto val = variableValToValue(type, value);
  if (val.type() == Value::Type::__EMPTY__ || val.type() == Value::Type::NULLVALUE) {
    LOG(ERROR) << "Get variable " << varName << " value " << value << " mismatch";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_VARIABLE);
    onFinished();
    return;
  }
  item.name_ref() = varName;
  item.value_ref() = val;
  resp_.item_ref() = std::move(item);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

ErrorOr<nebula::cpp2::ErrorCode, std::string> SetVariableProcessor::checkVariableNameAndValue(
    const std::string& name, Value val) {
  auto ret = MetaKeyUtils::getVariableTypeAndDefaultValue(name);
  if (ret.first.empty()) {
    LOG(ERROR) << "Variable " << name << " is invalid";
    return nebula::cpp2::ErrorCode::E_INVALID_VARIABLE;
  }
  auto type = ret.first;
  if (type == "bool") {
    auto result = val.toBool();
    if (result == Value::kNullValue || result == Value::kNullBadType) {
      return nebula::cpp2::ErrorCode::E_VARIABLE_TYPE_VALUE_MISMATCH;
    }
    return result.toString();
  }
  return nebula::cpp2::ErrorCode::E_INVALID_VARIABLE;
}

void SetVariableProcessor::process(const cpp2::SetVariableReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);

  auto name = req.get_item().get_name();
  auto value = req.get_item().get_value();

  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto ret = checkVariableNameAndValue(name, value);

  if (!nebula::ok(ret)) {
    LOG(ERROR) << "Variable " << name << " value " << value.toString() << " is invalid in space "
               << space;
    handleErrorCode(nebula::error(ret));
    onFinished();
    return;
  }

  auto valStr = nebula::value(ret);
  std::vector<kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::variableKey(space, name), valStr);
  doSyncPutAndUpdate(std::move(data));
}

void ListVariablesProcessor::process(const cpp2::ListVariablesReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);

  folly::SharedMutex::ReadHolder holder(LockUtils::lock());

  auto allVariableInfo = MetaKeyUtils::getAllVariableInfo();
  std::unordered_map<std::string, Value> data;
  for (auto& elem : allVariableInfo) {
    auto varName = elem.first;
    std::string value;
    const auto& variableKey = MetaKeyUtils::variableKey(space, varName);
    auto result = doGet(variableKey);
    if (!nebula::ok(result)) {
      auto errCode = nebula::error(result);
      if (errCode != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
        LOG(ERROR) << "Get variable " << varName
                   << " failed, error: " << apache::thrift::util::enumNameSafe(errCode);
        handleErrorCode(errCode);
        onFinished();
        return;
      } else {
        // get default Value
        value = elem.second.second;
      }
    } else {
      value = nebula::value(result);
    }
    auto val = variableValToValue(elem.second.first, value);
    if (val.type() == Value::Type::__EMPTY__ || val.type() == Value::Type::NULLVALUE) {
      LOG(ERROR) << "Get variable " << varName << " value " << value << " mismatch";
      handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_VARIABLE);
      onFinished();
      return;
    }
    data.emplace(varName, val);
  }

  resp_.variables_ref() = std::move(data);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
