/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTENERPROCESSOR_H_
#define META_LISTENERPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Add typed listeners for given space, assign one listener for each part round robin.
 *        Notice that in each space, each type of listeners could be only added once now.
 *        But, of course, you could remove them and re-add new ones.
 *        It will use heartbeat to instruct the relative storaged to add listeners physically.
 *
 */
class AddListenerProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static AddListenerProcessor* instance(kvstore::KVStore* kvstore) {
    return new AddListenerProcessor(kvstore);
  }

  void process(const cpp2::AddListenerReq& req);

 private:
  explicit AddListenerProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

/**
 * @brief Remove added listener information. Return failed if not found.
 *
 */
class RemoveListenerProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static RemoveListenerProcessor* instance(kvstore::KVStore* kvstore) {
    return new RemoveListenerProcessor(kvstore);
  }

  void process(const cpp2::RemoveListenerReq& req);

 private:
  explicit RemoveListenerProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

/**
 * @brief List all listeners for given spaces and listener type.
 *        And will fill the listener ative state.
 *
 */
class ListListenersProcessor : public BaseProcessor<cpp2::ListListenersResp> {
 public:
  static ListListenersProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListListenersProcessor(kvstore);
  }

  void process(const cpp2::ListListenersReq& req);

 private:
  explicit ListListenersProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListListenersResp>(kvstore) {}
};

/**
 * @brief Allocate the drainer client of the space to the sync listener in the way of round robin.
 * For the master cluster, the survival status of the drainer cluster is unknown.
 *
 */
class ListListenerDrainersProcessor : public BaseProcessor<cpp2::ListListenerDrainersResp> {
 public:
  static ListListenerDrainersProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListListenerDrainersProcessor(kvstore);
  }

  void process(const cpp2::ListListenerDrainersReq& req);

 private:
  explicit ListListenerDrainersProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListListenerDrainersResp>(kvstore) {}
};

/**
 * @brief The sync listener of specified space stops sending data to the drainer.
 *
 */
class StopSyncProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static StopSyncProcessor* instance(kvstore::KVStore* kvstore) {
    return new StopSyncProcessor(kvstore);
  }

  void process(const cpp2::StopSyncReq& req);

 private:
  explicit StopSyncProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

/**
 * @brief The sync listener of specified space restarts sending data to the drainer.
 *
 */
class RestartSyncProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static RestartSyncProcessor* instance(kvstore::KVStore* kvstore) {
    return new RestartSyncProcessor(kvstore);
  }

  void process(const cpp2::RestartSyncReq& req);

 private:
  explicit RestartSyncProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // META_LISTENERPROCESSOR_H_
