/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_PARTMANAGER_H_
#define KVSTORE_PARTMANAGER_H_

#include <gtest/gtest_prod.h>

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/meta/Common.h"
#include "kvstore/DiskManager.h"

namespace nebula {
namespace kvstore {

class Handler {
 public:
  virtual ~Handler() = default;

  virtual void addSpace(GraphSpaceID spaceId, bool isListener = false) = 0;

  virtual void addPart(GraphSpaceID spaceId,
                       PartitionID partId,
                       bool asLearner,
                       const std::vector<HostAddr>& peers) = 0;

  virtual void updateSpaceOption(GraphSpaceID spaceId,
                                 const std::unordered_map<std::string, std::string>& options,
                                 bool isDbOption) = 0;

  virtual void removeSpace(GraphSpaceID spaceId, bool isListener = false) = 0;

  virtual void removePart(GraphSpaceID spaceId, PartitionID partId) = 0;

  virtual void addListener(GraphSpaceID spaceId,
                           PartitionID partId,
                           meta::cpp2::ListenerType type,
                           const std::vector<HostAddr>& peers) = 0;

  virtual void removeListener(GraphSpaceID spaceId,
                              PartitionID partId,
                              meta::cpp2::ListenerType type) = 0;

  virtual void checkRemoteListeners(GraphSpaceID spaceId,
                                    PartitionID partId,
                                    const std::vector<HostAddr>& remoteListeners) = 0;

  // get infos from handler(nebula store) to listener(meta_client -> meta)
  virtual int32_t allLeader(
      std::unordered_map<GraphSpaceID, std::vector<meta::cpp2::LeaderInfo>>& leaderIds) = 0;

  virtual void fetchDiskParts(SpaceDiskPartsMap& diskParts) = 0;
};

/**
 * This class manages all meta information one storage host needed.
 * */
class PartManager {
 public:
  PartManager() = default;

  virtual ~PartManager() = default;

  /**
   * return meta::PartsMap for host
   * */
  virtual meta::PartsMap parts(const HostAddr& host) = 0;

  /**
   * return meta::PartHosts for <spaceId, partId>
   * */
  virtual StatusOr<meta::PartHosts> partMeta(GraphSpaceID spaceId, PartitionID partId) = 0;

  /**
   * Check current part exist or not on host.
   * */
  virtual Status partExist(const HostAddr& host, GraphSpaceID spaceId, PartitionID partId) = 0;

  /**
   * Check current space exist or not.
   * */
  virtual Status spaceExist(const HostAddr& host, GraphSpaceID spaceId) = 0;

  virtual meta::ListenersMap listeners(const HostAddr& host) = 0;

  virtual StatusOr<std::vector<meta::RemoteListenerInfo>> listenerPeerExist(GraphSpaceID spaceId,
                                                                            PartitionID partId) = 0;

  /**
   * Register Handler
   * */
  void registerHandler(Handler* handler) {
    handler_ = handler;
  }

 protected:
  Handler* handler_ = nullptr;
};

/**
: * Memory based PartManager, it is used in UTs now.
 * */
class MemPartManager final : public PartManager {
  FRIEND_TEST(NebulaStoreTest, SimpleTest);
  FRIEND_TEST(NebulaStoreTest, PartsTest);
  FRIEND_TEST(NebulaStoreTest, ThreeCopiesTest);
  FRIEND_TEST(NebulaStoreTest, TransLeaderTest);
  FRIEND_TEST(NebulaStoreTest, CheckpointTest);
  FRIEND_TEST(NebulaStoreTest, ThreeCopiesCheckpointTest);
  FRIEND_TEST(NebulaStoreTest, AtomicOpBatchTest);
  FRIEND_TEST(NebulaStoreTest, RemoveInvalidSpaceTest);
  FRIEND_TEST(NebulaStoreTest, BackupRestoreTest);
  friend class ListenerBasicTest;

 public:
  MemPartManager() = default;

  ~MemPartManager() = default;

  meta::PartsMap parts(const HostAddr& host) override;

  StatusOr<meta::PartHosts> partMeta(GraphSpaceID spaceId, PartitionID partId) override;

  void addPart(GraphSpaceID spaceId, PartitionID partId, std::vector<HostAddr> peers = {}) {
    bool noSpace = partsMap_.find(spaceId) == partsMap_.end();
    auto& p = partsMap_[spaceId];
    bool noPart = p.find(partId) == p.end();
    p[partId] = meta::PartHosts();
    auto& pm = p[partId];
    pm.spaceId_ = spaceId;
    pm.partId_ = partId;
    pm.hosts_ = std::move(peers);
    auto isNull = handler_ == nullptr ? true : false;
    LOG(INFO) << "handler_ is null " << isNull;
    if (noSpace && handler_) {
      LOG(INFO) << "add space  spaceId " << spaceId;
      handler_->addSpace(spaceId);
    }
    if (noPart && handler_) {
      LOG(INFO) << "add part  spaceId " << spaceId << " part " << partId;
      handler_->addPart(spaceId, partId, false, {});
    }
  }

  // meta listener replica used
  Status addListener(GraphSpaceID spaceId,
                     PartitionID partId,
                     meta::cpp2::ListenerType type,
                     HostAddr listener,
                     const std::vector<HostAddr>& peers) {
    // The one listener host only has one listener type
    // But there can be multiple meta listener hosts
    auto iterHost = listenersMap_.find(listener);
    if (iterHost != listenersMap_.end()) {
      return Status::Error("Listener host conflict!");
    }

    // because FLAGS_meta_sync_listener is one host. so sync listener only has one.
    listenersMap_[listener][spaceId][partId].emplace_back(type, peers);
    return Status::OK();
  }

  // Normal meta replica use
  Status addRemoteListener(GraphSpaceID spaceId,
                           PartitionID partId,
                           const meta::cpp2::ListenerType& type,
                           const HostAddr& listener) {
    auto& p = remoteListeners_[spaceId];
    auto iter = p.find(partId);
    if (iter != p.end()) {
      auto remoteListenerInfo = iter->second;
      for (auto& rListener : remoteListenerInfo) {
        // A listener host can only have one listener type
        if (rListener.first == listener) {
          return Status::Error("Listener Host conflict!");
        }
      }
    }
    p[partId].emplace_back(listener, type);
    LOG(INFO) << "Meta listener size is  " << p[partId].size();
    return Status::OK();
  }

  void removePart(GraphSpaceID spaceId, PartitionID partId) {
    auto it = partsMap_.find(spaceId);
    CHECK(it != partsMap_.end());
    if (it->second.find(partId) != it->second.end()) {
      it->second.erase(partId);
      if (handler_) {
        handler_->removePart(spaceId, partId);
        if (it->second.empty()) {
          handler_->removeSpace(spaceId);
        }
      }
    }
  }

  // TODO
  // void removeListener(GraphSpaceID spaceId, PartitionID partId) {}

  Status partExist(const HostAddr& host, GraphSpaceID spaceId, PartitionID partId) override;

  Status spaceExist(const HostAddr&, GraphSpaceID spaceId) override {
    if (partsMap_.find(spaceId) != partsMap_.end()) {
      return Status::OK();
    } else {
      return Status::SpaceNotFound();
    }
  }

  meta::PartsMap& partsMap() {
    return partsMap_;
  }

  // Get all the listener information used by a host
  meta::ListenersMap listeners(const HostAddr& host) override;

  StatusOr<std::vector<meta::RemoteListenerInfo>> listenerPeerExist(GraphSpaceID spaceId,
                                                                    PartitionID partId) override;

 private:
  meta::PartsMap partsMap_;

  // For multiple types of listeners, used for meta Listener replica.
  std::unordered_map<HostAddr, meta::ListenersMap> listenersMap_;

  // Normal meta replica used.
  meta::RemoteListeners remoteListeners_;
};

class MetaServerBasedPartManager : public PartManager, public meta::MetaChangedListener {
 public:
  explicit MetaServerBasedPartManager(HostAddr host, meta::MetaClient* client = nullptr);

  ~MetaServerBasedPartManager();

  meta::PartsMap parts(const HostAddr& host) override;

  StatusOr<meta::PartHosts> partMeta(GraphSpaceID spaceId, PartitionID partId) override;

  Status partExist(const HostAddr& host, GraphSpaceID spaceId, PartitionID partId) override;

  Status spaceExist(const HostAddr& host, GraphSpaceID spaceId) override;

  meta::ListenersMap listeners(const HostAddr& host) override;

  StatusOr<std::vector<meta::RemoteListenerInfo>> listenerPeerExist(GraphSpaceID spaceId,
                                                                    PartitionID partId) override;

  /**
   * Implement the interfaces in MetaChangedListener
   * */
  void onSpaceAdded(GraphSpaceID spaceId, bool isListener) override;

  void onSpaceRemoved(GraphSpaceID spaceId, bool isListener) override;

  void onSpaceOptionUpdated(GraphSpaceID spaceId,
                            const std::unordered_map<std::string, std::string>& options) override;

  void onPartAdded(const meta::PartHosts& partMeta) override;

  void onPartRemoved(GraphSpaceID spaceId, PartitionID partId) override;

  void onPartUpdated(const meta::PartHosts& partMeta) override;

  void fetchLeaderInfo(
      std::unordered_map<GraphSpaceID, std::vector<meta::cpp2::LeaderInfo>>& leaderParts) override;

  void fetchDiskParts(SpaceDiskPartsMap& diskParts) override;

  void onListenerAdded(GraphSpaceID spaceId,
                       PartitionID partId,
                       const meta::ListenerHosts& listenerHosts) override;

  void onListenerRemoved(GraphSpaceID spaceId,
                         PartitionID partId,
                         meta::cpp2::ListenerType type) override;

  void onCheckRemoteListeners(GraphSpaceID spaceId,
                              PartitionID partId,
                              const std::vector<HostAddr>& remoteListeners) override;

 private:
  meta::MetaClient* client_{nullptr};
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PARTMANAGER_H_
