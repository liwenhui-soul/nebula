/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_CACHE_STORAGECACHE_H
#define STORAGE_CACHE_STORAGECACHE_H

#include "common/base/Base.h"
#include "common/base/CacheLibLRU.h"

namespace nebula {
namespace kvstore {

static const char kVertexPoolName[] = "VertextPool";
static const char kStorageCacheName[] = "__StorageCache__";

struct VertexPoolInfo {
  std::string poolName_;
  uint32_t capacity_;
  VertexPoolInfo(std::string poolName, uint32_t capacity)
      : poolName_(poolName), capacity_(capacity) {}
};

class StorageCache {
 public:
  StorageCache();

  ~StorageCache() {
    LOG(INFO) << "Destroy storage cache";
  }

  bool init();

  /**
   * @brief create a vertex pool
   *
   * @param poolName
   * @return
   */
  bool createVertexPool(std::string poolName = kVertexPoolName);

  /**
   * @brief create a edge pool
   *
   * @param key
   * @param value
   * @return
   */
  bool createEdgePool(std::string& key, std::string* value);

  /**
   * @brief get the property of a vertex
   *
   * @param key
   * @param value: the property of a vertex
   * @return
   */
  bool getVertexProp(const std::string& key, std::string* value);

  /**
   * @brief insert or update vertex property in cache
   *
   * @param key
   * @param value
   * @return
   */
  bool putVertexProp(const std::string& key, std::string& value);

  /**
   * @brief evict a vertex in cache
   *
   * @param key
   */
  void invalidateVertex(const std::string& key);

  /**
   * @brief Data may be written in batch. To avoid frequently aquiring and releasing locks, we will
   * remove keys in batches in this case.
   * @param keys: keys to remove from cache
   */
  void invalidateVertices(const std::vector<std::string>& keys);

  /**
   * @brief get the size of the vertex pool
   *
   * @return uint32_t
   */
  uint32_t getVertexPoolSize();

  /**
   * @brief check whether vertex pool exists
   *
   * @return bool
   */
  bool vertexPoolExists() {
    return vertexPool_ != nullptr;
  }

  /**
   * @brief add a key to vector to invalidate in cache later
   *
   * @param spaceId:
   * @param rawKey: key like tag, vertix, edge, etc...
   * @param vertixKeys: a vector storing the cacheKey to invalidate
   */
  void addCacheItemsToDelete(GraphSpaceID spaceId,
                             const folly::StringPiece& rawKey,
                             std::vector<std::string>& vertexKeys);

 private:
  uint32_t capacity_ = 0;  // in MB
  std::unique_ptr<CacheLibLRU> cacheInternal_{nullptr};
  std::shared_ptr<VertexPoolInfo> vertexPool_{nullptr};
};

}  // namespace kvstore
}  // namespace nebula

#endif  // STORAGE_CACHE_STORAGECACHE_H
