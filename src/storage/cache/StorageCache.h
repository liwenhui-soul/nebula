/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_CACHE_STORAGECACHE_H
#define STORAGE_CACHE_STORAGECACHE_H

#include "common/base/Base.h"
#include "common/base/CacheLibLRU.h"

namespace nebula {
namespace storage {

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
  void invalidateVertex(std::string& key);

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

 private:
  uint32_t capacity_ = 0;  // in MB
  std::unique_ptr<CacheLibLRU> cacheInternal_{nullptr};
  std::shared_ptr<VertexPoolInfo> vertexPool_{nullptr};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_CACHE_STORAGECACHE_H
