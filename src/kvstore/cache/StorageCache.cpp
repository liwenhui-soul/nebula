/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/cache/StorageCache.h"

#include "common/utils/NebulaKeyUtils.h"

DEFINE_uint32(storage_cache_capacity,
              100,
              "Total capacity reservered for storage in memory cache in MB");

DEFINE_uint32(storage_cache_buckets_power,
              10,
              "Number of buckets in base 2 logarithm. "
              "E.g., in case of 10, the total number of buckets will be 2^10.");

DEFINE_uint32(storage_cache_locks_power,
              5,
              "Number of locks in base 2 logarithm. "
              "E.g., in case of 5, the total number of locks will be 2^5.");

DEFINE_uint32(vertex_pool_capacity, 50, "Vertex pool size in MB");
DEFINE_uint32(vertex_item_ttl, 300, "TTL for vertex item in the cache");

DEFINE_uint32(empty_key_pool_capacity, 50, "Empty key pool size in MB");
DEFINE_uint32(empty_key_item_ttl, 300, "TTL for empty key item in the cache");

DEFINE_bool(enable_storage_cache, false, "Whether to enable storage cache");
DEFINE_bool(enable_vertex_pool, false, "Whether to add vertex pool in cache");
DEFINE_bool(enable_empty_key_pool, false, "Whether to add empty key pool in cache");

namespace nebula {
namespace kvstore {

StorageCache::StorageCache() {
  capacity_ = FLAGS_storage_cache_capacity;
  cacheInternal_ = std::make_unique<CacheLibLRU>(kStorageCacheName,
                                                 capacity_,
                                                 FLAGS_storage_cache_buckets_power,
                                                 FLAGS_storage_cache_locks_power);
}

bool StorageCache::init() {
  LOG(INFO) << "Start storage cache...";
  auto ret = cacheInternal_->initializeCache();
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return false;
  }
  return true;
}

bool StorageCache::createVertexPool(std::string poolName) {
  LOG(INFO) << "Create vertex pool: " << poolName;
  auto ret = cacheInternal_->addPool(poolName, FLAGS_vertex_pool_capacity);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return false;
  }
  vertexPool_ = std::make_unique<VertexPoolInfo>(poolName, FLAGS_vertex_pool_capacity);
  return true;
}

bool StorageCache::createEmptyKeyPool(std::string poolName) {
  LOG(INFO) << "Create empty key pool: " << poolName;
  auto ret = cacheInternal_->addPool(poolName, FLAGS_empty_key_pool_capacity);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return false;
  }
  emptyKeyPool_ = std::make_unique<EmptyKeyPoolInfo>(poolName, FLAGS_empty_key_pool_capacity);
  return true;
}

bool StorageCache::getVertexProp(const std::string& key, std::string* value) {
  auto ret = cacheInternal_->get(key, value);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return false;
  }
  return true;
}

// This function can be called on cache miss or writing data.
// We do not use async mode here via returning future to ensure strong consistency.
// Or maybe we can separate this function into putVertexPropOnMiss and putVertexPropOnWrite,
// and the former one can return a future.
bool StorageCache::putVertexProp(const std::string& key, std::string& value) {
  if (!vertexPool_) {
    LOG(ERROR) << "No vertext pool exists!";
    return false;
  }
  auto ret = cacheInternal_->put(key, value, vertexPool_->poolName_, FLAGS_vertex_item_ttl);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return false;
  }
  return true;
}

bool StorageCache::addEmptyKey(const std::string& key) {
  if (!emptyKeyPool_) {
    LOG(ERROR) << "No empty key pool exists!";
    return false;
  }
  auto ret =
      cacheInternal_->put(key, kEmptyValue, emptyKeyPool_->poolName_, FLAGS_empty_key_item_ttl);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return false;
  }
  return true;
}

void StorageCache::invalidateVertex(const std::string& key) {
  cacheInternal_->invalidateItem(key);
}

void StorageCache::invalidateVertices(const std::vector<std::string>& keys) {
  cacheInternal_->invalidateItems(keys);
}

uint32_t StorageCache::getVertexPoolSize() {
  auto ret = cacheInternal_->getConfiguredPoolSize(vertexPool_->poolName_);
  if (!nebula::ok(ret)) {
    return 0;
  }
  return nebula::value(ret) / 1024 / 1024;
}

void StorageCache::addCacheItemsToDelete(GraphSpaceID spaceId,
                                         const folly::StringPiece& rawKey,
                                         std::vector<std::string>& vertexKeys) {
  if (FLAGS_enable_vertex_pool && NebulaKeyUtils::isTagOrVertex(rawKey)) {
    vertexKeys.emplace_back(NebulaKeyUtils::cacheKey(spaceId, rawKey));
  }
}

}  // namespace kvstore
}  // namespace nebula
