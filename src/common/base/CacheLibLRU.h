/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_BASE_CACHELIBLRU_H_
#define COMMON_BASE_CACHELIBLRU_H_

#include <cachelib/allocator/CacheAllocator.h>

#include "common/base/Base.h"
#include "common/base/ErrorOr.h"
#include "interface/gen-cpp2/common_types.h"

namespace nebula {

using Cache = facebook::cachelib::LruAllocator;

class CacheLibLRU {
 public:
  explicit CacheLibLRU(std::string name, uint32_t capacity, uint32_t cacheEntriesPower)
      : name_(name), capacity_(capacity), cacheEntriesPower_(cacheEntriesPower) {}

  /**
   * @brief Create cache instance. If there is any exception, we will allow the process continue.
   *
   * @param poolName: the pool to allocate cache space
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode initializeCache() {
    Cache::Config config;
    if (cacheEntriesPower_ > kMaxCacheEntriesPower) {
      LOG(WARNING) << "Estimated number of cache entries exceeds the cache limit. Nebula will trim "
                      "the the maximum allowed: "
                   << kMaxCacheEntriesPower;
      cacheEntriesPower_ = kMaxCacheEntriesPower;
    }
    try {
      config
          // size cannot exceed the maximum cache size (274'877'906'944 bytes)
          .setCacheSize(capacity_ * 1024 * 1024)
          .setCacheName(name_)
          .setAccessConfig(std::pow(2, cacheEntriesPower_))
          .validate();  // will throw if bad config
    } catch (const std::exception& e) {
      // We do not stop the service. Users should refer to the log to determine whether to restart
      // the service.
      LOG(ERROR) << "Cache configuration error: " << e.what();
      return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }
    nebulaCache_ = std::make_unique<Cache>(config);

    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  /**
   * @brief add cache pool into cache instance
   *
   * @param poolName
   * @param poolSize
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode addPool(std::string poolName, uint32_t poolSize) {
    if (poolIdMap_.find(poolName) != poolIdMap_.end()) {
      LOG(ERROR) << "Cache pool creation error. Cache pool exists: " << poolName.data();
      return nebula::cpp2::ErrorCode::E_EXISTED;
    }
    try {
      auto poolId = nebulaCache_->addPool(poolName, poolSize * 1024 * 1024);
      poolIdMap_[poolName] = poolId;
    } catch (const std::exception& e) {
      LOG(ERROR) << "Adding cache pool error: " << e.what();
      return nebula::cpp2::ErrorCode::E_NOT_ENOUGH_SPACE;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  /**
   * @brief Get key from cache. Return true if found.
   *
   * @param key
   * @return Error (cache miss) or value
   */
  nebula::cpp2::ErrorCode get(const std::string& key, std::string* value) {
    folly::SharedMutex::ReadHolder rHolder(lock_);
    auto itemHandle = nebulaCache_->find(key);
    if (itemHandle) {
      value->assign(reinterpret_cast<const char*>(itemHandle->getMemory()), itemHandle->getSize());
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    VLOG(3) << "Cache miss: " << key << " Not Found";
    return nebula::cpp2::ErrorCode::E_CACHE_MISS;
  }

  /**
   * @brief Insert or update value in cache pool
   *
   * @param key
   * @param value
   * @param poolName: The pool name to insert/update cache item
   * @param ttl
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode put(const std::string& key,
                              const std::string& value,
                              std::string poolName,
                              uint32_t ttl = 300) {
    if (poolIdMap_.find(poolName) == poolIdMap_.end()) {
      LOG(ERROR) << "Cache write error. Pool does not exist: " << poolName.data();
      return nebula::cpp2::ErrorCode::E_POOL_NOT_FOUND;
    }
    auto itemHandle = nebulaCache_->allocate(poolIdMap_[poolName], key, value.size(), ttl);
    if (!itemHandle) {
      LOG(ERROR) << "Cache write error. Too many pending writes.";
      return nebula::cpp2::ErrorCode::E_CACHE_WRITE_FAILURE;
    }

    {
      folly::SharedMutex::WriteHolder wHolder(lock_);
      std::memcpy(itemHandle->getMemory(), value.data(), value.size());
      nebulaCache_->insertOrReplace(itemHandle);
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  /**
   * @brief CacheLib will first search for the key. If found, remove it.
   * Note here we do not log anything if not found, as it can have a good chance that an item is not
   * in the cache.
   *
   * @param key
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode invalidateItem(const std::string& key) {
    folly::SharedMutex::WriteHolder wHolder(lock_);
    VLOG(3) << "Invalidate vertex key: " << folly::hexlify(key);
    nebulaCache_->remove(key);
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  /**
   * @brief CacheLib remove multiple items
   *
   * @param keys
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode invalidateItems(const std::vector<std::string>& keys) {
    folly::SharedMutex::WriteHolder wHolder(lock_);
    for (auto key : keys) {
      VLOG(3) << "Invalidate vertex key: " << folly::hexlify(key);
      nebulaCache_->remove(key);
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  /**
   * @brief Get the configured size of the pool
   *
   * @param poolName
   * @return Error (pool not existing) or unit64_t
   */
  ErrorOr<nebula::cpp2::ErrorCode, uint64_t> getConfiguredPoolSize(const std::string& poolName) {
    if (poolIdMap_.find(poolName) == poolIdMap_.end()) {
      LOG(ERROR) << "Get cache pool size error. Pool does not exist: " << poolName.data();
      return nebula::cpp2::ErrorCode::E_POOL_NOT_FOUND;
    }
    return nebulaCache_->getPoolStats(poolIdMap_[poolName]).poolSize;
  }

  /**
   * @brief Get the count of cache hit of a pool
   *
   * @param poolName
   * @return Error (pool not existing) or unit64_t
   */
  ErrorOr<nebula::cpp2::ErrorCode, uint64_t> getPoolCacheHitCount(const std::string& poolName) {
    if (poolIdMap_.find(poolName) == poolIdMap_.end()) {
      LOG(ERROR) << "Get cache hit count error. Pool does not exist: " << poolName.data();
      return nebula::cpp2::ErrorCode::E_POOL_NOT_FOUND;
    }
    return nebulaCache_->getPoolStats(poolIdMap_[poolName]).numPoolGetHits;
  }

 private:
  static constexpr uint32_t kMaxCacheEntriesPower = 31;  // the cap by cachelib
  std::unique_ptr<Cache> nebulaCache_ = nullptr;
  std::unordered_map<std::string, facebook::cachelib::PoolId> poolIdMap_;
  std::string name_;
  uint32_t capacity_ = 0;            // in MB
  uint32_t cacheEntriesPower_ = 20;  // estimated number of cache entries in base 2 logarithm

  // CacheLib does not protect data at item level. We need to synchronize the access.
  folly::SharedMutex lock_;
};

}  // namespace nebula

#endif  // COMMON_BASE_CACHELIBLRU_H_
