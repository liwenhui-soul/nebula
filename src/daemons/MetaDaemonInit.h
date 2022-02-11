/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef DAEMONS_METADAEMONINIT_H
#define DAEMONS_METADAEMONINIT_H

#include <memory>

#include "common/base/Status.h"
#include "common/hdfs/HdfsCommandHelper.h"
#include "kvstore/KVStore.h"
#include "webservice/WebService.h"

nebula::ClusterID& metaClusterId();

/**
 * @brief The common interface for initializing meta kv, including normal replica kv and listener
 * replica kv
 *
 * @param peers
 * @param syncListenerHost
 * @param localhost
 * @return std::unique_ptr<nebula::kvstore::KVStore>
 */
std::unique_ptr<nebula::kvstore::KVStore> initKV(std::vector<nebula::HostAddr> peers,
                                                 nebula::HostAddr syncListenerHost,
                                                 nebula::HostAddr localhost);

/**
 * @brief Meta normal replica, precondition is syncListenerHost != localhost
 *
 * @param ioPool
 * @param threadManager
 * @param syncListenerHost
 * @param localhost
 * @param kvOptions
 * @return std::unique_ptr<nebula::kvstore::KVStore>
 */
std::unique_ptr<nebula::kvstore::KVStore> initMetaKV(
    std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
    std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager,
    nebula::HostAddr& syncListenerHost,
    nebula::HostAddr& localhost,
    nebula::kvstore::KVOptions& kvOptions);

/**
 * @brief  Meta listener replica, precondition is syncListenerHost != nebula::HostAddr("", 0) &&
 * syncListenerHost == localhost
 *
 * @param ioPool
 * @param threadManager
 * @param peers
 * @param syncListenerHost
 * @param localhost
 * @param kvOptions
 * @return std::unique_ptr<nebula::kvstore::KVStore>
 */
std::unique_ptr<nebula::kvstore::KVStore> initMetaListenerKV(
    std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
    std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager,
    std::vector<nebula::HostAddr>& peers,
    nebula::HostAddr& syncListenerHost,
    nebula::HostAddr& localhost,
    nebula::kvstore::KVOptions& kvOptions);

nebula::Status initWebService(nebula::WebService* svc,
                              nebula::kvstore::KVStore* kvstore,
                              nebula::hdfs::HdfsCommandHelper* helper,
                              nebula::thread::GenericThreadPool* pool);
#endif
