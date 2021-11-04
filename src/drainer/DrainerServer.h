/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef DRAINER_DRAINERSERVER_H_
#define DRAINER_DRAINERSERVER_H_

#include <thrift/lib/cpp2/server/ThriftServer.h>

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/meta/IndexManager.h"
#include "common/meta/SchemaManager.h"
#include "drainer/CommonUtils.h"

namespace nebula {

class WebService;

namespace drainer {

class DrainerServer final {
 public:
  DrainerServer(HostAddr localHost, std::vector<HostAddr> metaAddrs, std::string dataPath);

  ~DrainerServer();

  // Return false if failed.
  bool start();

  void stop();

  void waitUntilStop();

 private:
  enum ServiceStatus { STATUS_UNINITIALIZED = 0, STATUS_RUNNING = 1, STATUS_STOPPED = 2 };

 private:
  bool initWebService();

  HostAddr localHost_;
  std::vector<HostAddr> metaAddrs_;
  // Only one data directory is supported
  std::string dataPath_;

  std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
  std::shared_ptr<apache::thrift::concurrency::ThreadManager> workers_;
  std::unique_ptr<meta::MetaClient> metaClient_;
  std::unique_ptr<meta::SchemaManager> schemaMan_;
  std::unique_ptr<nebula::WebService> webSvc_;
  std::unique_ptr<drainer::DrainerEnv> env_;
  std::unique_ptr<std::thread> drainerThread_;
  std::atomic<ServiceStatus> drainerSvcStatus_{STATUS_UNINITIALIZED};
  std::unique_ptr<apache::thrift::ThriftServer> drainerServer_;
};

}  // namespace drainer
}  // namespace nebula

#endif  // DRAINER_DRAINERSERVER_H_
