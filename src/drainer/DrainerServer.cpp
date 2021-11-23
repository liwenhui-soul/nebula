/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "drainer/DrainerServer.h"

#include <thrift/lib/cpp/concurrency/ThreadManager.h>

#include "common/meta/ServerBasedIndexManager.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "common/network/NetworkUtils.h"
#include "common/thread/GenericThreadPool.h"
#include "common/utils/Utils.h"
#include "drainer/DrainerServiceHandler.h"
#include "kvstore/PartManager.h"
#include "version/Version.h"
#include "webservice/WebService.h"

DEFINE_int32(port, 46500, "Drainer daemon listening port");
DEFINE_int32(num_io_threads, 16, "Number of IO threads");
DEFINE_int32(num_worker_threads, 32, "Number of workers");
DEFINE_int32(drainer_http_thread_num, 3, "Number of drainer daemon's http thread");
DEFINE_bool(local_config, false, "meta client will not retrieve latest configuration from meta");

namespace nebula {
namespace drainer {

DrainerServer::DrainerServer(HostAddr localHost,
                             std::vector<HostAddr> metaAddrs,
                             std::string dataPath)
    : localHost_(localHost), metaAddrs_(std::move(metaAddrs)), dataPath_(std::move(dataPath)) {}

DrainerServer::~DrainerServer() { stop(); }

bool DrainerServer::initWebService() {
  LOG(INFO) << "Starting Drainer HTTP Service";
  webSvc_ = std::make_unique<WebService>();
  auto status = webSvc_->start();
  return status.ok();
}

bool DrainerServer::start() {
  ioThreadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_num_io_threads);
  workers_ = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(
      FLAGS_num_worker_threads, true /*stats*/);
  workers_->setNamePrefix("executor");
  workers_->start();

  // Meta client
  meta::MetaClientOptions options;
  options.localHost_ = localHost_;
  options.serviceName_ = "";
  options.skipConfig_ = FLAGS_local_config;
  options.role_ = nebula::meta::cpp2::HostRole::DRAINER;
  options.gitInfoSHA_ = gitInfoSha();

  metaClient_ = std::make_unique<meta::MetaClient>(ioThreadPool_, metaAddrs_, options);
  if (!metaClient_->waitForMetadReady()) {
    LOG(ERROR) << "waitForMetadReady error!";
    return false;
  }

  LOG(INFO) << "Init schema manager";
  schemaMan_ = meta::ServerBasedSchemaManager::create(metaClient_.get());

  LOG(INFO) << "Init index manager";
  indexMan_ = meta::ServerBasedIndexManager::create(metaClient_.get());
  if (!initWebService()) {
    LOG(ERROR) << "Init webservice failed!";
    return false;
  }

  env_ = std::make_unique<drainer::DrainerEnv>();
  env_->schemaMan_ = schemaMan_.get();
  env_->indexMan_ = indexMan_.get();
  env_->metaClient_ = metaClient_.get();
  env_->drainerPath_ = dataPath_;

  taskMgr_ = DrainerTaskManager::instance();
  if (!taskMgr_->init(env_.get(), ioThreadPool_)) {
    LOG(ERROR) << "Init drainer task manager failed!";
    return false;
  }

  drainerThread_.reset(new std::thread([this] {
    try {
      auto handler = std::make_shared<DrainerServiceHandler>(env_.get());
      drainerServer_ = std::make_unique<apache::thrift::ThriftServer>();
      drainerServer_->setPort(FLAGS_port);
      drainerServer_->setIdleTimeout(std::chrono::seconds(0));
      drainerServer_->setIOThreadPool(ioThreadPool_);
      drainerServer_->setThreadManager(workers_);
      drainerServer_->setStopWorkersOnStopListening(false);
      drainerServer_->setInterface(std::move(handler));

      ServiceStatus expected = STATUS_UNINITIALIZED;
      if (!drainerSvcStatus_.compare_exchange_strong(expected, STATUS_RUNNING)) {
        LOG(ERROR) << "Impossible! How could it happen!";
        return;
      }
      LOG(INFO) << "The drainer service start on " << localHost_;
      drainerServer_->serve();  // Will wait until the server shuts down
    } catch (const std::exception& e) {
      LOG(ERROR) << "Start drainer service failed, error:" << e.what();
    }
    drainerSvcStatus_.store(STATUS_STOPPED);
    LOG(INFO) << "The drainer service stopped";
  }));

  while (drainerSvcStatus_.load() == STATUS_UNINITIALIZED) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  if (drainerSvcStatus_.load() != STATUS_RUNNING) {
    return false;
  }
  return true;
}

void DrainerServer::waitUntilStop() { drainerThread_->join(); }

void DrainerServer::stop() {
  if (drainerSvcStatus_.load() == ServiceStatus::STATUS_STOPPED) {
    LOG(INFO) << "All services has been stopped";
    return;
  }

  ServiceStatus drainerExpected = ServiceStatus::STATUS_RUNNING;
  drainerSvcStatus_.compare_exchange_strong(drainerExpected, STATUS_STOPPED);

  webSvc_.reset();

  if (taskMgr_) {
    taskMgr_->shutdown();
  }

  if (metaClient_) {
    metaClient_->stop();
  }

  if (drainerServer_) {
    drainerServer_->stop();
  }

  for (auto& fd : env_->recvLogIdFd_) {
    close(fd.second);
  }
  for (auto& fd : env_->sendLogIdFd_) {
    close(fd.second);
  }
}

}  // namespace drainer
}  // namespace nebula
