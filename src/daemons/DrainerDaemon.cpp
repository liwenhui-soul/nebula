/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <thrift/lib/cpp2/server/ThriftServer.h>

#include "common/base/Base.h"
#include "common/base/SignalHandler.h"
#include "common/network/NetworkUtils.h"
#include "common/process/ProcessUtils.h"
#include "common/time/TimezoneInfo.h"
#include "drainer/DrainerServer.h"
#include "version/Version.h"

DEFINE_string(local_ip, "", "IP address which is used to identify this server");
DEFINE_string(data_path,
              "",
              "Root data path for drainer, only one path is allowed."
              "the data is wal file.");

DEFINE_bool(daemonize, true, "Whether to run the process as a daemon");
DEFINE_string(pid_file, "pids/nebula-drainerd.pid", "File to hold the process id");
DEFINE_string(meta_server_addrs,
              "",
              "list of meta server addresses,"
              "the format looks like ip1:port1, ip2:port2, ip3:port3");
DECLARE_int32(port);

using nebula::operator<<;
using nebula::HostAddr;
using nebula::ProcessUtils;
using nebula::Status;
using nebula::network::NetworkUtils;

static void signalHandler(int sig);
static Status setupSignalHandler();
extern Status setupLogging();
#if defined(__x86_64__)
extern Status setupBreakpad();
#endif

std::unique_ptr<nebula::drainer::DrainerServer> gDrainerServer;

int main(int argc, char *argv[]) {
  google::SetVersionString(nebula::versionString());
  // Detect if the server has already been started
  // Check pid before glog init, in case of user may start daemon twice
  // the 2nd will make the 1st failed to output log anymore
  gflags::ParseCommandLineFlags(&argc, &argv, false);

  // Setup logging
  auto status = setupLogging();
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

#if defined(__x86_64__)
  status = setupBreakpad();
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }
#endif

  auto pidPath = FLAGS_pid_file;
  status = ProcessUtils::isPidAvailable(pidPath);
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

  folly::init(&argc, &argv, true);
  if (FLAGS_daemonize) {
    google::SetStderrLogging(google::FATAL);
  } else {
    google::SetStderrLogging(google::INFO);
  }

  if (FLAGS_daemonize) {
    status = ProcessUtils::daemonize(pidPath);
    if (!status.ok()) {
      LOG(ERROR) << status;
      return EXIT_FAILURE;
    }
  } else {
    // Write the current pid into the pid file
    status = ProcessUtils::makePidFile(pidPath);
    if (!status.ok()) {
      LOG(ERROR) << status;
      return EXIT_FAILURE;
    }
  }

  if (FLAGS_data_path.empty()) {
    LOG(ERROR) << "Drainer Data Path should not empty";
    return EXIT_FAILURE;
  }

  std::string hostName;
  if (FLAGS_local_ip.empty()) {
    hostName = nebula::network::NetworkUtils::getHostname();
  } else {
    status = NetworkUtils::validateHostOrIp(FLAGS_local_ip);
    if (!status.ok()) {
      LOG(ERROR) << status;
      return EXIT_FAILURE;
    }
    hostName = FLAGS_local_ip;
  }

  HostAddr localhost(hostName, FLAGS_port);
  LOG(INFO) << "localhost = " << localhost;

  auto metaAddrsRet = nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
  if (!metaAddrsRet.ok() || metaAddrsRet.value().empty()) {
    LOG(ERROR) << "Can't get metaServer address, status:" << metaAddrsRet.status()
               << ", FLAGS_meta_server_addrs:" << FLAGS_meta_server_addrs;
    return EXIT_FAILURE;
  }

  // For drainer, for simplicity, only one data directory is supported
  std::vector<std::string> paths;
  folly::split(",", FLAGS_data_path, paths, true);
  std::transform(paths.begin(), paths.end(), paths.begin(), [](auto &p) {
    return folly::trimWhitespace(p).str();
  });
  if (paths.size() != 1) {
    LOG(ERROR) << "Bad data_path format:" << FLAGS_data_path;
    return EXIT_FAILURE;
  }

  // Setup the signal handlers
  status = setupSignalHandler();
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

  // Initialize the global timezone, it's only used for datetime type compute
  // won't affect the process timezone.
  status = nebula::time::Timezone::initializeGlobalTimezone();
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }

  gDrainerServer =
      std::make_unique<nebula::drainer::DrainerServer>(localhost, metaAddrsRet.value(), paths[0]);
  if (!gDrainerServer->start()) {
    LOG(ERROR) << "Drainer server start failed";
    gDrainerServer->stop();
    return EXIT_FAILURE;
  }

  gDrainerServer->waitUntilStop();
  LOG(INFO) << "The drainer Daemon stopped";
  return EXIT_SUCCESS;
}

Status setupSignalHandler() {
  return nebula::SignalHandler::install(
      {SIGINT, SIGTERM},
      [](nebula::SignalHandler::GeneralSignalInfo *info) { signalHandler(info->sig()); });
}

void signalHandler(int sig) {
  switch (sig) {
    case SIGINT:
    case SIGTERM:
      FLOG_INFO("Signal %d(%s) received, stopping this server", sig, ::strsignal(sig));
      if (gDrainerServer) {
        gDrainerServer->stop();
      }
      break;
    default:
      FLOG_ERROR("Signal %d(%s) received but ignored", sig, ::strsignal(sig));
  }
}
