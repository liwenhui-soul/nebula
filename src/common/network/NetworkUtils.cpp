/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/network/NetworkUtils.h"

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <net/if_arp.h>
#include <netdb.h>
#include <sys/ioctl.h>

#include <cstdio>

#include "common/base/Base.h"
#include "common/base/Logging.h"
#include "common/fs/FileUtils.h"

namespace nebula {
namespace network {

static const int32_t kMaxHostNameLen = 256;

// TODO(liuyu) this only works in ipv4
std::string NetworkUtils::getHostname() {
  char hn[kMaxHostNameLen];

  if (gethostname(hn, kMaxHostNameLen) < 0) {
    LOG(ERROR) << "gethostname error : " << strerror(errno);
    return "";
  }

  return std::string(hn);
}

StatusOr<std::string> NetworkUtils::getIPv4FromDevice(const std::string& device) {
  if (device == "any") {
    return "0.0.0.0";
  }
  auto result = listDeviceAndIPv4s();
  if (!result.ok()) {
    return std::move(result).status();
  }
  auto iter = result.value().find(device);
  if (iter == result.value().end()) {
    return Status::Error("No IPv4 address found for `%s'", device.c_str());
  }
  return iter->second;
}

StatusOr<std::vector<std::string>> NetworkUtils::listIPv4s() {
  auto result = listDeviceAndIPv4s();
  if (!result.ok()) {
    return std::move(result).status();
  }
  auto getval = [](const auto& entry) { return entry.second; };
  std::vector<std::string> ipv4s;
  ipv4s.resize(result.value().size());
  std::transform(result.value().begin(), result.value().end(), ipv4s.begin(), getval);
  return ipv4s;
}

StatusOr<std::unordered_map<std::string, std::string>> NetworkUtils::listDeviceAndIPv4s() {
  struct ifaddrs* iflist;
  std::unordered_map<std::string, std::string> dev2ipv4s;
  if (::getifaddrs(&iflist) != 0) {
    return Status::Error("%s", ::strerror(errno));
  }
  for (auto* ifa = iflist; ifa != nullptr; ifa = ifa->ifa_next) {
    // Skip non-IPv4 devices
    if (ifa->ifa_addr->sa_family != AF_INET) {
      continue;
    }
    auto* addr = reinterpret_cast<struct sockaddr_in*>(ifa->ifa_addr);
    // inet_ntoa is thread safe but not re-entrant,
    // we could use inet_ntop instead when we need support for IPv6
    dev2ipv4s[ifa->ifa_name] = ::inet_ntoa(addr->sin_addr);
  }
  ::freeifaddrs(iflist);
  if (dev2ipv4s.empty()) {
    return Status::Error("No IPv4 devices found");
  }
  return dev2ipv4s;
}

bool NetworkUtils::getDynamicPortRange(uint16_t& low, uint16_t& high) {
  FILE* pipe = popen("cat /proc/sys/net/ipv4/ip_local_port_range", "r");
  if (!pipe) {
    LOG(ERROR) << "Failed to open /proc/sys/net/ipv4/ip_local_port_range: " << strerror(errno);
    return false;
  }

  if (fscanf(pipe, "%hu %hu", &low, &high) != 2) {
    LOG(ERROR) << "Failed to read from /proc/sys/net/ipv4/ip_local_port_range";
    // According to ICANN, the port range is divided into three sections
    //
    // Well-known ports: 0 to 1023 (used for system services)
    // Registered/user ports: 1024 to 49151
    // Dynamic/private ports: 49152 to 65535
    low = 49152;
    high = 65535;
  }

  if (pclose(pipe) < 0) {
    LOG(ERROR) << "Failed to close the pipe: " << strerror(errno);
    return false;
  }

  return true;
}

std::unordered_set<uint16_t> NetworkUtils::getPortsInUse() {
  static const std::regex regex("[^:]+:[^:]+:([0-9A-F]+).+");
  std::unordered_set<uint16_t> inUse;
  {
    fs::FileUtils::FileLineIterator iter("/proc/net/tcp", &regex);
    while (iter.valid()) {
      auto& sm = iter.matched();
      inUse.emplace(std::stoul(sm[1].str(), nullptr, 16));
      ++iter;
    }
  }
  {
    fs::FileUtils::FileLineIterator iter("/proc/net/tcp6", &regex);
    while (iter.valid()) {
      auto& sm = iter.matched();
      inUse.emplace(std::stoul(sm[1].str(), nullptr, 16));
      ++iter;
    }
  }
  {
    fs::FileUtils::FileLineIterator iter("/proc/net/udp", &regex);
    while (iter.valid()) {
      auto& sm = iter.matched();
      inUse.emplace(std::stoul(sm[1].str(), nullptr, 16));
      ++iter;
    }
  }
  {
    fs::FileUtils::FileLineIterator iter("/proc/net/udp6", &regex);
    while (iter.valid()) {
      auto& sm = iter.matched();
      inUse.emplace(std::stoul(sm[1].str(), nullptr, 16));
      ++iter;
    }
  }
  {
    fs::FileUtils::FileLineIterator iter("/proc/net/raw", &regex);
    while (iter.valid()) {
      auto& sm = iter.matched();
      inUse.emplace(std::stoul(sm[1].str(), nullptr, 16));
      ++iter;
    }
  }
  {
    fs::FileUtils::FileLineIterator iter("/proc/net/raw6", &regex);
    while (iter.valid()) {
      auto& sm = iter.matched();
      inUse.emplace(std::stoul(sm[1].str(), nullptr, 16));
      ++iter;
    }
  }

  return inUse;
}

uint16_t NetworkUtils::getAvailablePort() {
  uint16_t low = 0;
  uint16_t high = 0;

  CHECK(getDynamicPortRange(low, high)) << "Failed to get the dynamic port range";
  VLOG(1) << "Dynamic port range is [" << low << ", " << high << "]";

  std::unordered_set<uint16_t> portsInUse = getPortsInUse();
  uint16_t port = 0;
  while (true) {
    // NOTE
    // The availability of port number *outside* the ephemeral port range is
    // relatively stable for the binding purpose.
    port = folly::Random::rand32(1025, low);
    if (portsInUse.find(port) != portsInUse.end()) {
      continue;
    }
    if (portsInUse.find(port + 1) == portsInUse.end()) {
      break;
    }
  }

  return port;
}

StatusOr<std::vector<HostAddr>> NetworkUtils::resolveHost(const std::string& host, int32_t port) {
  std::vector<HostAddr> addrs;
  struct addrinfo hints, *res, *rp;
  ::memset(&hints, 0, sizeof(struct addrinfo));

  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_ADDRCONFIG;

  if (getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0) {
    return Status::Error("host not found:%s", host.c_str());
  }

  for (rp = res; rp != nullptr; rp = rp->ai_next) {
    switch (rp->ai_family) {
      case AF_INET:
        break;
      case AF_INET6:
        VLOG(1) << "Currently does not support Ipv6 address";
        continue;
      default:
        continue;
    }

    auto address = (reinterpret_cast<struct sockaddr_in*>(rp->ai_addr))->sin_addr.s_addr;
    // We need to match the integer byte order generated by ipv4ToInt,
    // so we need to convert here.
    addrs.emplace_back(intToIPv4(htonl(std::move(address))), port);
  }

  freeaddrinfo(res);

  if (addrs.empty()) {
    return Status::Error("host not found: %s", host.c_str());
  }

  return addrs;
}

std::string NetworkUtils::intToIPv4(uint32_t ip) {
  static const std::vector<std::string> kDict{
      "0",   "1",   "2",   "3",   "4",   "5",   "6",   "7",   "8",   "9",   "10",  "11",  "12",
      "13",  "14",  "15",  "16",  "17",  "18",  "19",  "20",  "21",  "22",  "23",  "24",  "25",
      "26",  "27",  "28",  "29",  "30",  "31",  "32",  "33",  "34",  "35",  "36",  "37",  "38",
      "39",  "40",  "41",  "42",  "43",  "44",  "45",  "46",  "47",  "48",  "49",  "50",  "51",
      "52",  "53",  "54",  "55",  "56",  "57",  "58",  "59",  "60",  "61",  "62",  "63",  "64",
      "65",  "66",  "67",  "68",  "69",  "70",  "71",  "72",  "73",  "74",  "75",  "76",  "77",
      "78",  "79",  "80",  "81",  "82",  "83",  "84",  "85",  "86",  "87",  "88",  "89",  "90",
      "91",  "92",  "93",  "94",  "95",  "96",  "97",  "98",  "99",  "100", "101", "102", "103",
      "104", "105", "106", "107", "108", "109", "110", "111", "112", "113", "114", "115", "116",
      "117", "118", "119", "120", "121", "122", "123", "124", "125", "126", "127", "128", "129",
      "130", "131", "132", "133", "134", "135", "136", "137", "138", "139", "140", "141", "142",
      "143", "144", "145", "146", "147", "148", "149", "150", "151", "152", "153", "154", "155",
      "156", "157", "158", "159", "160", "161", "162", "163", "164", "165", "166", "167", "168",
      "169", "170", "171", "172", "173", "174", "175", "176", "177", "178", "179", "180", "181",
      "182", "183", "184", "185", "186", "187", "188", "189", "190", "191", "192", "193", "194",
      "195", "196", "197", "198", "199", "200", "201", "202", "203", "204", "205", "206", "207",
      "208", "209", "210", "211", "212", "213", "214", "215", "216", "217", "218", "219", "220",
      "221", "222", "223", "224", "225", "226", "227", "228", "229", "230", "231", "232", "233",
      "234", "235", "236", "237", "238", "239", "240", "241", "242", "243", "244", "245", "246",
      "247", "248", "249", "250", "251", "252", "253", "254", "255"};

  auto& f1 = kDict[ip & 0x000000FF];
  auto& f2 = kDict[(ip >> 8) & 0x000000FF];
  auto& f3 = kDict[(ip >> 16) & 0x000000FF];
  auto& f4 = kDict[(ip >> 24) & 0x000000FF];

  char buf[16];
  char* pt = buf;
  strcpy(pt, f4.c_str());  // NOLINT
  pt += f4.size();
  *pt++ = '.';
  strcpy(pt, f3.c_str());  // NOLINT
  pt += f3.size();
  *pt++ = '.';
  strcpy(pt, f2.c_str());  // NOLINT
  pt += f2.size();
  *pt++ = '.';
  strcpy(pt, f1.c_str());  // NOLINT
  pt += f1.size();

  return buf;
}

StatusOr<std::vector<HostAddr>> NetworkUtils::toHosts(const std::string& peersStr) {
  std::vector<HostAddr> hosts;
  std::vector<std::string> peers;
  folly::split(",", peersStr, peers, true);
  hosts.reserve(peers.size());
  for (auto& peerStr : peers) {
    auto addrPort = folly::trimWhitespace(peerStr);
    auto pos = addrPort.find(':');
    if (pos == folly::StringPiece::npos) {
      return Status::Error("Bad peer format: %s", addrPort.start());
    }

    int32_t port;
    try {
      port = folly::to<int32_t>(addrPort.subpiece(pos + 1));
    } catch (const std::exception& ex) {
      return Status::Error("Bad port number, error: %s", ex.what());
    }

    auto addr = addrPort.subpiece(0, pos).toString();
    hosts.emplace_back(std::move(addr), port);
  }
  return hosts;
}

std::string NetworkUtils::toHostsStr(const std::vector<HostAddr>& hosts) {
  std::string hostsString = "";
  for (auto& host : hosts) {
    if (!hostsString.empty()) {
      hostsString.append(", ");
    }
    hostsString += folly::stringPrintf("%s:%d", host.host.c_str(), host.port);
  }
  return hostsString;
}

Status NetworkUtils::validateIP(const std::string& ip) {
  if (ip.empty()) {
    return Status::Error("ip is empty.");
  }
  const std::regex ipv4(
      R"(^((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3}$)");
  if (!std::regex_match(ip, ipv4)) {
    return Status::Error("%s is not a valid IP", ip.c_str());
  }
  return Status::OK();
}

Status NetworkUtils::validateHostOrIp(const std::string& hostOrIp) {
  if (hostOrIp.empty()) {
    return Status::Error("local_ip is empty, need to config it through config file.");
  }
  const std::regex ipv4(
      "((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}"
      "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)");
  if (std::regex_match(hostOrIp, ipv4)) {
    const std::regex loopbackOrAny(
        "^127(?:\\.[0-9]+){0,2}\\.[0-9]+$|^(?:0*\\:)*?:?0*1$|(0\\.){3}0");
    if (std::regex_match(hostOrIp, loopbackOrAny)) {
      return Status::OK();
    }
    auto ipsStatus = listIPv4s();
    NG_RETURN_IF_ERROR(ipsStatus);
    const auto& ips = ipsStatus.value();
    auto result = std::find(ips.begin(), ips.end(), hostOrIp);
    if (result == ips.end()) {
      return Status::Error("%s is not a valid ip in current host, candidates: %s",
                           hostOrIp.c_str(),
                           folly::join(",", ips).c_str());
    }
  } else {
    NG_RETURN_IF_ERROR(resolveHost(hostOrIp, 0));
  }
  return Status::OK();
}

std::string NetworkUtils::GetIPv4MacAddr() {
  // Get all ipv4 devices
  auto result = listDeviceAndIPv4s();
  if (!result.ok()) {
    LOG(WARNING) << "Failed to collect system info: " << result;
    return "";
  }

  // Collect all device names
  auto getval = [](const auto& entry) { return entry.first; };
  std::vector<std::string> ipv4Devices;
  ipv4Devices.resize(result.value().size());
  std::transform(result.value().begin(), result.value().end(), ipv4Devices.begin(), getval);

  std::vector<std::string> macList;
  // Iterate divices and retrive mac addresses
  for (auto& if_name : ipv4Devices) {
    auto if_nameStr = std::string(if_name);

    // Skip virtual network adapter
    if (if_nameStr.find("docker") != std::string::npos || if_nameStr.rfind("veth", 0) == 0) {
      continue;
    }

    struct ifreq ifr;
    size_t if_name_len = strlen(if_name.c_str());
    if (if_name_len < sizeof(ifr.ifr_name)) {
      memcpy(ifr.ifr_name, if_name.c_str(), if_name_len);
      ifr.ifr_name[if_name_len] = 0;
    } else {
      // Interface name is too long, ignore
      DLOG(WARNING) << "The interface name is too long";
      continue;
    }

    int fd = socket(AF_UNIX, SOCK_DGRAM, 0);
    // Failed to create socket using protocal
    if (fd == -1) {
      DLOG(WARNING) << "Failed to create socket using protocal, error: " << strerror(errno);
      continue;
    }

    // Failed to collect hardware info of the device, ignore
    if (ioctl(fd, SIOCGIFHWADDR, &ifr) == -1) {
      DLOG(WARNING) << "Failed to collect hardware info of the device";
      close(fd);
      continue;
    }
    close(fd);

    // The interface is not an Ethernet interface, ignore
    if (ifr.ifr_hwaddr.sa_family != ARPHRD_ETHER) {
      DLOG(WARNING) << "The interface is not an Ethernet interface";
      continue;
    }

    const unsigned char* mac = reinterpret_cast<unsigned char*>(ifr.ifr_hwaddr.sa_data);

    char buff[20];
    snprintf(buff,
             sizeof(buff),
             "%02X:%02X:%02X:%02X:%02X:%02X",
             mac[0],
             mac[1],
             mac[2],
             mac[3],
             mac[4],
             mac[5]);
    std::string buffAsStdStr = buff;

    // Skip virtual network adapter
    if (buffAsStdStr == "00:00:00:00:00:00") {
      continue;
    }
    // append all mac address
    macList.emplace_back(buffAsStdStr);
  }
  // Use ',' as the delimeter to separate multiple mac addresses
  auto code = folly::join(",", macList);

  return code;
}

}  // namespace network
}  // namespace nebula
