/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/service/GraphFlags.h"

#include "version/Version.h"

DEFINE_int32(port, 3699, "Nebula Graph daemon's listen port");
DEFINE_int32(client_idle_timeout_secs,
             0,
             "Seconds before we close the idle connections, 0 for infinite");
DEFINE_int32(session_idle_timeout_secs,
             0,
             "Seconds before we expire the idle sessions, 0 for infinite");
DEFINE_int32(session_reclaim_interval_secs, 10, "Period we try to reclaim expired sessions");
DEFINE_int32(num_netio_threads,
             0,
             "Number of networking threads, 0 for number of physical CPU cores");
DEFINE_int32(num_accept_threads, 1, "Number of threads to accept incoming connections");
DEFINE_int32(num_worker_threads, 0, "Number of threads to execute user queries");
DEFINE_bool(reuse_port, true, "Whether to turn on the SO_REUSEPORT option");
DEFINE_int32(listen_backlog, 1024, "Backlog of the listen socket");
DEFINE_string(listen_netdev, "any", "The network device to listen on");
DEFINE_string(local_ip, "", "Local ip specified for graphd");
DEFINE_string(pid_file, "pids/nebula-graphd.pid", "File to hold the process id");

DEFINE_bool(daemonize, true, "Whether run as a daemon process");
DEFINE_string(meta_server_addrs,
              "",
              "list of meta server addresses,"
              "the format looks like ip1:port1, ip2:port2, ip3:port3");
DEFINE_bool(local_config, false, "meta client will not retrieve latest configuration from meta");

DEFINE_string(default_charset, "utf8", "The default charset when a space is created");
DEFINE_string(default_collate, "utf8_bin", "The default collate when a space is created");

DEFINE_bool(enable_authorize, false, "Enable authorization, default false");
DEFINE_string(auth_type,
              "password",
              "User login authentication type,"
              "password for nebula authentication,"
              "ldap for ldap authentication,"
              "cloud for cloud authentication");

DEFINE_string(cloud_http_url, "", "cloud http url including ip, port, url path");
DEFINE_uint32(max_allowed_statements, 512, "Max allowed sequential statements");
DEFINE_uint32(max_allowed_query_size, 4194304, "Max allowed sequential query size");

DEFINE_int64(max_allowed_connections,
             std::numeric_limits<int64_t>::max(),
             "Max connections of the whole cluster");

DEFINE_bool(enable_optimizer, false, "Whether to enable optimizer");

DEFINE_uint32(ft_request_retry_times, 3, "Retry times if fulltext request failed");

DEFINE_bool(accept_partial_success, false, "Whether to accept partial success, default false");

DEFINE_bool(disable_octal_escape_char,
            false,
            "Octal escape character will be disabled"
            " in next version to ensure compatibility with cypher.");

DEFINE_bool(enable_experimental_feature, false, "Whether to enable experimental feature");

// LDAP authentication common parameters
DEFINE_string(ldap_server,
              "",
              "list of ldap server addresses, "
              "separate multiple addresses with comma");
DEFINE_int32(ldap_port, 0, "Ldap server port, if no port is specified, use the default port");
DEFINE_string(ldap_scheme, "ldap", "Only support ldap");
DEFINE_bool(ldap_tls,
            false,
            "Whether the connection between graphd and the LDAP server uses "
            "TLS encryption");

// LDAP authentication simple bind mode parameters
DEFINE_string(ldap_prefix,
              "",
              "Prepend the string to the user name "
              "to form the distinguished name");
DEFINE_string(ldap_suffix,
              "",
              "Append the string to the user name "
              "to form the distinguished name");

// LDAP authentication search bind mode parameters
DEFINE_string(ldap_basedn, "", "Root distinguished name to search the user");
DEFINE_string(ldap_binddn,
              "",
              "User distinguished name binding to the "
              "directory to perform the search");
DEFINE_string(ldap_bindpasswd,
              "",
              "User password binding to the directory "
              "to perform the search");
DEFINE_string(ldap_searchattribute, "", "Attribute to match the user name in the search");
DEFINE_string(ldap_searchfilter, "", "Use search filter, more flexible than searchattribut");

DEFINE_bool(enable_client_white_list, true, "Turn on/off the client white list.");
DEFINE_string(client_white_list,
              nebula::getOriginVersion() + ":2.5.0:2.5.1:2.6.0",
              "A white list for different client versions, separate with colon.");

DEFINE_int32(num_rows_to_check_memory, 1024, "number rows to check memory");
// License file path
DEFINE_string(license_path, "/nebula.license", "File path to license file");
