/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/service/GraphFlags.h"

#include "version/Version.h"

DEFINE_int32(port, 3699, "Nebula Graph daemon's listen port");
DEFINE_int32(client_idle_timeout_secs,
             28800,
             "The number of seconds Nebula service waits before closing the idle connections");
DEFINE_int32(session_idle_timeout_secs, 28800, "The number of seconds before idle sessions expire");
DEFINE_int32(session_reclaim_interval_secs, 10, "Period we try to reclaim expired sessions");
DEFINE_int32(num_netio_threads,
             0,
             "The number of networking threads, 0 for number of physical CPU cores");
DEFINE_int32(num_accept_threads, 1, "Number of threads to accept incoming connections");
DEFINE_int32(num_worker_threads, 0, "Number of threads to execute user queries");
DEFINE_int32(num_operator_threads, 2, "Number of threads to execute a single operator");
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

DEFINE_int32(max_sessions_per_ip_per_user,
             300,
             "Maximum number of sessions that can be created per IP and per user");

DEFINE_bool(enable_optimizer, false, "Whether to enable optimizer");

#ifndef BUILD_STANDALONE
DEFINE_uint32(ft_request_retry_times, 3, "Retry times if fulltext request failed");
DEFINE_bool(enable_client_white_list, true, "Turn on/off the client white list.");
DEFINE_string(client_white_list,
              nebula::getOriginVersion() + ":3.0.0",
              "A white list for different client versions, separate with colon.");

#endif

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
DEFINE_string(ldap_searchfilter, "", "Use search filter, more flexible than searchattribute");

DEFINE_int32(num_rows_to_check_memory, 1024, "number rows to check memory");

// License file path
DEFINE_string(license_path, "/nebula.license", "File path to license file");

// audit
DEFINE_bool(enable_audit, false, "Whether to enable the audit log.");
DEFINE_string(audit_log_handler, "file", "Where to write the audit log. Optional: [ file | es ]");
DEFINE_string(audit_log_file,
              "./logs/audit/audit.log",
              "When audit_log_handler is 'file', the name of the log file.");
DEFINE_string(audit_log_strategy,
              "synchronous",
              "When audit_log_handler is 'file', audit log flush strategy."
              "Optional: [ synchronous｜ asynchronous ]."
              "Caution: For performance reasons, when the buffer is full and has not been "
              "flushed to the disk, the 'asynchronous' mode will discard subsequent requests.");
DEFINE_uint64(audit_log_max_buffer_size,
              1048576,
              "When audit_log_strategy is 'asynchronous', the maximum size "
              "of the memory buffer. Uint: B");
DEFINE_string(audit_log_format,
              "xml",
              "When audit_log_handler is 'file', specify the log format."
              "Optional: [ xml | json | csv ]");
DEFINE_string(audit_log_es_address,
              "",
              "When audit_log_handler is 'es', specify the comma-seperated list of "
              "Elasticsearch addresses, eg, 192.168.0.1:7001, 192.168.0.2:7001");
DEFINE_string(audit_log_es_user, "", "Specify the user name of the Elasticsearch.");
DEFINE_string(audit_log_es_password, "", "Specify the user password of the Elasticsearch.");
DEFINE_uint64(audit_log_es_batch_size,
              1000,
              "The number of logs which are sent to Elasticsearch at one time.");
DEFINE_string(audit_log_exclude_spaces,
              "",
              "Specify the list of spaces for not tracking. The value can be "
              "comma separated list of spaces, ie, 'nba, basketball'.");
DEFINE_string(audit_log_categories,
              "login,exit",
              "Specify the list of log categories for tracking, eg, 'login, ddl'."
              "Categories contains [ login | exit | ddl | dql | dml | dcl | util | unknown ].");

// Sanity-checking Flag Values
static bool ValidateSessIdleTimeout(const char* flagname, int32_t value) {
  // The max timeout is 604800 seconds(a week)
  if (value > 0 && value < 604801)  // value is ok
    return true;

  FLOG_WARN("Invalid value for --%s: %d, the timeout should be an integer between 1 and 604800\n",
            flagname,
            (int)value);
  return false;
}
DEFINE_validator(session_idle_timeout_secs, &ValidateSessIdleTimeout);
DEFINE_validator(client_idle_timeout_secs, &ValidateSessIdleTimeout);
