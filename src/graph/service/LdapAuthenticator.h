/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_SERVICE_LDAPAUTHENTICATOR_H_
#define GRAPH_SERVICE_LDAPAUTHENTICATOR_H_

#include <ldap.h>

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/base/StatusOr.h"
#include "graph/service/Authenticator.h"

namespace nebula {
namespace graph {

/**
 * LDAP authentication has two modes: simple bind mode and search bind mode.
 * common parameters:
 * FLAGS_ldap_server,
 * FLAGS_ldap_port,
 * FLAGS_ldap_scheme,
 * FLAGS_ldap_tls
 *
 * Simple bind mode uses the parameters:
 * FLAGS_ldap_prefix,
 * FLAGS_ldap_suffix
 * dn(distinguished name) = FLAGS_ldap_prefix + userName + FLAGS_ldap_suffix
 *
 * Search bind mode uses the parameters:
 * FLAGS_ldap_basedn,
 * FLAGS_ldap_binddn,
 * FLAGS_ldap_bindpasswd,
 * one of FLAGS_ldap_searchattribute or FLAGS_ldap_searchfilter
 *
 * Disallow mixing the parameters of two modes.
 */
class LdapAuthenticator final : public Authenticator {
 public:
  explicit LdapAuthenticator(const meta::MetaClient* client);

  /**
   * Execute LDAP authentication.
   */
  bool auth(const std::string& user, const std::string& password) override;

 private:
  /**
   * Check if LDAP authentication parameters are set legally.
   */
  Status prepare();

  /**
   * Init LDAP connect to LADP server.
   * FLAGS_ldap_server may be a comma-separated list of IP addresses.
   */
  Status initLDAPConnection();

  /**
   * Build the custom search filter.
   * Replace all occurrences of the placeholder "$username" in FLAGS_ldap_searchfilter
   * with variable userName_.
   */
  std::string buildSearchFilter();

  /**
   * Build the filter
   * Either search filter, or attribute filter, or default value.
   */
  std::string buildFilter();

  /**
   * Execute LDAP simple bind mode authentication
   */
  StatusOr<bool> simpleBindAuth();

  /**
   * Execute LDAP search bind mode authentication
   */
  StatusOr<bool> searchBindAuth();

 private:
  const meta::MetaClient* metaClient_;

  LDAP* ldap_{nullptr};

  // Default LDAP port
  int ldapPort_{389};

  std::string userName_;
  std::string password_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_SERVICE_LDAPAUTHENTICATOR_H_
