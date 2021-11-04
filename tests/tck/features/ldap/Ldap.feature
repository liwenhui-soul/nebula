# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
#
@skip
# need ldap server And modify the graph conf
Feature: ldap authentication-SimpleBindAuth

  Background:
  # FLAGS_enable_authorize=true;
  # FLAGS_auth_type=password;
  # use username "root", password "nebula" to login, and execute:
  # CREATE SPACE space1(partition_num=1, replica_factor=1);
  # Create shadow account
  # CREATE USER test2 WITH PASSWORD "";
  # GRANT ROLE ADMIN ON space1 TO test2;
  # then signout.
  Scenario: SimpleBindAuth
    # modify the graph conf:
    # FLAGS_auth_type=ldap;
    # FLAGS_ldap_server=127.0.0.1;
    # FLAGS_ldap_port=389;
    # FLAGS_ldap_scheme=ldap;
    # FLAGS_ldap_prefix=uid=;
    # FLAGS_ldap_suffix=,ou=it,dc=sys,dc=com;
    When executing query:
      """
      # Ldap server contains corresponding records
      # use username "test2", password "passwdtest2" to login
      """
    Then the login should be successful
    When executing query:
      """
      # Ldap server has no corresponding record
      # use username "admin", password "987" to login
      """
    Then the login should be failed
    When executing query:
      """
      # Ldap server has no corresponding record
      # use username "test2", password "987" to login
      """
    Then the login should be failed

  # signout, and modify graph conf
  # FLAGS_auth_type=password;
  # use username "root", password "nebula" to login, and execute:
  # When executing query:
  # """
  # DROP USER IF test2
  # """
  # Then the login should be successful
  Scenario: SearchBindAuth
    # modify the graph conf:
    # FLAGS_auth_type=ldap;
    # FLAGS_ldap_server=127.0.0.1;
    # FLAGS_ldap_port=389;
    # FLAGS_ldap_scheme=ldap;
    # FLAGS_ldap_basedn=ou=it,dc=sys,dc=com;
    When executing query:
      """
      # Ldap server contains corresponding records
      # use username "test2", password "passwdtest2" to login
      """
    Then the login should be successful
    When executing query:
      """
      # Ldap server has no corresponding record
      # use username "admin", password "987" to login
      """
    Then the login should be failed
    When executing query:
      """
      # Ldap server has no corresponding record
      # use username "test2", password "987" to login
      """
    Then the login should be failed
    # signout, and modify graph conf
    # FLAGS_auth_type=password;
    # use username "root", password "nebula" to login, and execute:
    When executing query:
      """
      DROP USER IF test2
      """
    Then the login should be successful
