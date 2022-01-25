# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Audit function Test
  This file is mainly used to test the audit function

  Scenario: XML format test
    Given audit_log_format is assigned 'xml' and restart nebula
    When connect to any graphd
    Then nebula will record the connection information to an xml format file

  Scenario: JSON format test
    Given audit_log_format is assigned 'json' and restart nebula
    When connect to any graphd
    Then nebula will record the connection information to an json format file

  Scenario: CSV format test
    Given audit_log_format is assigned 'csv' and restart nebula
    When connect to any graphd
    Then nebula will record the connection information to an csv format file

  Scenario: Disable audit
    Given Disable audit log
    When connect to any graphd
    Then nebula will not record any connection information

  @skip
  Scenario: Elasticsearch test with authorization
    Given elasticsearch address
    When connect to any graphd
    Then nebula will record the connection information to es

  @skip
  Scenario: Elasticsearch test without authorization
    Given elasticsearch address, but wrong user and password
    When connect to any graphd
    Then nebula will not record any connection information to es

  Scenario: Exclude spaces test
    Given audit_log_exclude_spaces is assigned with some spaces
    When perform some operations(A) within thoese spaces
    And perform some operations(B) without thoese spaces
    Then nebula only record B operations

  Scenario: Log categories test
    Given audit_log_categories is assigned with some categories
    When perform some operations(A) within thoese categories
    And perform some operations(B) without thoese categories
    Then nebula only record A operations

  Scenario: Log categories test2
    Given audit_log_categories is assigned with all categories
    When perform some operations within thoese categories
    Then nebula will record all operations
