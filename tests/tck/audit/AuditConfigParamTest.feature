# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Audit config Param test
  This file is mainly used to test the audit config param

  Scenario Outline: Normal enable_audit parameter
    Given enable_audit is assigned an normal parameter: <param>
    When restart nebula
    Then nebula will restart successfully

    Examples:
      | param |
      | true  |
      | TRUE  |
      | TRue  |
      | false |

  Scenario Outline: Exception enable_audit parameter
    Given enable_audit is assigned an exception parameter: <param>
    When restart nebula
    Then nebula will fail to restart

    Examples:
      | param   |
      |         |
      | ''      |
      | ' true' |
      | abc     |

  Scenario Outline: Normal audit_log_handler parameter
    Given audit_log_handler is assigned an normal parameter: <param>
    When restart nebula
    Then nebula will restart successfully

    Examples:
      | param  |
      | file   |
      | es     |
      | 'file' |
      | FIle   |

  Scenario Outline: Exception audit_log_handler parameter
    Given audit_log_handler is assigned an exception parameter: <param>
    When restart nebula
    Then nebula will fail to restart

    Examples:
      | param |
      | 'es ' |
      |       |
      | other |

  Scenario Outline: Normal audit_log_file parameter
    Given audit_log_file is assigned an normal parameter: <param>
    When restart nebula
    Then nebula will restart successfully

    Examples:
      | param                    |
      | './logs/audit/audit.log' |
      | './Audit.log'            |
      | ./logs/aaa.txt           |

  Scenario Outline: Normal audit_log_strategy parameter
    Given audit_log_strategy is assigned an normal parameter: <param>
    When restart nebula
    Then nebula will restart successfully

    Examples:
      | param         |
      | synchronous   |
      | asynchronous  |
      | 'SYNCHrONOUS' |

  Scenario Outline: Exception audit_log_strategy parameter
    Given audit_log_strategy is assigned an exception parameter: <param>
    When restart nebula
    Then nebula will fail to restart

    Examples:
      | param |
      |       |
      | other |

  Scenario Outline: Normal audit_log_max_buffer_size parameter
    Given audit_log_max_buffer_size is assigned an normal parameter: <param>
    When restart nebula
    Then nebula will restart successfully

    Examples:
      | param |
      | 1     |
      | 100   |

  Scenario Outline: Exception audit_log_max_buffer_size parameter
    Given audit_log_max_buffer_size is assigned an exception parameter: <param>
    When restart nebula
    Then nebula will fail to restart

    Examples:
      | param                                              |
      | 0                                                  |
      | -1                                                 |
      | 99999999999999999999999999999999999999999999999999 |
      |                                                    |
      | abc                                                |

  Scenario Outline: Normal audit_log_format parameter
    Given audit_log_format is assigned an normal parameter: <param>
    When restart nebula
    Then nebula will restart successfully

    Examples:
      | param  |
      | xml    |
      | 'json' |
      | csv    |

  Scenario Outline: Exception audit_log_format parameter
    Given audit_log_format is assigned an exception parameter: <param>
    When restart nebula
    Then nebula will fail to restart

    Examples:
      | param   |
      | ,xml,   |
      | 'json ' |
      |         |
      | abc     |

  Scenario Outline: Normal audit_log_es_address parameter
    Given audit_log_es_address is assigned an normal parameter: <param>
    When restart nebula
    Then nebula will restart successfully

    Examples:
      | param                          |
      | localhost:9200                 |
      | '127:0:0:1:9200'               |
      | localhost:9200, 127:0:0:1:9200 |

  Scenario Outline: Exception audit_log_es_address parameter
    Given audit_log_es_address is assigned an exception parameter: <param>
    When restart nebula
    Then nebula will fail to restart

    Examples:
      | param |
      |       |
      | ,,    |

  Scenario Outline: Normal audit_log_categories parameter
    Given audit_log_categories is assigned an normal parameter: <param>
    When restart nebula
    Then nebula will restart successfully

    Examples:
      | param                                         |
      | login                                         |
      | exit                                          |
      | ddl                                           |
      | dml                                           |
      | 'DqL'                                         |
      | dcl                                           |
      | util                                          |
      | unknown                                       |
      | login,exit                                    |
      | login, exit, ddl, dml, dql, dcl, util,unknown |
      |                                               |

  Scenario Outline: Exception audit_log_categories parameter
    Given audit_log_categories is assigned an exception parameter: <param>
    When restart nebula
    Then nebula will fail to restart

    Examples:
      | param        |
      | other        |
      | 'login;exit' |
