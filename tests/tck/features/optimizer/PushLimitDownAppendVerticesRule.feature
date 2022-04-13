# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Limit down append vertices rule

  Background:
    Given a graph with space named "nba"

  Scenario: push limit down to AppendVertices
    When profiling query:
      """
      MATCH (v:player)-[:like]->(p)
      RETURN p.player.name LIMIT 3
      """
    Then the result should be, in any order:
      | p.player.name |
      | /\w+/         |
      | /\w+/         |
      | /\w+/         |
    And the execution plan should be:
      | id | name           | dependencies | operator info  |
      | 19 | Project        | 16           |                |
      | 16 | Limit          | 11           |                |
      | 11 | AppendVertices | 3            | {"limit": "3"} |
      | 3  | Traverse       | 2            |                |
      | 2  | IndexScan      | 0            |                |
      | 0  | Start          |              |                |

  Scenario: with limit push down
    # Require https://github.com/vesoft-inc/nebula/issues/3826
    # One Project node don't release symbols, and still read result of AppendVertices
    # in symbol table, so check data flow failed and lead to optimize failed.
    When profiling query:
      """
      MATCH (v)
      RETURN v.player.name LIMIT 3
      """
    Then the result should be, in any order:
      | v.player.name |
      | /\w+/         |
      | /\w+/         |
      | /\w+/         |
    And the execution plan should be:
      | id | name           | dependencies | operator info  |
      | 19 | Project        | 16           |                |
      | 16 | Limit          | 11           |                |
      | 11 | AppendVertices | 2            | {"limit": "3"} |
      | 2  | ScanVertices   | 0            |                |
      | 0  | Start          |              |                |

  @skip
  Scenario: push limit down to AppendVertices unimplemented
    # due to vertex filter is not null, it's already filtered by IndexScan, we could eliminate it.
    When profiling query:
      """
      MATCH (v:player)
      RETURN v.player.name LIMIT 3
      """
    Then the result should be, in any order:
      | v.player.name |
      | /\w+/         |
      | /\w+/         |
      | /\w+/         |
    And the execution plan should be:
      | id | name           | dependencies | operator info  |
      | 19 | Project        | 16           |                |
      | 16 | Limit          | 11           |                |
      | 11 | AppendVertices | 2            | {"limit": "3"} |
      | 2  | IndexScan      | 0            |                |
      | 0  | Start          |              |                |
