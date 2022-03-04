# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Push Limit down scan vertices rule
  Examples:
    | space_name |
    | nba         |
    | nba_int_vid      |

  Background:
    Given a graph with space named "<space_name>"

  Scenario: push limit down to Traverse
    When profiling query:
      """
      MATCH (v)-[:like]->()
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
      | 11 | AppendVertices | 3            |                |
      | 3  | Traverse       | 7            | {"limit": "3"} |
      | 7  | Dedup          | 2            |                |
      | 2  | IndexScan      | 0            |                |
      | 0  | Start          |              |                |
    When profiling query:
      """
      MATCH (v)-[:like]->()-[:like]->()
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
      | 11 | AppendVertices | 4            |                |
      | 4  | Traverse       | 3            | {"limit": "3"} |
      | 3  | Traverse       | 7            |                |
      | 7  | Dedup          | 2            |                |
      | 2  | IndexScan      | 0            |                |
      | 0  | Start          |              |                |
    When profiling query:
      """
      MATCH (v:player)-[:like*0..1]->()
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
      | 11 | AppendVertices | 3            |                |
      | 3  | Traverse       | 2            | {"limit": "3"} |
      | 2  | IndexScan      | 0            |                |
      | 0  | Start          |              |                |

  Scenario: push limit down to Traverse with parameter
    Given parameters: {"lmt": 3}
    When profiling query:
      """
      MATCH (v:player)-[:like*0..1]->()
      RETURN v.player.name LIMIT $lmt
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
      | 11 | AppendVertices | 3            |                |
      | 3  | Traverse       | 2            | {"limit": "3"} |
      | 2  | IndexScan      | 0            |                |
      | 0  | Start          |              |                |
