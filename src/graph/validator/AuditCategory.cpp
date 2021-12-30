/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <string>

#include "parser/Sentence.h"

namespace nebula {
namespace graph {

using nebula::Sentence;

/**
 * The original categories of NGQL is not user-friendly, they are divided into
 * 'maintain', 'use', 'set', 'assignment' and 'mutate'. The audit log reclassified
 * NGQL and divided them into 'ddl', 'dql', 'dml', 'dcl', 'util' and 'unknown'.
 * In addition, there is also two categories named 'login' and 'exit', which is
 * about login and disconnection. Subsequent sentences should be classified here.
 */
std::string auditCategory(Sentence::Kind kind) {
  switch (kind) {
    case Sentence::Kind::kCreateTag:
    case Sentence::Kind::kAlterTag:
    case Sentence::Kind::kCreateEdge:
    case Sentence::Kind::kAlterEdge:
    case Sentence::Kind::kCreateTagIndex:
    case Sentence::Kind::kCreateEdgeIndex:
    case Sentence::Kind::kDropTagIndex:
    case Sentence::Kind::kDropEdgeIndex:
    case Sentence::Kind::kDropTag:
    case Sentence::Kind::kDropEdge:
    case Sentence::Kind::kDeleteTags:
    case Sentence::Kind::kCreateSpace:
    case Sentence::Kind::kCreateSpaceAs:
    case Sentence::Kind::kDropSpace:
    case Sentence::Kind::kCreateFTIndex:
    case Sentence::Kind::kDropFTIndex:
      return "ddl";
    case Sentence::Kind::kGo:
    case Sentence::Kind::kMatch:
    case Sentence::Kind::kLookup:
    case Sentence::Kind::kYield:
    case Sentence::Kind::kOrderBy:
    case Sentence::Kind::kFetchVertices:
    case Sentence::Kind::kFetchEdges:
    case Sentence::Kind::kFindPath:
    case Sentence::Kind::kLimit:
    case Sentence::Kind::kGroupBy:
    case Sentence::Kind::kReturn:
    case Sentence::Kind::kGetSubgraph:
      return "dql";
    case Sentence::Kind::kInsertVertices:
    case Sentence::Kind::kUpdateVertex:
    case Sentence::Kind::kInsertEdges:
    case Sentence::Kind::kUpdateEdge:
    case Sentence::Kind::kDeleteVertices:
    case Sentence::Kind::kDeleteEdges:
      return "dml";
    case Sentence::Kind::kCreateUser:
    case Sentence::Kind::kDropUser:
    case Sentence::Kind::kAlterUser:
    case Sentence::Kind::kGrant:
    case Sentence::Kind::kRevoke:
    case Sentence::Kind::kChangePassword:
    case Sentence::Kind::kCreateSnapshot:
    case Sentence::Kind::kDropSnapshot:
    case Sentence::Kind::kAdminJob:
    case Sentence::Kind::kMergeZone:
    case Sentence::Kind::kRenameZone:
    case Sentence::Kind::kSplitZone:
    case Sentence::Kind::kDropZone:
    case Sentence::Kind::kAddHosts:
    case Sentence::Kind::kAddHostsIntoZone:
    case Sentence::Kind::kDropHosts:
    case Sentence::Kind::kAddListener:
    case Sentence::Kind::kRemoveListener:
    case Sentence::Kind::kAddDrainer:
    case Sentence::Kind::kRemoveDrainer:
      return "dcl";
    case Sentence::Kind::kExplain:
    case Sentence::Kind::kUse:
    case Sentence::Kind::kAssignment:
    case Sentence::Kind::kDescribeTag:
    case Sentence::Kind::kDescribeEdge:
    case Sentence::Kind::kDescribeTagIndex:
    case Sentence::Kind::kDescribeEdgeIndex:
    case Sentence::Kind::kDescribeUser:
    case Sentence::Kind::kShowHosts:
    case Sentence::Kind::kShowSpaces:
    case Sentence::Kind::kShowParts:
    case Sentence::Kind::kShowTags:
    case Sentence::Kind::kShowEdges:
    case Sentence::Kind::kShowTagIndexes:
    case Sentence::Kind::kShowEdgeIndexes:
    case Sentence::Kind::kShowTagIndexStatus:
    case Sentence::Kind::kShowEdgeIndexStatus:
    case Sentence::Kind::kShowUsers:
    case Sentence::Kind::kShowRoles:
    case Sentence::Kind::kShowCreateSpace:
    case Sentence::Kind::kShowCreateTag:
    case Sentence::Kind::kShowCreateEdge:
    case Sentence::Kind::kShowCreateTagIndex:
    case Sentence::Kind::kShowCreateEdgeIndex:
    case Sentence::Kind::kShowSnapshots:
    case Sentence::Kind::kShowCharset:
    case Sentence::Kind::kShowCollation:
    case Sentence::Kind::kShowGroups:
    case Sentence::Kind::kShowZones:
    case Sentence::Kind::kShowStats:
    case Sentence::Kind::kShowServiceClients:
    case Sentence::Kind::kShowFTIndexes:
    case Sentence::Kind::kDescribeSpace:
    case Sentence::Kind::kDownload:
    case Sentence::Kind::kIngest:
    case Sentence::Kind::kShowConfigs:
    case Sentence::Kind::kSetConfig:
    case Sentence::Kind::kGetConfig:
    case Sentence::Kind::kAdminShowJobs:
    case Sentence::Kind::kDescribeZone:
    case Sentence::Kind::kListZones:
    case Sentence::Kind::kShowListener:
    case Sentence::Kind::kShowDrainers:
    case Sentence::Kind::kSignInService:
    case Sentence::Kind::kSignOutService:
    case Sentence::Kind::kShowSessions:
    case Sentence::Kind::kShowQueries:
    case Sentence::Kind::kKillQuery:
    case Sentence::Kind::kShowMetaLeader:
      return "util";
    default:
      return "unknown";
  }
}

}  // namespace graph
}  // namespace nebula
