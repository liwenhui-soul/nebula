/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownAppendVerticesRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"

using nebula::graph::AppendVertices;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownAppendVerticesRule::kInstance =
    std::unique_ptr<PushLimitDownAppendVerticesRule>(new PushLimitDownAppendVerticesRule());

PushLimitDownAppendVerticesRule::PushLimitDownAppendVerticesRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownAppendVerticesRule::pattern() const {
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kLimit, {Pattern::create(graph::PlanNode::Kind::kAppendVertices)});
  return pattern;
}

// Limit can't push over filter and VFilter apply after push-down limit, so we check it
bool PushLimitDownAppendVerticesRule::match(OptContext *ctx, const MatchedResult &matched) const {
  DLOG(ERROR) << "DEBUG POINT: entry of PushLimitDownAppendVerticesRule::match";
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto appendVerticesGroupNode = matched.dependencies.front().node;
  DCHECK_EQ(appendVerticesGroupNode->node()->kind(), PlanNode::Kind::kAppendVertices);
  auto appendVertices = static_cast<const AppendVertices *>(appendVerticesGroupNode->node());
  return appendVertices->vFilter() == nullptr;
}

StatusOr<OptRule::TransformResult> PushLimitDownAppendVerticesRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto *qctx = octx->qctx();
  auto limitGroupNode = matched.node;
  auto appendVerticesGroupNode = matched.dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto appendVertices = static_cast<const AppendVertices *>(appendVerticesGroupNode->node());

  int64_t limitRows = limit->offset() + limit->count(qctx);
  if (appendVertices->limit(qctx) >= 0 && limitRows >= appendVertices->limit(qctx)) {
    return TransformResult::noTransform();
  }

  auto newLimit = static_cast<Limit *>(limit->clone());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto newAppendVertices = static_cast<AppendVertices *>(appendVertices->clone());
  newAppendVertices->setLimit(limitRows);
  auto newAppendVerticesGroup = OptGroup::create(octx);
  auto newAppendVerticesGroupNode = newAppendVerticesGroup->makeGroupNode(newAppendVertices);

  newLimitGroupNode->dependsOn(newAppendVerticesGroup);
  for (auto dep : appendVerticesGroupNode->dependencies()) {
    newAppendVerticesGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushLimitDownAppendVerticesRule::toString() const {
  return "PushLimitDownAppendVerticesRule";
}

}  // namespace opt
}  // namespace nebula
