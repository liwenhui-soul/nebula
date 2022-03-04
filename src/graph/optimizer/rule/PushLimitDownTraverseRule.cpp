/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownTraverseRule.h"

#include <cstdlib>

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::AppendVertices;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;
using nebula::graph::Traverse;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownTraverseRule::kInstance =
    std::unique_ptr<PushLimitDownTraverseRule>(new PushLimitDownTraverseRule());

PushLimitDownTraverseRule::PushLimitDownTraverseRule() {
  RuleSet::QueryRules().addRule(this);
}

// AppendVertices will retrieve destination vertices from Traverse and join result of Traverse
// by equality of this destination vertex ID, so we could push Limit from AppendVertices to
// Traverse.
const Pattern &PushLimitDownTraverseRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kAppendVertices,
                                           {Pattern::create(graph::PlanNode::Kind::kTraverse)});
  return pattern;
}

bool PushLimitDownTraverseRule::match(OptContext *ctx, const MatchedResult &matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto *qctx = ctx->qctx();
  auto appendVerticesGroupNode = matched.node;
  DCHECK_EQ(appendVerticesGroupNode->node()->kind(), PlanNode::Kind::kAppendVertices);
  auto appendVertices = static_cast<const AppendVertices *>(appendVerticesGroupNode->node());
  auto traverseGroupNode = matched.dependencies.front().node;
  const auto traverse = static_cast<const Traverse *>(traverseGroupNode->node());
  if (appendVertices->vFilter() != nullptr || appendVertices->filter() != nullptr) {
    // Limit can't push over filter, so we check it
    return false;
  }
  if (appendVertices->limitExpr() == nullptr) {
    return false;
  } else if (graph::ExpressionUtils::isEvaluableExpr(appendVertices->limitExpr())) {
    if (appendVertices->limit(qctx) < 0) {
      return false;
    }
  } else {
    return false;
  }
  int64_t limitRows = appendVertices->limit(qctx);
  if (traverse->limitExpr() != nullptr) {
    if (!graph::ExpressionUtils::isEvaluableExpr(traverse->limitExpr())) {
      return false;
    }
    if (traverse->limit(qctx) >= 0 && limitRows >= traverse->limit(qctx)) {
      return false;
    }
  }
  // Only match when limit is valid
  return true;
}

StatusOr<OptRule::TransformResult> PushLimitDownTraverseRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto *qctx = octx->qctx();
  auto appendVerticesGroupNode = matched.node;
  auto traverseGroupNode = matched.dependencies.front().node;

  const auto appendVertices = static_cast<const AppendVertices *>(appendVerticesGroupNode->node());
  const auto traverse = static_cast<const Traverse *>(traverseGroupNode->node());

  auto newAppendVertices = static_cast<AppendVertices *>(appendVertices->clone());
  auto newAppendVerticesGroupNode =
      OptGroupNode::create(octx, newAppendVertices, appendVerticesGroupNode->group());

  auto newTraverse = static_cast<Traverse *>(traverse->clone());
  newTraverse->setLimit(appendVertices->limit(qctx));
  auto newTraverseGroup = OptGroup::create(octx);
  auto newTraverseGroupNode = newTraverseGroup->makeGroupNode(newTraverse);

  newAppendVerticesGroupNode->dependsOn(newTraverseGroup);
  for (auto dep : traverseGroupNode->dependencies()) {
    newTraverseGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newAppendVerticesGroupNode);
  return result;
}

std::string PushLimitDownTraverseRule::toString() const {
  return "PushLimitDownTraverseRule";
}

}  // namespace opt
}  // namespace nebula
