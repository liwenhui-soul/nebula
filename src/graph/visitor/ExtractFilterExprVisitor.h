/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef GRAPH_VISITOR_EXTRACTFILTEREXPRVISITOR_H_
#define GRAPH_VISITOR_EXTRACTFILTEREXPRVISITOR_H_

#include <memory>

#include "common/meta/SchemaManager.h"
#include "graph/visitor/ExprVisitorImpl.h"

namespace nebula {
namespace graph {

class ExtractFilterExprVisitor final : public ExprVisitorImpl {
 public:
  explicit ExtractFilterExprVisitor(ObjectPool *ObjPool) : pool_(ObjPool) {}
  ExtractFilterExprVisitor(ObjectPool *ObjPool,
                           GraphSpaceID spaceId,
                           meta::SchemaManager *schemaMng)
      : pool_(ObjPool), spaceId_(spaceId), schemaMng_(schemaMng) {}

  bool ok() const override {
    return canBePushed_;
  }

  Expression *remainedExpr() {
    return remainedExpr_;
  }

  static ExtractFilterExprVisitor makePushGetNeighbors(ObjectPool *pool,
                                                       GraphSpaceID spaceId = -1,
                                                       meta::SchemaManager *schemaMng = nullptr) {
    ExtractFilterExprVisitor visitor(pool, spaceId, schemaMng);
    visitor.pushType_ = PushType::kGetNeighbors;
    return visitor;
  }

  static ExtractFilterExprVisitor makePushGetVertices(ObjectPool *pool,
                                                      GraphSpaceID spaceId = -1,
                                                      meta::SchemaManager *schemaMng = nullptr) {
    ExtractFilterExprVisitor visitor(pool, spaceId, schemaMng);
    visitor.pushType_ = PushType::kGetVertices;
    return visitor;
  }

  static ExtractFilterExprVisitor makePushGetEdges(ObjectPool *pool,
                                                   GraphSpaceID spaceId = -1,
                                                   meta::SchemaManager *schemaMng = nullptr) {
    ExtractFilterExprVisitor visitor(pool, spaceId, schemaMng);
    visitor.pushType_ = PushType::kGetEdges;
    return visitor;
  }

 private:
  using ExprVisitorImpl::visit;

  void visit(ConstantExpression *) override;
  void visit(LabelExpression *) override;
  void visit(UUIDExpression *) override;
  void visit(VariableExpression *) override;
  void visit(VersionedVariableExpression *) override;
  void visit(TagPropertyExpression *) override;
  void visit(LabelTagPropertyExpression *) override;
  void visit(EdgePropertyExpression *) override;
  void visit(InputPropertyExpression *) override;
  void visit(VariablePropertyExpression *) override;
  void visit(DestPropertyExpression *) override;
  void visit(SourcePropertyExpression *) override;
  void visit(EdgeSrcIdExpression *) override;
  void visit(EdgeTypeExpression *) override;
  void visit(EdgeRankExpression *) override;
  void visit(EdgeDstIdExpression *) override;
  void visit(VertexExpression *) override;
  void visit(EdgeExpression *) override;
  void visit(LogicalExpression *) override;
  void visit(ColumnExpression *) override;
  void visit(SubscriptRangeExpression *) override;

 private:
  enum class PushType {
    kGetNeighbors,
    kGetVertices,  // Get/Append/Scan Vertices
    kGetEdges,     // Get/Append/Scan Edges
  };
  bool visitLogicalAnd(LogicalExpression *expr, std::vector<bool> &flags);
  bool visitLogicalOr(LogicalExpression *expr);
  void splitOrExpr(LogicalExpression *expr,
                   std::vector<bool> &flags,
                   const unsigned int canNotPushedIndex);
  // void rewriteAndExpr(Expression *rewriteExpr);
  Expression *rewriteExpr(std::vector<Expression *> rel, std::vector<Expression *> sharedExprs);
  void ExtractRemainExpr(LogicalExpression *expr, std::vector<bool> &flags);

  ObjectPool *pool_;
  bool canBePushed_{true};
  bool isNested_{false};
  bool hasSplit{false};
  bool splitForbidden{false};
  Expression *remainedExpr_{nullptr};
  PushType pushType_{PushType::kGetNeighbors};
  GraphSpaceID spaceId_{-1};
  meta::SchemaManager *schemaMng_{nullptr};
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VISITOR_EXTRACTFILTEREXPRVISITOR_H_
