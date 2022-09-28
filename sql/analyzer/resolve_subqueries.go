// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analyzer

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func resolveSubqueries(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("resolve_subqueries")
	defer span.End()

	// Because we need to pass outer scope information to SubqueryAliases now... we need a different strategy for finding them...
	// we need to find them top down instead of bottom up, so that we have the right scope in place when we call the analyzer.
	// transform.Inspect can be used to traverse from the top down
	//
	// Because of derived tables getting scope visibility, we now need to combine our logic for calculating subquery scope

	if _, ok := n.(*plan.Project); ok {
		fmt.Println()
	}

	selectorFunc := func(context transform.Context) bool {
		// TODO: Do we need to do something here to account for SubqueryExpressions? Couldn't we mess up scope by processing
		//       multiple levels of SubqueryExpressions otherwise? Seems like it!
		if _, ok := context.Parent.(*plan.SubqueryAlias); ok {
			// If the parent of the current node is a SubqueryAlias, return false to prevent
			// this node from being processed. We only want to process the next level of nested SubqueryAliases
			// so that we can calculate the scope iteratively, otherwise the scope passed to SubqueryAliases further
			// down in the tree won't be correct.
			return false
		}
		return true
	}
	ctxFunc := func(context transform.Context) (sql.Node, transform.TreeIdentity, error) {
		if sqa, ok := context.Node.(*plan.SubqueryAlias); ok {
			return analyzeSubqueryAlias(ctx, a, sqa, scope, sel)
		} else if expressioner, ok := context.Node.(sql.Expressioner); ok {
			exprs := expressioner.Expressions()
			var newExprs []sql.Expression
			for i, expr := range exprs {
				newExpr, identity, err := transform.Expr(expr, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
					if sq, ok := e.(*plan.Subquery); ok {
						return analyzeSubqueryExpression(ctx, a, context.Node, sq, scope, sel)
					} else {
						return e, transform.SameTree, nil
					}
				})
				if err != nil {
					return context.Node, transform.SameTree, err
				}
				if identity == transform.NewTree {
					if newExprs == nil {
						newExprs = make([]sql.Expression, len(exprs))
						copy(newExprs, exprs)
					}
					newExprs[i] = newExpr
				}
			}

			if newExprs != nil {
				newNode, err := expressioner.WithExpressions(newExprs...)
				return newNode, transform.NewTree, err
			}
		}

		return context.Node, transform.SameTree, nil
	}

	return transform.NodeWithCtx(n, selectorFunc, ctxFunc)
}

func finalizeSubqueries(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("finalize_subqueries")
	defer span.End()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.SubqueryAlias:
			// TODO: In MySQL 8.0.14 and higher, SubqueryAliases can access the OUTER scopes of the clause that defined them.
			//       Note: They still do not have access to the other tables defined in the same scope as derived table,
			//       and from testing... they don't seem to be able to access expression aliases (only tables and table aliases),
			//       but documentation doesn't seem to indicate that limitation.
			//       https://dev.mysql.com/blog-archive/supporting-all-kinds-of-outer-references-in-derived-tables-lateral-or-not/
			subScope := newScopeWithDepth(scope.RecursionDepth() + 1)
			if scope != nil && len(scope.nodes) > 1 {
				// As of MySQL 8.0.14 MySQL provides OUTER scope visibility to derived tables. Unlike LATERAL scope visibility, which
				// gives a derived table visibility to the adjacent expressions where the subquery is defined, OUTER scope visibility
				// gives a derived table visibility to the OUTER scope where the subquery is defined.
				subScope.nodes = scope.InnerToOuter()[1:]
			}

			child, same, err := a.analyzeStartingAtBatch(ctx, n.Child, subScope, "default-rules", sel)
			if err != nil {
				return nil, same, err
			}

			if len(n.Columns) > 0 {
				schemaLen := schemaLength(n.Child)
				if schemaLen != len(n.Columns) {
					return nil, transform.SameTree, sql.ErrColumnCountMismatch.New()
				}
			}
			if same {
				return n, transform.SameTree, nil
			}
			newn, err := n.WithChildren(StripPassthroughNodes(child))
			return newn, transform.NewTree, err
		default:
			return n, transform.SameTree, nil
		}
	})
}

func flattenTableAliases(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("flatten_table_aliases")
	defer span.End()
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.TableAlias:
			if sa, isSA := n.Children()[0].(*plan.SubqueryAlias); isSA {
				return sa.WithName(n.Name()), transform.NewTree, nil
			}
			if ta, isTA := n.Children()[0].(*plan.TableAlias); isTA {
				return ta.WithName(n.Name()), transform.NewTree, nil
			}
			return n, transform.SameTree, nil
		default:
			return n, transform.SameTree, nil
		}
	})
}

func analyzeSubqueryExpression(ctx *sql.Context, a *Analyzer, n sql.Node, sq *plan.Subquery, scope *Scope, sel RuleSelector) (sql.Expression, transform.TreeIdentity, error) {
	// We always analyze subquery expressions even if they are resolved, since other transformations to the surrounding
	// query might cause them to need to shift their field indexes.
	subqueryCtx, cancelFunc := ctx.NewSubContext()
	defer cancelFunc()
	subScope := scope.newScope(n)

	analyzed, _, err := a.analyzeWithSelector(subqueryCtx, sq.Query, subScope, SelectAllBatches, sel)
	if err != nil {
		// We ignore certain errors, deferring them to later analysis passes. Specifically, if the subquery isn't
		// resolved or a column can't be found in the scope node, wait until a later pass.
		// TODO: we won't be able to give the right error message in all cases when we do this, although we attempt to
		//  recover the actual error in the validation step.
		if ErrValidationResolved.Is(err) || sql.ErrTableColumnNotFound.Is(err) || sql.ErrColumnNotFound.Is(err) {
			// keep the work we have and defer remainder of analysis of this subquery until a later pass
			return sq.WithQuery(analyzed), transform.NewTree, nil
		}
		return nil, transform.SameTree, err
	}

	//todo(max): Infinite cycles with subqueries, unions, ctes, catalog.
	// we squashed most negative errors, where a rule fails to report a plan change
	// to the expense of positive errors, where a rule reports a change when the plan
	// is the same before/after.
	// .Resolved() might be useful for fixing these bugs.
	return sq.WithQuery(StripPassthroughNodes(analyzed)), transform.NewTree, nil
}

func analyzeSubqueryAlias(ctx *sql.Context, a *Analyzer, n *plan.SubqueryAlias, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	// TODO: In MySQL 8.0.14 and higher, SubqueryAliases can access the OUTER scopes of the clause that defined them.
	//       Note: They still do not have access to the other tables defined in the same scope as derived table,
	//       and from testing... they don't seem to be able to access expression aliases (only tables and table aliases),
	//       but documentation doesn't seem to indicate that limitation.
	//       https://dev.mysql.com/blog-archive/supporting-all-kinds-of-outer-references-in-derived-tables-lateral-or-not/
	subScope := newScopeWithDepth(scope.RecursionDepth() + 1)
	if scope != nil && len(scope.nodes) > 1 {
		// As of MySQL 8.0.14 MySQL provides OUTER scope visibility to derived tables. Unlike LATERAL scope visibility, which
		// gives a derived table visibility to the adjacent expressions where the subquery is defined, OUTER scope visibility
		// gives a derived table visibility to the OUTER scope where the subquery is defined.
		// In this case, we rip off the current inner node so that the outer scope nodes are still present, but not the lateral nodes
		subScope.nodes = scope.InnerToOuter()[1:]
	}

	child, same, err := a.analyzeThroughBatch(ctx, n.Child, subScope, "default-rules", sel)
	if err != nil {
		return nil, same, err
	}

	if len(n.Columns) > 0 {
		schemaLen := schemaLength(n.Child)
		if schemaLen != len(n.Columns) {
			return nil, transform.SameTree, sql.ErrColumnCountMismatch.New()
		}
	}
	if same {
		return n, transform.SameTree, nil
	}
	newn, err := n.WithChildren(StripPassthroughNodes(child))
	return newn, transform.NewTree, err
}

func resolveSubqueryExpressions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {

	// TODO: When we combine this one with resolveSubqueries, we won't need to have this one listed in the analyzer rules anymore

	// NOTE: This operates ONLY on the current node. Looking at all expressions in the tree to find all Subqueries.
	// Any subqueries identified in the expression trees will have the correct scope because subqueries cannot directly
	// contain expressions for other subqueries (only through their Query nodes can they embed more subqueries).

	return transform.NodeExprsWithNode(n, func(n sql.Node, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		sq, ok := e.(*plan.Subquery)
		if !ok {
			return e, transform.SameTree, nil
		}
		return analyzeSubqueryExpression(ctx, a, n, sq, scope, sel)
	})
}

// StripPassthroughNodes strips all top-level passthrough nodes meant to apply only to top-level queries (query
// tracking, transaction logic, etc) from the node tree given and return the first non-passthrough child element. This
// is useful for when we invoke the analyzer recursively when e.g. analyzing subqueries or triggers
// TODO: instead of stripping this node off after analysis, it would be better to just not add it in the first place.
func StripPassthroughNodes(n sql.Node) sql.Node {
	nodeIsPassthrough := true
	for nodeIsPassthrough {
		switch tn := n.(type) {
		case *plan.QueryProcess:
			n = tn.Child()
		case *plan.StartTransaction:
			n = tn.Child
		case *plan.TransactionCommittingNode:
			n = tn.Child()
		default:
			nodeIsPassthrough = false
		}
	}

	return n
}

func exprIsCacheable(expr sql.Expression, lowestAllowedIdx int) bool {
	cacheable := true
	sql.Inspect(expr, func(e sql.Expression) bool {
		if gf, ok := e.(*expression.GetField); ok {
			if gf.Index() < lowestAllowedIdx {
				cacheable = false
				return false
			}
		}
		if nd, ok := e.(sql.NonDeterministicExpression); ok && nd.IsNonDeterministic() {
			cacheable = false
			return false
		}
		return true
	})
	return cacheable
}

func nodeIsCacheable(n sql.Node, lowestAllowedIdx int) bool {
	cacheable := true
	transform.Inspect(n, func(node sql.Node) bool {
		if er, ok := node.(sql.Expressioner); ok {
			for _, expr := range er.Expressions() {
				if !exprIsCacheable(expr, lowestAllowedIdx) {
					cacheable = false
					return false
				}
			}
		} else if _, ok := node.(*plan.SubqueryAlias); ok {
			// SubqueryAliases are always cacheable.  In fact, we
			// do not go far enough here yet. CTEs must be cached /
			// materialized and the same result set used throughout
			// the query when they are non-determinstic in order to
			// give correct results.
			return false
		}
		return true
	})
	return cacheable
}

func isDeterminstic(n sql.Node) bool {
	res := true
	transform.InspectExpressions(n, func(e sql.Expression) bool {
		if s, ok := e.(*plan.Subquery); ok {
			if !isDeterminstic(s.Query) {
				res = false
			}
			return false
		} else if nd, ok := e.(sql.NonDeterministicExpression); ok && nd.IsNonDeterministic() {
			res = false
			return false
		}
		return true
	})
	return res
}

// cacheSubqueryResults determines whether it's safe to cache the results for any subquery expressions, and marks the
// subquery as cacheable if so. Caching subquery results is safe in the case that no outer scope columns are referenced,
// if all expressions in the subquery are deterministic, and if the subquery isn't inside a trigger block.
func cacheSubqueryResults(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	// No need to inspect for trigger blocks as the Analyzer is recursively invoked on trigger blocks.
	if n, ok := n.(*plan.TriggerBeginEndBlock); ok {
		return n, transform.SameTree, nil
	}
	return transform.NodeExprsWithNode(n, func(n sql.Node, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		s, ok := e.(*plan.Subquery)
		if !ok || !s.Resolved() {
			return e, transform.SameTree, nil
		}

		scopeLen := len(scope.newScope(n).Schema())
		cacheable := nodeIsCacheable(s.Query, scopeLen)

		if cacheable {
			return s.WithCachedResults(), transform.NewTree, nil
		}

		return s, transform.SameTree, nil
	})
}

// cacheSubqueryAlisesInJoins will look for joins against subquery aliases that
// will repeatedly execute the subquery, and will insert a *plan.CachedResults
// node on top of those nodes.
func cacheSubqueryAlisesInJoins(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	n, sameA, err := transform.NodeWithCtx(n, nil, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		_, isJoin := c.Parent.(plan.JoinNode)
		_, isIndexedJoin := c.Parent.(*plan.IndexedJoin)
		if isJoin || isIndexedJoin {
			_, isSubqueryAlias := c.Node.(*plan.SubqueryAlias)
			if isSubqueryAlias {
				// SubqueryAliases are always cacheable. They
				// cannot reference their outside scope and
				// even when they have non-determinstic
				// expressions they should return the same
				// results across multiple iterations.
				return plan.NewCachedResults(c.Node), transform.NewTree, nil
			}
		}
		return c.Node, transform.SameTree, nil
	})
	if err != nil {
		return n, transform.SameTree, err
	}

	// If the most primary table in the top level join is a CachedResults, remove it.
	// We only want to do this if we're at the top of the tree.
	// TODO: Not a perfect indicator of whether we're at the top of the tree...
	sameD := transform.SameTree
	if scope.IsEmpty() {
		selector := func(c transform.Context) bool {
			if _, isIndexedJoin := c.Parent.(*plan.IndexedJoin); isIndexedJoin {
				return c.ChildNum == 0
			} else if j, isJoin := c.Parent.(plan.JoinNode); isJoin {
				if j.JoinType() == plan.JoinTypeRight {
					return c.ChildNum == 1
				} else {
					return c.ChildNum == 0
				}
			}
			return true
		}
		n, sameD, err = transform.NodeWithCtx(n, selector, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
			cr, isCR := c.Node.(*plan.CachedResults)
			if isCR {
				return cr.UnaryNode.Child, transform.NewTree, nil
			}
			return c.Node, transform.SameTree, nil
		})
	}
	return n, sameA && sameD, err
}

func setJoinScopeLen(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	scopeLen := len(scope.Schema())
	if scopeLen == 0 {
		return n, transform.SameTree, nil
	}
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if j, ok := n.(plan.JoinNode); ok {
			nj := j.WithScopeLen(scopeLen)
			if _, ok := nj.Left().(*plan.StripRowNode); !ok {
				nj, err := nj.WithChildren(
					plan.NewStripRowNode(nj.Left(), scopeLen),
					plan.NewStripRowNode(nj.Right(), scopeLen),
				)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return nj, transform.NewTree, nil
			} else {
				return nj, transform.NewTree, nil
			}
		}
		return n, transform.SameTree, nil
	})
}

// setViewTargetSchema is used to set the target schema for views. It is run after resolve_subqueries in order for
// SubqueryAlias resolution to happen.
func setViewTargetSchema(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("set_view_target_schema")
	defer span.End()

	if _, ok := n.(*plan.ShowColumns); !ok {
		return n, transform.SameTree, nil
	}

	t, ok := n.(sql.SchemaTarget)
	if !ok {
		return n, transform.SameTree, nil
	}

	sq := getSubqueryAlias(n)
	if sq == nil {
		return n, transform.SameTree, nil
	}

	n, err := t.WithTargetSchema(sq.Schema())
	if err != nil {
		return nil, transform.SameTree, err
	}

	return n, transform.NewTree, nil
}

func getSubqueryAlias(node sql.Node) *plan.SubqueryAlias {
	var sq *plan.SubqueryAlias
	transform.Inspect(node, func(node sql.Node) bool {
		// Only want to the first match
		if sq != nil {
			return false
		}

		switch n := node.(type) {
		case *plan.SubqueryAlias:
			sq = n
			return false
		}
		return true
	})
	return sq
}
