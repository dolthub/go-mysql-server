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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func resolveSubqueries(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_subqueries")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.SubqueryAlias:
			// subqueries do not have access to outer scope
			child, err := a.analyzeThroughBatch(ctx, n.Child, nil, "default-rules")
			if err != nil {
				return nil, err
			}

			if len(n.Columns) > 0 {
				schemaLen := schemaLength(n.Child)
				if schemaLen != len(n.Columns) {
					return nil, sql.ErrColumnCountMismatch.New()
				}
			}

			return n.WithChildren(stripQueryProcess(child))
		default:
			return n, nil
		}
	})
}

func finalizeSubqueries(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, ctx := ctx.Span("finalize_subqueries")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.SubqueryAlias:
			// subqueries do not have access to outer scope
			child, err := a.analyzeStartingAtBatch(ctx, n.Child, nil, "default-rules")
			if err != nil {
				return nil, err
			}

			if len(n.Columns) > 0 {
				schemaLen := schemaLength(n.Child)
				if schemaLen != len(n.Columns) {
					return nil, sql.ErrColumnCountMismatch.New()
				}
			}

			return n.WithChildren(stripQueryProcess(child))
		default:
			return n, nil
		}
	})
}

func flattenTableAliases(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, ctx := ctx.Span("flatten_table_aliases")
	defer span.Finish()
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.TableAlias:
			if sa, isSA := n.Children()[0].(*plan.SubqueryAlias); isSA {
				return sa.WithName(n.Name()), nil
			}
			if ta, isTA := n.Children()[0].(*plan.TableAlias); isTA {
				return ta.WithName(n.Name()), nil
			}
			return n, nil
		default:
			return n, nil
		}
	})
}

func resolveSubqueryExpressions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformExpressionsUpWithNode(ctx, n, func(n sql.Node, e sql.Expression) (sql.Expression, error) {
		s, ok := e.(*plan.Subquery)
		// We always analyze subquery expressions even if they are resolved, since other transformations to the surrounding
		// query might cause them to need to shift their field indexes.
		if !ok {
			return e, nil
		}

		subqueryCtx, cancelFunc := ctx.NewSubContext()
		defer cancelFunc()
		subScope := scope.newScope(n)

		analyzed, err := a.Analyze(subqueryCtx, s.Query, subScope)
		if err != nil {
			// We ignore certain errors, deferring them to later analysis passes. Specifically, if the subquery isn't
			// resolved or a column can't be found in the scope node, wait until a later pass.
			// TODO: we won't be able to give the right error message in all cases when we do this, although we attempt to
			//  recover the actual error in the validation step.
			if ErrValidationResolved.Is(err) || sql.ErrTableColumnNotFound.Is(err) || sql.ErrColumnNotFound.Is(err) {
				// keep the work we have and defer remainder of analysis of this subquery until a later pass
				return s.WithQuery(analyzed), nil
			}
			return nil, err
		}

		return s.WithQuery(stripQueryProcess(analyzed)), nil
	})
}

// If the node given is a QueryProcess, returns its child. Otherwise, returns the node.
// Something similar happens in the trackProcess analyzer step, but we can't always wait that long to get rid of the
// QueryProcess node.
// TODO: instead of stripping this node off after analysis, it would be better to just not add it in the first place.
func stripQueryProcess(n sql.Node) sql.Node {
	nodeIsPassthrough := true
	for nodeIsPassthrough {
		switch tn := n.(type) {
		case *plan.QueryProcess:
			n = tn.Child
		case *plan.StartTransaction:
			n = tn.Child
		default:
			nodeIsPassthrough = false
		}
	}

	return n
}

func exprIsCacheable(expr sql.Expression, lowestAlloewdIdx int) bool {
	cacheable := true
	sql.Inspect(expr, func(e sql.Expression) bool {
		if gf, ok := e.(*expression.GetField); ok {
			if gf.Index() < lowestAlloewdIdx {
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
	plan.Inspect(n, func(node sql.Node) bool {
		if er, ok := node.(sql.Expressioner); ok {
			for _, expr := range er.Expressions() {
				if !exprIsCacheable(expr, lowestAllowedIdx) {
					cacheable = false
					return false
				}
			}
		} else if sa, ok := node.(*plan.SubqueryAlias); ok {
			if !nodeIsCacheable(sa.Child, 0) {
				cacheable = false
			}
			return false
		}
		return true
	})
	return cacheable
}

func isDeterminstic(n sql.Node) bool {
	res := true
	plan.InspectExpressions(n, func(e sql.Expression) bool {
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
// and if all expressions in the subquery are deterministic.
func cacheSubqueryResults(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformExpressionsUpWithNode(ctx, n, func(n sql.Node, e sql.Expression) (sql.Expression, error) {
		s, ok := e.(*plan.Subquery)
		if !ok || !s.Resolved() {
			return e, nil
		}

		scopeLen := len(scope.newScope(n).Schema())
		cacheable := nodeIsCacheable(s.Query, scopeLen)

		if cacheable {
			return s.WithCachedResults(), nil
		}

		return s, nil
	})
}

// cacheSubqueryAlisesInJoins will look for joins against subquery aliases that
// will repeatedly execute the subquery, and will insert a *plan.CachedResults
// node on top of those nodes when it is safe to do so.
func cacheSubqueryAlisesInJoins(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	n, err := plan.TransformUpWithParent(n, func(child, parent sql.Node, childNum int) (sql.Node, error) {
		_, isJoin := parent.(plan.JoinNode)
		_, isIndexedJoin := parent.(*plan.IndexedJoin)
		if isJoin || isIndexedJoin {
			sa, isSubqueryAlias := child.(*plan.SubqueryAlias)
			if isSubqueryAlias && isDeterminstic(sa.Child) {
				return plan.NewCachedResults(child), nil
			}
		}
		return child, nil
	})
	if err != nil {
		return n, err
	}

	// If the most primary table in the top level join is a CachedResults, remove it.
	// We only want to do this if we're at the top of the tree.
	// TODO: Not a perfect indicator of whether we're at the top of the tree...
	if scope == nil {
		selector := func(parent sql.Node, child sql.Node, childNum int) bool {
			if _, isIndexedJoin := parent.(*plan.IndexedJoin); isIndexedJoin {
				return childNum == 0
			} else if j, isJoin := parent.(plan.JoinNode); isJoin {
				if j.JoinType() == plan.JoinTypeRight {
					return childNum == 1
				} else {
					return childNum == 0
				}
			}
			return true
		}
		n, err = plan.TransformUpWithSelector(n, selector, func(n sql.Node) (sql.Node, error) {
			cr, isCR := n.(*plan.CachedResults)
			if isCR {
				return cr.UnaryNode.Child, nil
			}
			return n, nil
		})
	}
	return n, err
}

func setJoinScopeLen(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	scopeLen := len(scope.Schema())
	if scopeLen == 0 {
		return n, nil
	}
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if j, ok := n.(plan.JoinNode); ok {
			nj := j.WithScopeLen(scopeLen)
			if _, ok := nj.Left().(*plan.StripRowNode); !ok {
				return nj.WithChildren(
					plan.NewStripRowNode(nj.Left(), scopeLen),
					plan.NewStripRowNode(nj.Right(), scopeLen),
				)
			} else {
				return nj, nil
			}
		}
		return n, nil
	})
}
