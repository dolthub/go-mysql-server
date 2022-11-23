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
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// resolveSubqueries runs analysis on each subquery expression and subquery alias in the specified node tree.
// Subqueries are processed from the top down and a new scope level is created for each subquery when it is sent
// to be analyzed.
func resolveSubqueries(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("resolve_subqueries")
	defer span.End()

	return resolveSubqueriesHelper(ctx, a, n, scope, sel, false)
}

// finalizeSubqueries runs the final analysis pass on subquery expressions and subquery aliases in the node tree to ensure
// they are fully resolved and that the plan is ready to be executed. The logic is similar to when subqueries are initially
// resolved with resolveSubqueries, but with a few small differences:
//   - resolveSubqueries skips pruneColumns and optimizeJoins for subquery expressions and only runs the OnceBeforeDefault
//     rule set on subquery aliases.
//   - finalizeSubqueries runs a full analysis pass on subquery expressions and runs all rule batches except for OnceBeforeDefault.
func finalizeSubqueries(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("finalize_subqueries")
	defer span.End()

	return resolveSubqueriesHelper(ctx, a, n, scope, sel, true)
}

func resolveSubqueriesHelper(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector, finalize bool) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(node, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if sqa, ok := n.(*plan.SubqueryAlias); ok {
			return analyzeSubqueryAlias(ctx, a, node, sqa, scope, sel, finalize)
		} else {
			return transform.OneNodeExprsWithNode(n, func(node sql.Node, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				if sq, ok := e.(*plan.Subquery); ok {
					return analyzeSubqueryExpression(ctx, a, n, sq, scope, sel, finalize)
				} else {
					return e, transform.SameTree, nil
				}
			})
		}
	})
}

// flattenTableAliases transforms TableAlias nodes that contain a SubqueryAlias or TableAlias node as the immediate
// child so that the top level TableAlias is removed and the nested SubqueryAlias or nested TableAlias is the new top
// level node, making sure to capture the alias name and transfer it to the new node. The parser doesn't directly
// create this nested structure; it occurs as the execution plan is built and altered during analysis, for
// example with CTEs that get plugged into the execution plan as the analyzer processes it.
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

// analyzeSubqueryExpression runs analysis on the specified subquery expression, |sq|. The specified node |n| is the node
// that contains the subquery expression and |finalize| indicates if this is the final run of the analyzer on the query
// before execution, which means all analyzer rules are included, otherwise SubqueryExprResolveSelector is used to prevent
// running pruneColumns and optimizeJoins for all non-final analysis passes.
func analyzeSubqueryExpression(ctx *sql.Context, a *Analyzer, n sql.Node, sq *plan.Subquery, scope *Scope, sel RuleSelector, finalize bool) (sql.Expression, transform.TreeIdentity, error) {
	// We always analyze subquery expressions even if they are resolved, since other transformations to the surrounding
	// query might cause them to need to shift their field indexes.
	subqueryCtx, cancelFunc := ctx.NewSubContext()
	defer cancelFunc()

	var analyzed sql.Node
	var err error
	if finalize {
		analyzed, _, err = a.analyzeStartingAtBatch(subqueryCtx, sq.Query,
			scope.newScopeFromSubqueryExpression(n), "default-rules", NewFinalizeSubqueryExprSelector(sel))
	} else {
		analyzed, _, err = a.analyzeThroughBatch(subqueryCtx, sq.Query,
			scope.newScopeFromSubqueryExpression(n), "once-after-default", NewResolveSubqueryExprSelector(sel))
	}
	if err != nil {
		// We ignore certain errors during non-final passes of the analyzer, deferring them to later analysis passes.
		// Specifically, if the subquery isn't resolved or a column can't be found in the scope node, wait until a later pass.
		if !finalize && (ErrValidationResolved.Is(err) || sql.ErrTableColumnNotFound.Is(err) || sql.ErrColumnNotFound.Is(err)) {
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

// analyzeSubqueryAlias runs analysis on the specified subquery alias, |sqa|. The |finalize| parameter indicates if this is
// the final run of the analyzer on the query before execution, which means all rules, starting from the default-rules
// batch are processed, otherwise only the once-before-default batch of rules is processed for all other non-final passes.
func analyzeSubqueryAlias(ctx *sql.Context, a *Analyzer, node sql.Node, sqa *plan.SubqueryAlias, scope *Scope, sel RuleSelector, finalize bool) (sql.Node, transform.TreeIdentity, error) {
	subScope := scope.newScopeFromSubqueryAlias(sqa)

	var child sql.Node
	var same transform.TreeIdentity
	var err error
	if finalize {
		child, same, err = a.analyzeStartingAtBatch(ctx, sqa.Child, subScope, "default-rules", NewFinalizeNestedSubquerySel(sel))
	} else {
		child, same, err = a.analyzeThroughBatch(ctx, sqa.Child, subScope, "default-rules", sel)
	}
	if err != nil {
		return nil, same, err
	}

	if len(sqa.Columns) > 0 {
		schemaLen := schemaLength(child)
		if schemaLen != len(sqa.Columns) {
			return nil, transform.SameTree, sql.ErrColumnCountMismatch.New()
		}
	}
	if same {
		return sqa, transform.SameTree, nil
	}
	newn, err := sqa.WithChildren(StripPassthroughNodes(child))
	return newn, transform.NewTree, err
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
		if sq, ok := e.(*plan.Subquery); ok {
			// even if the subquery is not itself cacheable, it may be cacheable within the larger scope we are processing
			if !nodeIsCacheable(sq.Query, lowestAllowedIdx) {
				cacheable = false
				return false
			}
		} else if nd, ok := e.(sql.NonDeterministicExpression); ok && nd.IsNonDeterministic() {
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
		} else if sqa, ok := node.(*plan.SubqueryAlias); ok {
			// TODO: Need more logic and testing with CTEs. For example, CTEs that are non-deterministic MUST be
			//       cached and have their result sets reused, otherwise query result will be incorrect.
			// If a subquery has visibility to outer scopes, then we need to check if it has
			// references to that outer scope. If not, it can be always be cached.
			if sqa.OuterScopeVisibility {
				if !nodeIsCacheable(sqa.Child, lowestAllowedIdx) {
					cacheable = false
				}
			}
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

// cacheSubqueryResults determines whether it's safe to cache the results for subqueries (expressions and aliases), and marks the
// subquery as cacheable if so. Caching subquery results is safe in the case that no outer scope columns are referenced,
// if all expressions in the subquery are deterministic, and if the subquery isn't inside a trigger block.
func cacheSubqueryResults(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	// No need to inspect for trigger blocks as the Analyzer is recursively invoked on trigger blocks.
	if n, ok := node.(*plan.TriggerBeginEndBlock); ok {
		return n, transform.SameTree, nil
	}

	return transform.Node(node, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if sqa, ok := n.(*plan.SubqueryAlias); ok {
			subScope := scope.newScopeFromSubqueryAlias(sqa)
			if nodeIsCacheable(sqa.Child, len(subScope.Schema())) {
				return sqa.WithCachedResults(), transform.NewTree, nil
			}
			return n, transform.SameTree, nil
		} else {
			return transform.OneNodeExpressions(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				if sq, ok := e.(*plan.Subquery); ok {
					subScope := scope.newScopeFromSubqueryExpression(n)
					if nodeIsCacheable(sq.Query, len(subScope.Schema())) {
						return sq.WithCachedResults(), transform.NewTree, nil
					}
				}
				return e, transform.SameTree, nil
			})
		}
	})
}

// cacheSubqueryAlisesInJoins will look for joins against subquery aliases that
// will repeatedly execute the subquery, and will insert a *plan.CachedResults
// node on top of those nodes.
func cacheSubqueryAliasesInJoins(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	newNode, sameA, err := transform.NodeWithCtx(n, nil, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch nn := c.Node.(type) {
		case *plan.JoinNode:
			return transform.NodeWithCtx(nn, nil, func(context transform.Context) (sql.Node, transform.TreeIdentity, error) {
				n := context.Node
				if context.Parent != nil {
					if _, ok := context.Parent.(*plan.CachedResults); ok {
						return n, transform.SameTree, nil
					}
				}

				switch nn := n.(type) {
				case *plan.SubqueryAlias:
					if nn.CanCacheResults {
						return plan.NewCachedResults(nn), transform.NewTree, nil
					}
				}
				return n, transform.SameTree, nil
			})
		}
		return c.Node, transform.SameTree, nil
	})
	if err != nil {
		return n, transform.SameTree, err
	}

	// If the most primary table in the top level join is a CachedResults, remove it.
	// We only want to do this if we're at the top of the tree.
	// TODO: Not a perfect indicator of whether we're at the top of the tree...
	// TODO(max): this should use an in-order top-down search
	sameD := transform.SameTree
	if scope.IsEmpty() {
		selector := func(c transform.Context) bool {
			j, ok := c.Parent.(*plan.JoinNode)
			if !ok {
				return true
			}
			switch {
			case j.Op.IsRightOuter():
				// TODO i'm not sure if this is possible, should be transposed
				return c.ChildNum == 1
			default:
				return c.ChildNum == 0
			}
		}
		newNode, sameD, err = transform.NodeWithCtx(newNode, selector, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
			cr, isCR := c.Node.(*plan.CachedResults)
			if isCR {
				return cr.UnaryNode.Child, transform.NewTree, nil
			}
			return c.Node, transform.SameTree, nil
		})
	}
	return newNode, sameA && sameD, err
}

// TODO(max): join iterators should inline remove parentRow + scope,
// deprecate this rule.
func setJoinScopeLen(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	scopeLen := len(scope.Schema())
	if scopeLen == 0 {
		return n, transform.SameTree, nil
	}
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if j, ok := n.(*plan.JoinNode); ok {
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
