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
			a.Log("found subquery %q with child of type %T", n.Name(), n.Child)

			// subqueries do not have access to outer scope
			child, err := a.Analyze(ctx, n.Child, nil)
			if err != nil {
				return nil, err
			}

			return n.WithChildren(child)
		default:
			return n, nil
		}
	})
}

func resolveSubqueryExpressions(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformExpressionsUpWithNode(n, func(n sql.Node, e sql.Expression) (sql.Expression, error) {
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
func stripQueryProcess(analyzed sql.Node) sql.Node {
	if qp, ok := analyzed.(*plan.QueryProcess); ok {
		analyzed = qp.Child
	}
	return analyzed
}

// cacheSubqueryResults determines whether it's safe to cache the results for any subquery expressions, and marks the
// subquery as cacheable if so. Caching subquery results is safe in the case that no outer scope columns are referenced,
// and if all expressions in the subquery are deterministic.
func cacheSubqueryResults(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	return plan.TransformExpressionsUpWithNode(n, func(n sql.Node, e sql.Expression) (sql.Expression, error) {
		s, ok := e.(*plan.Subquery)
		if !ok || !s.Resolved() {
			return e, nil
		}

		scopeLen := len(scope.newScope(n).Schema())
		cacheable := true

		plan.InspectExpressions(s.Query, func(expr sql.Expression) bool {
			if gf, ok := expr.(*expression.GetField); ok {
				if gf.Index() < scopeLen {
					cacheable = false
					return false
				}
			}

			if nd, ok := expr.(sql.NonDeterministicExpression); ok && nd.IsNonDeterministic() {
				cacheable = false
				return false
			}

			return true
		})

		if cacheable {
			return s.WithCachedResults(), nil
		}

		return s, nil
	})
}
