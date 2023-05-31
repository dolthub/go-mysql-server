package analyzer

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql/fixidx"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// fixupAuxiliaryExprs calls FixUpExpressions on Sort and Project nodes
// to compensate for the new name resolution expression overloading GetField
// indexes.
func fixupAuxiliaryExprs(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n.(type) {
		case *plan.Sort, *plan.Project:
			return fixidx.FixFieldIndexesForExpressions(a.LogFn(), n, scope)
		default:
			return n, transform.SameTree, nil
		}
	})
}

func transformJoinApply_experimental(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	switch n.(type) {
	case *plan.DeleteFrom, *plan.InsertInto:
		return n, transform.SameTree, nil
	}
	var applyId int

	ret := n
	var err error
	same := transform.TreeIdentity(false)
	iters := 0
	for !same {
		if iters > 50 {
			return n, transform.SameTree, fmt.Errorf("hit applyJoin stack limit")
		}
		// simplifySubqExpr can merge two scopes, requiring us to either
		// recurse on the merged scope or perform a fixed-point iteration.
		ret, same, err = transform.Node(ret, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
			var filters []sql.Expression
			var child sql.Node
			switch n := n.(type) {
			case *plan.Filter:
				child = n.Child
				filters = expression.SplitConjunction(n.Expression)
			default:
			}

			if sel == nil {
				return n, transform.SameTree, nil
			}

			subScope := scope.NewScopeFromSubqueryExpression(n)
			var matches []applyJoin
			var newFilters []sql.Expression

			// separate decorrelation candidates
			for _, e := range filters {
				if !plan.IsNullRejecting(e) {
					// TODO: rewrite dual table to permit in-scope joins,
					// which aren't possible when values are projected
					// above join filter
					rt := getResolvedTable(n)
					if rt == nil || plan.IsDualTable(rt.Table) {
						newFilters = append(newFilters, e)
						continue
					}
				}

				candE := e
				op := plan.JoinTypeSemi
				if n, ok := e.(*expression.Not); ok {
					candE = n.Child
					op = plan.JoinTypeAnti
				}

				var sq *plan.Subquery
				var l sql.Expression
				var joinF sql.Expression
				var max1 bool
				switch e := candE.(type) {
				case *plan.InSubquery:
					sq, _ = e.Right.(*plan.Subquery)
					l = e.Left
					joinF = expression.NewEquals(nil, nil)
				case expression.Comparer:
					sq, _ = e.Right().(*plan.Subquery)
					l = e.Left()
					joinF = e
					max1 = true
				default:
				}
				if sq != nil && (sq.CanCacheResults() || nodeIsCacheable(sq.Query, len(subScope.Schema())+1)) {
					matches = append(matches, applyJoin{l: l, r: sq, op: op, filter: joinF, max1: max1})
				} else {
					newFilters = append(newFilters, e)
				}
			}
			if len(matches) == 0 {
				return n, transform.SameTree, nil
			}

			ret := child
			for _, m := range matches {
				// A successful candidate is built with:
				// (1) Semi or anti join between the outer scope and (2) conditioned on (3).
				// (2) Simplified or unnested subquery (table alias).
				// (3) Join condition synthesized from the original correlated expression
				//     normalized to match changes to (2).
				subq := m.r

				name := fmt.Sprintf("scalarSubq%d", applyId)
				applyId++

				sch := subq.Query.Schema()
				var rightF sql.Expression
				var newCols []string
				if len(sch) == 1 {
					subqCol := subq.Query.Schema()[0]
					rightF = expression.NewGetFieldWithTable(len(scope.Schema()), subqCol.Type, name, subqCol.Name, subqCol.Nullable)
					newCols = append(newCols, subqCol.Name)
				} else {
					tup := make(expression.Tuple, len(sch))
					for i, c := range sch {
						tup[i] = expression.NewGetFieldWithTable(len(scope.Schema())+i, c.Type, name, c.Name, c.Nullable)
						newCols = append(newCols, c.Name)
					}
					rightF = tup
				}

				q, _, err := fixidx.FixFieldIndexesForNode(a.LogFn(), scope, subq.Query)
				if err != nil {
					return nil, transform.SameTree, err
				}

				var newSubq sql.Node = plan.NewSubqueryAlias(name, subq.QueryString, q).WithColumns(newCols)
				newSubq, err = simplifySubqExpr(newSubq)
				if err != nil {
					return nil, transform.SameTree, err
				}
				if m.max1 {
					newSubq = plan.NewMax1Row(newSubq, name)
				}

				condSch := append(ret.Schema(), newSubq.Schema()...)
				filter, err := m.filter.WithChildren(m.l, rightF)
				if err != nil {
					return n, transform.SameTree, err
				}
				filter, _, err = fixidx.FixFieldIndexes(scope, a.LogFn(), condSch, filter)
				if err != nil {
					return n, transform.SameTree, err
				}
				var comment string
				if c, ok := ret.(sql.CommentedNode); ok {
					comment = c.Comment()
				}
				ret = plan.NewJoin(ret, newSubq, m.op, filter).WithComment(comment)
			}

			if len(newFilters) == 0 {
				return ret, transform.NewTree, nil
			}
			return plan.NewFilter(expression.JoinAnd(newFilters...), ret), transform.NewTree, nil
		})
		if err != nil {
			return n, transform.SameTree, err
		}
		iters++
	}
	return ret, transform.TreeIdentity(applyId == 0), nil
}
