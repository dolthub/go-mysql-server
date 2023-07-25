package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql/fixidx"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// fixupAuxiliaryExprs calls FixUpExpressions on Sort and Project nodes
// to compensate for the new name resolution expression overloading GetField
// indexes.
func fixupAuxiliaryExprs(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeWithOpaque(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		default:
			return fixidx.FixFieldIndexesForExpressions(a.LogFn(), n, scope)
		case *plan.ShowVariables:
			if n.Filter != nil {
				newF, same, err := fixidx.FixFieldIndexes(scope, a.LogFn(), n.Schema(), n.Filter)
				if same || err != nil {
					return n, transform.SameTree, err
				}
				n.Filter = newF
				return n, transform.NewTree, nil
			}
			return n, transform.SameTree, nil
		//case *plan.Set:
		//	exprs, same, err := fixidx.FixFieldIndexesOnExpressions(scope, a.LogFn(), nil, n.Exprs...)
		//	if err != nil || same {
		//		return n, transform.SameTree, err
		//	}
		//	return plan.NewSet(exprs), transform.NewTree, nil
		case *plan.CreateTable:
			allSame := transform.SameTree
			if len(n.ChDefs) > 0 {
				for i, ch := range n.ChDefs {
					newExpr, same, err := fixidx.FixFieldIndexes(scope, a.LogFn(), n.CreateSchema.Schema, ch.Expr)
					if err != nil {
						return n, transform.SameTree, err
					}
					allSame = allSame && same
					n.ChDefs[i].Expr = newExpr
				}
			}
			return n, allSame, nil
		case *plan.Update:
			allSame := transform.SameTree
			if len(n.Checks) > 0 {
				for i, ch := range n.Checks {
					newExpr, same, err := fixidx.FixFieldIndexes(scope, a.LogFn(), n.Schema(), ch.Expr)
					if err != nil {
						return n, transform.SameTree, err
					}
					allSame = allSame && same
					n.Checks[i].Expr = newExpr
				}
			}
			return n, allSame, nil
		case *plan.InsertInto:
			newN, same1, err := fixidx.FixFieldIndexesForExpressions(a.LogFn(), n, scope)
			if err != nil {
				return n, transform.SameTree, err
			}
			ins := newN.(*plan.InsertInto)
			newSource, same2, err := fixupAuxiliaryExprs(ctx, a, ins.Source, scope, sel)
			if err != nil || (same1 && same2) {
				return n, transform.SameTree, err
			}
			ins.Source = newSource
			return ins, transform.NewTree, nil
			//case *plan.InsertInto:
			//	newN, _, err := fixidx.FixFieldIndexesForExpressions(a.LogFn(), n, scope)
			//	if err != nil {
			//		return n, transform.SameTree, err
			//	}
			//	newIns := newN.(*plan.InsertInto)
			//	newIns.OnDupExprs, _, err = fixidx.FixFieldIndexesOnExpressions(scope, a.LogFn(), n.Destination.Schema(), n.OnDupExprs...)
			//	if err != nil {
			//		return n, transform.SameTree, err
			//	}
			//	return newIns, transform.NewTree, nil
		}
	})
}
