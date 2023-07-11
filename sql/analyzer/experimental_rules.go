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
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.Sort, *plan.Project, *plan.InsertInto:
			return fixidx.FixFieldIndexesForExpressions(a.LogFn(), n, scope)
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
		default:
			return n, transform.SameTree, nil
		}
	})
}
