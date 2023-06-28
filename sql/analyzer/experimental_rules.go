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
		switch n.(type) {
		case *plan.Sort, *plan.Project:
			return fixidx.FixFieldIndexesForExpressions(a.LogFn(), n, scope)
		default:
			return n, transform.SameTree, nil
		}
	})
}
