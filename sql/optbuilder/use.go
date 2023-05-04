package optbuilder

import (
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *PlanBuilder) buildUse(inScope *scope, n *ast.Use) (outScope *scope) {
	name := n.DBName.String()
	ret := plan.NewUse(b.resolveDb(name))
	ret.Catalog = b.cat
	outScope = inScope.push()
	outScope.node = ret
	return
}
