package optbuilder

import (
	"github.com/dolthub/go-mysql-server/sql/plan"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

func (b *PlanBuilder) buildUse(inScope *scope, n *ast.Use) (outScope *scope) {
	name := n.DBName.String()

	database, err := b.cat.Database(b.ctx, name)
	if err != nil {
		b.handleErr(err)
	}

	ret := plan.NewUse(database)
	ret.Catalog = b.cat
	outScope = inScope.push()
	outScope.node = ret
	return
}
