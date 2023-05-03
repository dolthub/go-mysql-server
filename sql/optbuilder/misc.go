package optbuilder

import (
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *PlanBuilder) buildAnalyze(inScope *scope, n *ast.Analyze, query string) (outScope *scope) {
	outScope = inScope.push()
	names := make([]sql.DbTable, len(n.Tables))
	for i, table := range n.Tables {
		names[i] = sql.DbTable{Db: table.Qualifier.String(), Table: table.Name.String()}
	}
	outScope.node = plan.NewAnalyze(names)
	return
}
