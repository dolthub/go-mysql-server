package planbuilder

import (
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *Builder) buildAnalyze(inScope *scope, n *ast.Analyze, query string) (outScope *scope) {
	outScope = inScope.push()
	names := make([]sql.DbTable, len(n.Tables))
	defaultDb := b.ctx.GetCurrentDatabase()
	for i, table := range n.Tables {
		dbName := table.Qualifier.String()
		if dbName == "" {
			if defaultDb == "" {
				err := sql.ErrNoDatabaseSelected.New()
				b.handleErr(err)
			}
			dbName = defaultDb
		}
		names[i] = sql.DbTable{Db: dbName, Table: strings.ToLower(table.Name.String())}
	}
	analyze := plan.NewAnalyze(names)

	stats, err := b.cat.Statistics(b.ctx)
	if err != nil {
		b.handleErr(err)
	}

	outScope.node = analyze.WithDb(defaultDb).WithStats(stats)
	return
}
