package planbuilder

import (
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *Builder) buildLoad(inScope *scope, d *ast.Load) (outScope *scope) {
	dbName := strings.ToLower(d.Table.Qualifier.String())
	if dbName == "" {
		dbName = b.ctx.GetCurrentDatabase()
	}
	database := b.resolveDb(dbName)
	table := b.resolveTable(d.Table.Name.String(), database.Name(), nil)

	var ignoreNumVal int64 = 0
	if d.IgnoreNum != nil {
		ignoreNumVal = b.getInt64Value(inScope, d.IgnoreNum, "Cannot parse ignore Value")
	}

	ld := plan.NewLoadData(bool(d.Local), d.Infile, table, columnsToStrings(d.Columns), d.Fields, d.Lines, ignoreNumVal, d.IgnoreOrReplace)

	outScope = inScope.push()
	outScope.node = plan.NewInsertInto(database, table, ld, ld.IsReplace, ld.ColumnNames, nil, ld.IsIgnore)
	return outScope
}
