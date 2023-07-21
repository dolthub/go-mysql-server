package planbuilder

import (
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *Builder) buildLoad(inScope *scope, d *ast.Load) (outScope *scope) {
	db := b.currentDatabase.Name()
	if d.Table.Qualifier.String() != "" {
		db = d.Table.Qualifier.String()
	}
	table := b.resolveTable(d.Table.Name.String(), db, nil)

	var ignoreNumVal int64 = 0
	if d.IgnoreNum != nil {
		ignoreNumVal = b.getInt64Value(inScope, d.IgnoreNum, "Cannot parse ignore Value")
	}

	ld := plan.NewLoadData(bool(d.Local), d.Infile, table, columnsToStrings(d.Columns), d.Fields, d.Lines, ignoreNumVal, d.IgnoreOrReplace)

	outScope = inScope.push()
	outScope.node = plan.NewInsertInto(sql.UnresolvedDatabase(d.Table.Qualifier.String()), table, ld, ld.IsReplace, ld.ColumnNames, nil, ld.IsIgnore)
	return outScope
}
