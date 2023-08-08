package planbuilder

import (
	"fmt"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *Builder) buildLoad(inScope *scope, d *ast.Load) (outScope *scope) {
	dbName := strings.ToLower(d.Table.Qualifier.String())
	if dbName == "" {
		dbName = b.ctx.GetCurrentDatabase()
	}
	//table := b.resolveTable(d.Table.Name.String(), database.Name(), nil)

	tableName := strings.ToLower(d.Table.Name.String())
	destScope, ok := b.buildTablescan(inScope, dbName, tableName, nil)
	if !ok {
		b.handleErr(sql.ErrTableNotFound.New(tableName))
	}
	var db sql.Database
	var rt *plan.ResolvedTable
	switch n := destScope.node.(type) {
	case *plan.ResolvedTable:
		rt = n
		db = rt.Database
	case *plan.UnresolvedTable:
		db = n.Database()
	default:
		b.handleErr(fmt.Errorf("expected insert destination to be resolved or unresolved table"))
	}
	if rt == nil {
		if b.TriggerCtx().Active && !b.TriggerCtx().Call {
			b.TriggerCtx().UnresolvedTables = append(b.TriggerCtx().UnresolvedTables, tableName)
		} else {
			err := fmt.Errorf("expected resolved table: %s", tableName)
			b.handleErr(err)
		}
	}

	var ignoreNumVal int64 = 0
	if d.IgnoreNum != nil {
		ignoreNumVal = b.getInt64Value(inScope, d.IgnoreNum, "Cannot parse ignore Value")
	}

	ld := plan.NewLoadData(bool(d.Local), d.Infile, destScope.node, columnsToStrings(d.Columns), d.Fields, d.Lines, ignoreNumVal, d.IgnoreOrReplace)

	outScope = inScope.push()
	ins := plan.NewInsertInto(db, destScope.node, ld, ld.IsReplace, ld.ColumnNames, nil, ld.IsIgnore)

	if rt != nil {
		checks := b.loadChecksFromTable(destScope, rt.Table)
		ins.Checks = checks
	}

	outScope.node = ins

	return outScope
}
