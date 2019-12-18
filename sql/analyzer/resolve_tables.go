package analyzer

import (
	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/plan"
)

const dualTableName = "dual"

var dualTable = func() sql.Table {
	t := memory.NewTable(dualTableName, sql.Schema{
		{Name: "dummy", Source: dualTableName, Type: sql.LongText, Nullable: false},
	})
	_ = t.Insert(sql.NewEmptyContext(), sql.NewRow("x"))
	return t
}()

func resolveTables(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, _ := ctx.Span("resolve_tables")
	defer span.Finish()

	a.Log("resolve table, node of type: %T", n)
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		a.Log("transforming node of type: %T", n)
		if n.Resolved() {
			return n, nil
		}

		t, ok := n.(*plan.UnresolvedTable)
		if !ok {
			return n, nil
		}

		name := t.Name()
		db := t.Database
		if db == "" {
			db = a.Catalog.CurrentDatabase()
		}

		rt, err := a.Catalog.Table(db, name)
		if err == nil {
			a.Log("table resolved: %q", t.Name())
			return plan.NewResolvedTable(rt), nil
		}

		if sql.ErrTableNotFound.Is(err) {
			if name == dualTableName {
				rt = dualTable
				name = dualTableName

				a.Log("table resolved: %q", t.Name())
				return plan.NewResolvedTable(rt), nil
			}
		}

		return nil, err
	})
}
