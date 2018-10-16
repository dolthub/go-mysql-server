package analyzer

import (
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

const dualTableName = "dual"

var dualTable = func() sql.Table {
	t := mem.NewTable(dualTableName, sql.Schema{
		{Name: "dummy", Source: dualTableName, Type: sql.Text, Nullable: false},
	})
	_ = t.Insert(sql.NewEmptyContext(), sql.NewRow("x"))
	return t
}()

func resolveTables(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_tables")
	defer span.Finish()

	a.Log("resolve table, node of type: %T", n)
	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
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
		if err != nil {
			if sql.ErrTableNotFound.Is(err) && name == dualTableName {
				rt = dualTable
				name = dualTableName
			} else {
				return nil, err
			}
		}

		a.Log("table resolved: %q", t.Name())

		return plan.NewResolvedTable(rt), nil
	})
}
