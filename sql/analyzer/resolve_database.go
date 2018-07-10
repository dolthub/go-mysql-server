package analyzer

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func resolveDatabase(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("resolve_database")
	defer span.Finish()

	a.Log("resolve database, node of type: %T", n)

	// TODO Database should implement node,
	// and ShowTables and CreateTable nodes should be binaryNodes
	switch v := n.(type) {
	case *plan.ShowTables:
		db, err := a.Catalog.Database(a.CurrentDatabase)
		if err != nil {
			return n, err
		}

		v.Database = db
	case *plan.CreateTable:
		db, err := a.Catalog.Database(a.CurrentDatabase)
		if err != nil {
			return n, err
		}

		v.Database = db
	}

	return n, nil
}
