package analyzer

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func resolveDatabase(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, _ := ctx.Span("resolve_database")
	defer span.Finish()

	a.Log("resolve database, node of type: %T", n)

	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		switch v := n.(type) {
		case *plan.ShowIndexes:
			db, err := a.Catalog.Database(a.Catalog.CurrentDatabase())
			if err != nil {
				return nil, err
			}

			nc := *v
			nc.Database = db
			return &nc, nil
		case *plan.ShowTables:
			var dbName = v.Database.Name()
			if dbName == "" {
				dbName = a.Catalog.CurrentDatabase()
			}

			db, err := a.Catalog.Database(dbName)
			if err != nil {
				return nil, err
			}

			nc := *v
			nc.Database = db
			return &nc, nil
		case *plan.CreateTable:
			db, err := a.Catalog.Database(a.Catalog.CurrentDatabase())
			if err != nil {
				return nil, err
			}

			nc := *v
			nc.Database = db
			return &nc, nil
		case *plan.Use:
			db, err := a.Catalog.Database(v.Database.Name())
			if err != nil {
				return nil, err
			}

			nc := *v
			nc.Database = db
			return &nc, nil
		case *plan.ShowCreateDatabase:
			db, err := a.Catalog.Database(v.Database.Name())
			if err != nil {
				return nil, err
			}

			nc := *v
			nc.Database = db
			return &nc, nil
		default:
			return n, nil
		}
	})
}
