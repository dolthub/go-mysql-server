package analyzer

import (
	"github.com/src-d/go-mysql-server/sql"
)

func resolveDatabase(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, _ := ctx.Span("resolve_database")
	defer span.Finish()

	a.Log("resolve database, node of type: %T", n)

	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		d, ok := n.(sql.Databaser)
		if !ok {
			return n, nil
		}

		var dbName = a.Catalog.CurrentDatabase()
		if db := d.Database(); db != nil {
			if _, ok := db.(sql.UnresolvedDatabase); !ok {
				return n, nil
			}

			if db.Name() != "" {
				dbName = db.Name()
			}
		}

		db, err := a.Catalog.Database(dbName)
		if err != nil {
			return nil, err
		}

		return d.WithDatabase(db)
	})
}
