package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// resolveDatabase sets a database for nodes that implement sql.Databaser. Replaces sql.UnresolvedDatabase with the
// actual sql.Database implementation from the catalog.
func resolveDatabase(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("resolve_database")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		d, ok := n.(sql.Databaser)
		if !ok {
			return n, nil
		}

		var dbName = ctx.GetCurrentDatabase()
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
