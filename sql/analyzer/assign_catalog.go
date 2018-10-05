package analyzer

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

// assignCatalog sets the catalog in the required nodes.
func assignCatalog(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	span, ctx := ctx.Span("assign_catalog")
	defer span.Finish()

	return n.TransformUp(func(n sql.Node) (sql.Node, error) {
		if !n.Resolved() {
			return n, nil
		}

		switch node := n.(type) {
		case *plan.CreateIndex:
			nc := *node
			nc.Catalog = a.Catalog
			nc.CurrentDatabase = a.Catalog.CurrentDatabase()
			return &nc, nil
		case *plan.DropIndex:
			nc := *node
			nc.Catalog = a.Catalog
			nc.CurrentDatabase = a.Catalog.CurrentDatabase()
			return &nc, nil
		case *plan.ShowIndexes:
			nc := *node
			nc.Registry = a.Catalog.IndexRegistry
			return &nc, nil
		case *plan.ShowDatabases:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, nil
		case *plan.ShowProcessList:
			nc := *node
			nc.Database = a.Catalog.CurrentDatabase()
			nc.ProcessList = a.Catalog.ProcessList
			return &nc, nil
		case *plan.ShowTableStatus:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, nil
		case *plan.Use:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, nil
		default:
			return n, nil
		}
	})
}
