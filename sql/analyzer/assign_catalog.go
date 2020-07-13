package analyzer

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

// assignCatalog sets the catalog in the required nodes.
func assignCatalog(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("assign_catalog")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if !n.Resolved() {
			return n, nil
		}

		switch node := n.(type) {
		case *plan.CreateIndex:
			nc := *node
			nc.Catalog = a.Catalog
			nc.CurrentDatabase = ctx.GetCurrentDatabase()
			return &nc, nil
		case *plan.DropIndex:
			nc := *node
			nc.Catalog = a.Catalog
			nc.CurrentDatabase = ctx.GetCurrentDatabase()
			return &nc, nil
		case *plan.ShowDatabases:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, nil
		case *plan.ShowProcessList:
			nc := *node
			nc.Database = ctx.GetCurrentDatabase()
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
		case *plan.LockTables:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, nil
		case *plan.UnlockTables:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, nil
		case *plan.CreateView:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, nil
		case *plan.DropView:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, nil
		default:
			return n, nil
		}
	})
}
