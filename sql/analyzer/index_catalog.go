package analyzer

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

// indexCatalog sets the catalog in the CreateIndexm, DropIndex and ShowIndexes nodes.
func indexCatalog(ctx *sql.Context, a *Analyzer, n sql.Node) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}

	span, ctx := ctx.Span("index_catalog")
	defer span.Finish()

	switch node := n.(type) {
	case *plan.CreateIndex:
		nc := *node
		nc.Catalog = a.Catalog
		nc.CurrentDatabase = a.CurrentDatabase
		return &nc, nil
	case *plan.DropIndex:
		nc := *node
		nc.Catalog = a.Catalog
		nc.CurrentDatabase = a.CurrentDatabase
		return &nc, nil
	case *plan.ShowIndexes:
		node.Registry = a.Catalog.IndexRegistry
		return node, nil
	default:
		return n, nil
	}
}
