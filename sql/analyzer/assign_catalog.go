// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// CatalogTable is a Table that depends on a Catalog.
type CatalogTable interface {
	sql.Table

	// AssignCatalog assigns a Catalog to the table.
	AssignCatalog(cat sql.Catalog) sql.Table
}

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
			return &nc, nil
		case *plan.ShowTableStatus:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, nil
		case *plan.Use:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, nil
		case *plan.CreateDB:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, nil
		case *plan.DropDB:
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
		case *plan.ResolvedTable:
			nc := *node
			ct, ok := nc.Table.(CatalogTable)
			if ok {
				nc.Table = ct.AssignCatalog(a.Catalog)
			}
			return &nc, nil
		default:
			return n, nil
		}
	})
}
