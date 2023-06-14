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
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// assignCatalog sets the catalog in the required nodes.
func assignCatalog(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("assign_catalog")
	defer span.End()

	// TODO make the catalog interfaces change sensitive
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if !n.Resolved() {
			return n, transform.SameTree, nil
		}

		switch node := n.(type) {
		case *plan.CreateIndex:
			nc := *node
			nc.Catalog = a.Catalog
			nc.CurrentDatabase = ctx.GetCurrentDatabase()
			return &nc, transform.NewTree, nil
		case *plan.DropIndex:
			nc := *node
			nc.Catalog = a.Catalog
			nc.CurrentDatabase = ctx.GetCurrentDatabase()
			return &nc, transform.NewTree, nil
		case *plan.ShowDatabases:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, transform.NewTree, nil
		case *plan.ShowProcessList:
			nc := *node
			nc.Database = ctx.GetCurrentDatabase()
			return &nc, transform.NewTree, nil
		case *plan.ShowTableStatus:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, transform.NewTree, nil
		case *plan.Use:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, transform.NewTree, nil
		case *plan.CreateDB:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, transform.NewTree, nil
		case *plan.AlterDB:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, transform.NewTree, nil
		case *plan.DropDB:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, transform.NewTree, nil
		case *plan.LockTables:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, transform.NewTree, nil
		case *plan.UnlockTables:
			nc := *node
			nc.Catalog = a.Catalog
			return &nc, transform.NewTree, nil
		case *plan.ResolvedTable:
			ct, ok := node.Table.(sql.CatalogTable)
			if ok {
				nc := *node
				nc.Table = ct.AssignCatalog(a.Catalog)
				return &nc, transform.NewTree, nil
			}
			return node, transform.SameTree, nil
		default:
			return n, transform.SameTree, nil
		}
	})
}

// resolveAnalyzeTables verifies analyze node target tables exist, and provides
// access to the statistics table.
func resolveAnalyzeTables(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("resolve_analyze_tables")
	defer span.End()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if n.Resolved() {
			return n, transform.SameTree, nil
		}

		at, ok := n.(*plan.AnalyzeTable)
		if !ok {
			return n, transform.SameTree, nil
		}

		if at.Stats != nil {
			return n, transform.SameTree, nil
		}

		db := ctx.GetCurrentDatabase()
		newTables := make([]sql.DbTable, len(at.Tables))
		for i, t := range at.Tables {
			if t.Db == "" {
				if db == "" {
					return n, transform.SameTree, sql.ErrNoDatabaseSelected.New()
				}
				t.Db = db
			}
			_, _, err := a.Catalog.Table(ctx, t.Db, t.Table)
			if err != nil {
				return n, transform.SameTree, sql.ErrTableNotFound.New(t.Table)
			}
			newTables[i] = t
		}

		stats, err := a.Catalog.Statistics(ctx)
		if err != nil {
			return n, transform.SameTree, err
		}

		return at.WithDb(db).WithTables(newTables).WithStats(stats), transform.NewTree, nil
	})
}
