// Copyright 2020-2022 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// resolveDynamicTables wraps information_schema tables with custom nodes that need use to the analyzer.
func resolveDynamicTables(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, transform.TreeIdentity, error) {
	span, _ := ctx.Span("resolveDynamicTables")
	defer span.Finish()

	return resolveInfoSchemaColumnTable(ctx, a.Catalog, n)
}

// resolveInfoSchemaColumnTable looks for the information_schema.Column table and wraps with an information_schema.ColumnsNode
// if found.
// TODO: Should we should hard code this in resolve_tables? Would be much simpler
func resolveInfoSchemaColumnTable(ctx *sql.Context, catalog sql.Catalog, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
	canSelectResolvedTable := func(c transform.Context) bool {
		switch c.Node.(type) {
		case *plan.ResolvedTable:
			// Once the information_schema.ColumnsNode wraps the information_schema.columns (resolved table) we don't
			// need to keep searching.
			if _, ok := c.Parent.(*information_schema.ColumnsNode); ok {
				return false
			}

			return true
		default:
			return true
		}
	}

	return transform.NodeWithCtx(node, canSelectResolvedTable, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch n := c.Node.(type) {
		case *plan.ResolvedTable:
			// Doing it twice
			_, ok := n.Table.(*information_schema.ColumnsTable)
			if !ok {
				return n, transform.SameTree, nil
			}

			tableColumnsToDefaultValue, err := getAllColumnsWithADefaultValue(ctx, catalog)
			if err != nil {
				return nil, transform.SameTree, err
			}

			return information_schema.CreateNewColumnsNode(n, tableColumnsToDefaultValue), transform.NewTree, nil
		default:
			return n, transform.SameTree, nil
		}
	})
}

// getAllColumnDefaults returns a map of tableName.Column to default column value.
func getAllColumnsWithADefaultValue(ctx *sql.Context, catalog sql.Catalog) (map[string]*sql.Column, error) {
	ret := make(map[string]*sql.Column)

	for _, db := range catalog.AllDatabases(ctx) {
		err := sql.DBTableIter(ctx, db, func(t sql.Table) (cont bool, err error) {
			for _, col := range t.Schema() {
				if col.Default == nil {
					continue
				}

				if ucd, ok := col.Default.Expression.(sql.UnresolvedColumnDefault); ok {
					newDefault, err := parse.StringToColumnDefaultValue(ctx, ucd.String())
					if err != nil {
						return false, err
					}

					col.Default = newDefault
				}

				key := db.Name() + "." + t.Name() + "." + col.Name
				ret[key] = col
			}

			return false, nil
		})

		if err != nil {
			return nil, err
		}
	}

	return ret, nil
}
