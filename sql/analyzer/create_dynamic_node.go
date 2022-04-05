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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// resolveDynamicTables creates custom analyzer nodes for information schema tables that need use the analyzer
func resolveDynamicTables(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, transform.TreeIdentity, error) {
	span, _ := ctx.Span("resolveDynamicTables")
	defer span.Finish()

	canSelectResolvedTable := func(c transform.Context) bool {
		switch c.Node.(type) {
		case *plan.ResolvedTable:
			// Don't want to transform already transformed node
			if _, ok := c.Parent.(*information_schema.ColumnsNode); ok {
				return false
			}

			return true
		default:
			return false
		}
	}

	return transform.NodeWithCtx(n, canSelectResolvedTable, func(c transform.Context) (sql.Node, transform.TreeIdentity, error) {
		switch node := c.Node.(type) {
		case *plan.ResolvedTable:
			// Doing it twice
			_, ok := node.Table.(*information_schema.ColumnsTable)
			if !ok {
				return node, transform.SameTree, nil
			}

			return information_schema.CreateNewColumnsNode(node, a.Catalog, ctx), transform.NewTree, nil
		default:
			return node, transform.SameTree, nil
		}
	})
}
