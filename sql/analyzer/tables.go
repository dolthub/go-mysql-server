// Copyright 2020 Liquidata, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// Returns the underlying table name for the node given
func getTableName(node sql.Node) string {
	var tableName string
	plan.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.TableAlias:
			tableName = node.Name()
			return false
		case *plan.ResolvedTable:
			tableName = node.Name()
			return false
		case *plan.UnresolvedTable:
			tableName = node.Name()
			return false
		}
		return true
	})

	return tableName
}

// Returns the underlying table name for the node given, ignoring table aliases
func getUnaliasedTableName(node sql.Node) string {
	var tableName string
	plan.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.ResolvedTable:
			tableName = node.Name()
			return false
		case *plan.UnresolvedTable:
			tableName = node.Name()
			return false
		}
		return true
	})

	return tableName
}

// Finds first table node that is a descendant of the node given
func getTable(node sql.Node) sql.Table {
	var table sql.Table
	plan.Inspect(node, func(node sql.Node) bool {
		switch n := node.(type) {
		case *plan.ResolvedTable:
			table = n.Table
			return false
		}
		return true
	})
	return table
}

// Finds first ResolvedTable node that is a descendant of the node given
func getResolvedTable(node sql.Node) *plan.ResolvedTable {
	var table *plan.ResolvedTable
	plan.Inspect(node, func(node sql.Node) bool {
		switch n := node.(type) {
		case *plan.ResolvedTable:
			table = n
			return false
		}
		return true
	})
	return table
}

// Returns the tables used in the expression given
func findTables(e sql.Expression) []string {
	tables := make(map[string]bool)
	sql.Inspect(e, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.GetField:
			tables[e.Table()] = true
			return false
		default:
			return true
		}
	})

	var names []string
	for table := range tables {
		names = append(names, table)
	}

	return names
}
