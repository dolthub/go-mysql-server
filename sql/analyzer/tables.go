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
	"strings"

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
		case *plan.IndexedTableAccess:
			tableName = node.Name()
			return false
		}
		return true
	})

	return tableName
}

type NameableNode interface {
	sql.Nameable
	sql.Node
}

// getTables returns all tables in the node given
func getTables(node sql.Node) []NameableNode {
	var tables []NameableNode
	plan.Inspect(node, func(node sql.Node) bool {
		switch node := node.(type) {
		case *plan.TableAlias:
			tables = append(tables, node)
			return false
		case *plan.ResolvedTable:
			tables = append(tables, node)
			return false
		case *plan.UnresolvedTable:
			tables = append(tables, node)
			return false
		case *plan.IndexedTableAccess:
			tables = append(tables, node)
			return false
		}
		return true
	})

	return tables
}

// byLowerCaseName returns all the nodes given mapped by their lowercase name.
func byLowerCaseName(nodes []NameableNode) map[string]NameableNode {
	byName := make(map[string]NameableNode)
	for _, n := range nodes {
		byName[strings.ToLower(n.Name())] = n
	}
	return byName
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
		case *plan.IndexedTableAccess:
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
		case *plan.IndexedTableAccess:
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
		// plan.Inspect will get called on all children of a node even if one of the children's calls returns false. We
		// only want the first ResolvedTable match.
		if table != nil {
			return false
		}

		switch n := node.(type) {
		case *plan.ResolvedTable:
			table = n
			return false
		case *plan.IndexedTableAccess:
			table = n.ResolvedTable
			return false
		}
		return true
	})
	return table
}

// Returns the tables used in the expressions given
func findTables(exprs ...sql.Expression) []string {
	tables := make(map[string]bool)
	for _, e := range exprs {
		sql.Inspect(e, func(e sql.Expression) bool {
			switch e := e.(type) {
			case *expression.GetField:
				tables[e.Table()] = true
				return false
			default:
				return true
			}
		})
	}

	var names []string
	for table := range tables {
		names = append(names, table)
	}

	return names
}

// Transforms the node given bottom up by setting resolve tables to reference the table given. Returns an error if more
// than one table was set in this way.
func withTable(node sql.Node, table sql.Table) (sql.Node, error) {
	foundTable := false
	return plan.TransformUp(node, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			if foundTable {
				return nil, ErrInAnalysis.New("attempted to set more than one table in withTable()")
			}
			foundTable = true
			return n.WithTable(table)
		case *plan.IndexedTableAccess:
			if foundTable {
				return nil, ErrInAnalysis.New("attempted to set more than one table in withTable()")
			}
			foundTable = true
			newRt, err := n.WithTable(table)
			if err != nil {
				return nil, err
			}
			n2 := *n
			n2.ResolvedTable = newRt
			return &n2, nil
		default:
			return n, nil
		}
	})
}

type fieldsByTable map[string][]string

// add adds the table and field given if not already present
func (f fieldsByTable) add(table, field string) {
	if !stringContains(f[table], field) {
		f[table] = append(f[table], field)
	}
}

// addAll adds the tables and fields given if not already present
func (f fieldsByTable) addAll(f2 fieldsByTable) {
	for table, fields := range f2 {
		for _, field := range fields {
			f.add(table, field)
		}
	}
}

// getFieldsByTable returns a map of table name to set of field names in the node provided
func getFieldsByTable(ctx *sql.Context, n sql.Node) fieldsByTable {
	colSpan, _ := ctx.Span("getFieldsByTable")
	defer colSpan.Finish()

	var fieldsByTable = make(fieldsByTable)
	plan.InspectExpressionsWithNode(n, func(n sql.Node, e sql.Expression) bool {
		if gf, ok := e.(*expression.GetField); ok {
			fieldsByTable.add(gf.Table(), gf.Name())
		}
		if s, ok := e.(*plan.Subquery); ok {
			fieldsByTable.addAll(getFieldsByTable(ctx, s.Query))
		}
		return true
	})
	return fieldsByTable
}
