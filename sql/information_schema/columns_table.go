// Copyright 2022 Dolthub, Inc.
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

package information_schema

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// ColumnsNode wraps the information_schema.columns table as a way to resolve column defaults.
type ColumnsNode struct {
	plan.UnaryNode
	columnNameToColumn map[string]*sql.Column // databaseName.tableName.columnName -> sql.Column
	defaultToColumn    map[*sql.ColumnDefaultValue]*sql.Column
}

var _ sql.Node = (*ColumnsNode)(nil)
var _ sql.Expressioner = (*ColumnsNode)(nil)
var _ sql.Nameable = (*ColumnsNode)(nil)

// CreateNewColumnsNode returns a new ColumnsNode.
func CreateNewColumnsNode(child sql.Node, tableToColumnsWithDefaultValue map[string]*sql.Column) *ColumnsNode {
	return &ColumnsNode{
		UnaryNode:          plan.UnaryNode{Child: child},
		columnNameToColumn: tableToColumnsWithDefaultValue,
	}
}

// Resolved implements the sql.Node interface.
func (c *ColumnsNode) Resolved() bool {
	return c.Child.Resolved() && c.defaultToColumn != nil
}

// String implements the sql.Node interface.
func (c *ColumnsNode) String() string {
	return fmt.Sprintf("ColumnsNode(%s)", c.Child.String())
}

// Schema implements the sql.Node interface.
func (c *ColumnsNode) Schema() sql.Schema {
	return c.Child.Schema()
}

// RowIter implements the sql.Node interface.
func (c *ColumnsNode) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	ct := getColumnsTable(c)

	colToDefaults := make(map[string]*sql.ColumnDefaultValue)
	for colName, col := range c.columnNameToColumn {
		colToDefaults[colName] = col.Default
	}

	ct.WithTableToDefaultMap(colToDefaults)

	return c.Child.RowIter(ctx, row)
}

// WithChildren implements the sql.Node interface.
func (c *ColumnsNode) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}

	nc := *c
	nc.Child = children[0]
	return &nc, nil
}

// CheckPrivileges implements the sql.Node interface.
func (c *ColumnsNode) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return c.Child.CheckPrivileges(ctx, opChecker)
}

// Expressions implements the sql.Expressioner interface.
func (c *ColumnsNode) Expressions() []sql.Expression {
	c.defaultToColumn = make(map[*sql.ColumnDefaultValue]*sql.Column)
	toResolvedColumnDefaults := make([]sql.Expression, 0)

	for _, col := range c.columnNameToColumn {
		toResolvedColumnDefaults = append(toResolvedColumnDefaults, expression.WrapExpression(col.Default))
		c.defaultToColumn[col.Default] = col
	}

	return toResolvedColumnDefaults
}

func (c *ColumnsNode) WithExpressions(expressions ...sql.Expression) (sql.Node, error) {
	// TODO: This is super hacky assumes order is the same....
	i := 0
	for _, col := range c.columnNameToColumn {
		expr := expressions[i].(*expression.Wrapper)

		col.Default = expr.Unwrap().(*sql.ColumnDefaultValue)
		c.defaultToColumn[col.Default] = col
		i++
	}

	return c, nil
}

func (c *ColumnsNode) GetColumnFromDefaultValue(d *sql.ColumnDefaultValue) (*sql.Column, bool) {
	col, ok := c.defaultToColumn[d]
	return col, ok
}

// Name implements the sql.Nameable interface.
func (c *ColumnsNode) Name() string {
	rt := getColumnsTable(c)
	return rt.name
}

// Finds first ResolvedTable node that is a descendant of the node given
func getColumnsTable(node sql.Node) *ColumnsTable {
	var table *plan.ResolvedTable
	transform.Inspect(node, func(node sql.Node) bool {
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

	return getInnerTable(table.Table)
}

func getInnerTable(t sql.Table) *ColumnsTable {
	switch tt := t.(type) {
	case *plan.ProcessTable:
		return getInnerTable(tt.Table)
	case *ColumnsTable:
		return tt
	default:
		return nil
	}
}

type ColumnsTable struct {
	name           string
	schema         sql.Schema
	rowIter        func(*sql.Context, sql.Catalog, map[string]*sql.ColumnDefaultValue) (sql.RowIter, error)
	Catalog        sql.Catalog
	tableToDefault map[string]*sql.ColumnDefaultValue
}

var _ sql.Table = (*ColumnsTable)(nil)

// String implements the sql.Table interface.
func (c *ColumnsTable) String() string {
	return c.name
}

// Schema implements the sql.Table interface.
func (c *ColumnsTable) Schema() sql.Schema {
	return c.schema
}

// Name implements the sql.Table interface.
func (c *ColumnsTable) Name() string {
	return c.name
}

// Partitions implements the sql.Table interface.
func (c *ColumnsTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	return &informationSchemaPartitionIter{informationSchemaPartition: informationSchemaPartition{partitionKey(c.Name())}}, nil
}

// PartitionRows implements the sql.Table interface.
func (c *ColumnsTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if !bytes.Equal(partition.Key(), partitionKey(c.Name())) {
		return nil, sql.ErrPartitionNotFound.New(partition.Key())
	}
	if c.rowIter == nil {
		return sql.RowsToRowIter(), nil
	}
	if c.Catalog == nil {
		return nil, fmt.Errorf("nil catalog for info schema table %s", c.name)
	}

	return c.rowIter(context, c.Catalog, c.tableToDefault)
}

// AssignCatalog implements the analyzer.Catalog interface.
func (c *ColumnsTable) AssignCatalog(cat sql.Catalog) sql.Table {
	c.Catalog = cat
	return c
}

func (c *ColumnsTable) WithTableToDefaultMap(tableToDefault map[string]*sql.ColumnDefaultValue) {
	c.tableToDefault = tableToDefault
}

func columnsRowIter(ctx *sql.Context, cat sql.Catalog, tableToDefault map[string]*sql.ColumnDefaultValue) (sql.RowIter, error) {
	var rows []sql.Row
	for _, db := range cat.AllDatabases(ctx) {
		// Get all Tables
		err := sql.DBTableIter(ctx, db, func(t sql.Table) (cont bool, err error) {
			for i, c := range t.Schema() {
				var (
					nullable   string
					charName   interface{}
					collName   interface{}
					ordinalPos uint64
					colDefault interface{}
				)
				if c.Nullable {
					nullable = "YES"
				} else {
					nullable = "NO"
				}
				if sql.IsText(c.Type) {
					charName = sql.Collation_Default.CharacterSet().String()
					collName = sql.Collation_Default.String()
				}
				ordinalPos = uint64(i + 1)

				// TODO: Clean this shit up
				key := db.Name() + "." + t.Name() + "." + c.Name
				colDefault, ok := tableToDefault[key]
				if !ok {
					colDefault = nil
				} else {
					colDefault = colDefault.(*sql.ColumnDefaultValue).String()
					if strings.HasPrefix(colDefault.(string), "\"") && strings.HasSuffix(colDefault.(string), "\"") {
						colDefault = strings.TrimSuffix(strings.TrimPrefix(colDefault.(string), "\""), "\"")
					} else if colDefault == "NULL" {
						colDefault = nil
					}
				}

				rows = append(rows, sql.Row{
					"def",                            // table_catalog
					db.Name(),                        // table_schema
					t.Name(),                         // table_name
					c.Name,                           // column_name
					ordinalPos,                       // ordinal_position
					colDefault,                       // column_default
					nullable,                         // is_nullable
					strings.ToLower(c.Type.String()), // data_type
					nil,                              // character_maximum_length
					nil,                              // character_octet_length
					nil,                              // numeric_precision
					nil,                              // numeric_scale
					nil,                              // datetime_precision
					charName,                         // character_set_name
					collName,                         // collation_name
					strings.ToLower(c.Type.String()), // column_type
					"",                               // column_key
					c.Extra,                          // extra
					"select",                         // privileges
					c.Comment,                        // column_comment
					"",                               // generation_expression
				})
			}
			return true, nil
		})

		// TODO: View Definition is lacking information to properly fill out these table
		// TODO: Should somehow get reference to table(s) view is referencing
		// TODO: Each column that view references should also show up as unique entries as well
		views, err := viewsInDatabase(ctx, db)
		if err != nil {
			return nil, err
		}

		for _, view := range views {
			rows = append(rows, sql.Row{
				"def",     // table_catalog
				db.Name(), // table_schema
				view.Name, // table_name
				"",        // column_name
				uint64(0), // ordinal_position
				nil,       // column_default
				nil,       // is_nullable
				nil,       // data_type
				nil,       // character_maximum_length
				nil,       // character_octet_length
				nil,       // numeric_precision
				nil,       // numeric_scale
				nil,       // datetime_precision
				"",        // character_set_name
				"",        // collation_name
				"",        // column_type
				"",        // column_key
				"",        // extra
				"select",  // privileges
				"",        // column_comment
				"",        // generation_expression
			})
		}
		if err != nil {
			return nil, err
		}
	}
	return sql.RowsToRowIter(rows...), nil
}
