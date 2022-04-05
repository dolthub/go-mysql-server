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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"strings"
)

type ColumnsNode struct {
	plan.UnaryNode
	Catalog sql.Catalog
	ctx     *sql.Context
}

var _ sql.Node = (*ColumnsNode)(nil)
var _ sql.Expressioner = (*ColumnsNode)(nil)

func CreateNewColumnsNode(child sql.Node, catalog sql.Catalog, ctx *sql.Context) *ColumnsNode {
	return &ColumnsNode{
		UnaryNode: plan.UnaryNode{Child: child},
		Catalog:   catalog,
		ctx:       ctx,
	}
}

func (c ColumnsNode) Resolved() bool {
	ct := getColumnsTable(c.Child)
	rt := ct.tableToDefault != nil
	if rt {
		return rt
	}

	return false
}

func (c ColumnsNode) String() string {
	return fmt.Sprintf("ColumnsNode(%s)", c.Child.String())
}

func (c ColumnsNode) Schema() sql.Schema {
	return c.Child.Schema()
}

func (c ColumnsNode) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return c.Child.RowIter(ctx, row)
}

func (c ColumnsNode) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}

	return CreateNewColumnsNode(children[0], c.Catalog, c.ctx), nil
}

func (c ColumnsNode) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return c.Child.CheckPrivileges(ctx, opChecker)
}

func (c ColumnsNode) Expressions() []sql.Expression {
	ct := getColumnsTable(c.Child)

	if c.Catalog == nil {
		return nil
	}

	toResolvedColumnDefaults := make([]sql.Expression, 0)
	for _, db := range c.Catalog.AllDatabases(c.ctx) {
		err := sql.DBTableIter(c.ctx, db, func(t sql.Table) (cont bool, err error) {
			for _, col := range t.Schema() {
				toResolvedColumnDefaults = append(toResolvedColumnDefaults, expression.WrapExpression(col.Default))
			}

			return false, nil
		})

		if err != nil {
			panic("dasasas")
		}
	}

	ct.tableToDefault = make(map[string]*sql.ColumnDefaultValue)

	return toResolvedColumnDefaults
}

func (c ColumnsNode) WithExpressions(expressions ...sql.Expression) (sql.Node, error) {
	ct := getColumnsTable(c.Child)

	if ct.tableToDefault != nil {
		return c, nil
	}

	ct.tableToDefault = make(map[string]*sql.ColumnDefaultValue)

	// TODO: This is super hacky assumes order is the same....
	i := 0
	ctx := sql.NewEmptyContext()
	for _, db := range ct.Catalog.AllDatabases(ctx) {
		err := sql.DBTableIter(ctx, db, func(t sql.Table) (cont bool, err error) {
			for _, col := range t.Schema() {
				key := t.Name() + col.Name
				ct.tableToDefault[key] = expressions[i].(*expression.Wrapper).Unwrap().(*sql.ColumnDefaultValue)
				i += 1
			}

			return false, nil
		})

		if err != nil {
			panic("dasasas")
		}
	}

	return c, nil
}

func getColumnsTable(n sql.Node) *ColumnsTable {
	var ct *ColumnsTable

	transform.Inspect(n, func(n sql.Node) bool {
		switch node := n.(type) {
		case *plan.ResolvedTable:
			switch t := node.Table.(type) {
			case *plan.ProcessTable:
				if cte, ok := t.Table.(*ColumnsTable); ok {
					ct = cte
				}
			case *ColumnsTable:
				ct = t
			}
			return false
		default:
			return true
		}
	})

	return ct
}

// Probably not the right solution
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
				key := db.Name() + t.Name()
				colDefault = tableToDefault[key]

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
