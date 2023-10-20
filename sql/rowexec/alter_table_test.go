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

package rowexec

import (
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestAddColumnToSchema(t *testing.T) {
	myTable := sql.Schema{
		{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
	}

	type testCase struct {
		name        string
		schema      sql.Schema
		newColumn   *sql.Column
		order       *sql.ColumnOrder
		newSchema   sql.Schema
		projections []sql.Expression
	}

	varchar20 := types.MustCreateStringWithDefaults(sqltypes.VarChar, 20)
	testCases := []testCase{
		{
			name:      "add at end",
			schema:    myTable,
			newColumn: &sql.Column{Name: "i2", Type: types.Int64, Source: "mytable"},
			newSchema: sql.Schema{
				{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
				{Name: "i2", Type: types.Int64, Source: "mytable"},
			},
			projections: []sql.Expression{
				expression.NewGetField(0, types.Int64, "i", false),
				expression.NewGetField(1, varchar20, "s", false),
				plan.ColDefaultExpression{&sql.Column{Name: "i2", Type: types.Int64, Source: "mytable"}},
			},
		},
		{
			name:      "add at end, with 'after'",
			schema:    myTable,
			newColumn: &sql.Column{Name: "i2", Type: types.Int64, Source: "mytable"},
			order:     &sql.ColumnOrder{AfterColumn: "s"},
			newSchema: sql.Schema{
				{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
				{Name: "i2", Type: types.Int64, Source: "mytable"},
			},
			projections: []sql.Expression{
				expression.NewGetField(0, types.Int64, "i", false),
				expression.NewGetField(1, varchar20, "s", false),
				plan.ColDefaultExpression{&sql.Column{Name: "i2", Type: types.Int64, Source: "mytable"}},
			},
		},
		{
			name:      "add at beginning",
			schema:    myTable,
			newColumn: &sql.Column{Name: "i2", Type: types.Int64, Source: "mytable"},
			order:     &sql.ColumnOrder{First: true},
			newSchema: sql.Schema{
				{Name: "i2", Type: types.Int64, Source: "mytable"},
				{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
			},
			projections: []sql.Expression{
				plan.ColDefaultExpression{&sql.Column{Name: "i2", Type: types.Int64, Source: "mytable"}},
				expression.NewGetField(0, types.Int64, "i", false),
				expression.NewGetField(1, varchar20, "s", false),
			},
		},
		{
			name:   "add at beginning with default",
			schema: myTable,
			newColumn: &sql.Column{
				Name:    "i2",
				Type:    types.Int64,
				Source:  "mytable",
				Default: mustDefault(expression.NewGetField(1, types.Int64, "i", false), types.Int64, false, true, true),
			},
			order: &sql.ColumnOrder{First: true},
			newSchema: sql.Schema{
				{Name: "i2", Type: types.Int64, Source: "mytable", Default: mustDefault(expression.NewGetField(0, types.Int64, "i", false), types.Int64, false, true, true)},
				{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
			},
			projections: []sql.Expression{
				plan.ColDefaultExpression{&sql.Column{
					Name:    "i2",
					Type:    types.Int64,
					Source:  "mytable",
					Default: mustDefault(expression.NewGetField(0, types.Int64, "i", false), types.Int64, false, true, true),
				}},
				expression.NewGetField(0, types.Int64, "i", false),
				expression.NewGetField(1, varchar20, "s", false),
			},
		},
		{
			name:      "add in middle",
			schema:    myTable,
			newColumn: &sql.Column{Name: "i2", Type: types.Int64, Source: "mytable"},
			order:     &sql.ColumnOrder{AfterColumn: "i"},
			newSchema: sql.Schema{
				{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "i2", Type: types.Int64, Source: "mytable"},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
			},
			projections: []sql.Expression{
				expression.NewGetField(0, types.Int64, "i", false),
				plan.ColDefaultExpression{&sql.Column{Name: "i2", Type: types.Int64, Source: "mytable"}},
				expression.NewGetField(1, varchar20, "s", false),
			},
		},
		{
			name:   "add in middle with default",
			schema: myTable,
			newColumn: &sql.Column{
				Name:    "i2",
				Type:    types.Int64,
				Source:  "mytable",
				Default: mustDefault(expression.NewGetField(2, types.Int64, "s", false), types.Int64, false, true, true),
			},
			order: &sql.ColumnOrder{AfterColumn: "i"},
			newSchema: sql.Schema{
				{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "i2", Type: types.Int64, Source: "mytable", Default: mustDefault(expression.NewGetField(1, types.Int64, "s", false), types.Int64, false, true, true)},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
			},
			projections: []sql.Expression{
				expression.NewGetField(0, types.Int64, "i", false),
				plan.ColDefaultExpression{&sql.Column{
					Name:    "i2",
					Type:    types.Int64,
					Source:  "mytable",
					Default: mustDefault(expression.NewGetField(1, types.Int64, "s", false), types.Int64, false, true, true),
				}},
				expression.NewGetField(1, varchar20, "s", false),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			schema, projections, err := addColumnToSchema(tc.schema, tc.newColumn, tc.order)
			if err != nil {
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.newSchema, schema)
			assert.Equal(t, tc.projections, projections)
		})
	}
}

func TestModifyColumnInSchema(t *testing.T) {
	varchar20 := types.MustCreateStringWithDefaults(sqltypes.VarChar, 20)

	myTable := sql.Schema{
		{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "f", Type: types.Float64, Source: "mytable"},
		{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
	}

	type testCase struct {
		name        string
		schema      sql.Schema
		colName     string
		newColumn   *sql.Column
		order       *sql.ColumnOrder
		newSchema   sql.Schema
		projections []sql.Expression
	}

	testCases := []testCase{
		{
			name:      "modify last in place",
			schema:    myTable,
			colName:   "s",
			newColumn: &sql.Column{Name: "s2", Type: types.Int64, Source: "mytable"},
			newSchema: sql.Schema{
				{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "f", Type: types.Float64, Source: "mytable"},
				{Name: "s2", Type: types.Int64, Source: "mytable"},
			},
			projections: []sql.Expression{
				expression.NewGetField(0, types.Int64, "i", false),
				expression.NewGetField(1, types.Float64, "f", false),
				expression.NewGetField(2, varchar20, "s", false),
			},
		},
		{
			name:      "modify first in place",
			schema:    myTable,
			colName:   "i",
			newColumn: &sql.Column{Name: "i2", Type: types.Int64, Source: "mytable", Comment: "my comment", PrimaryKey: true},
			newSchema: sql.Schema{
				{Name: "i2", Type: types.Int64, Source: "mytable", Comment: "my comment", PrimaryKey: true},
				{Name: "f", Type: types.Float64, Source: "mytable"},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
			},
			projections: []sql.Expression{
				expression.NewGetField(0, types.Int64, "i", false),
				expression.NewGetField(1, types.Float64, "f", false),
				expression.NewGetField(2, varchar20, "s", false),
			},
		},
		{
			name:      "modify first, move to middle",
			schema:    myTable,
			colName:   "i",
			order:     &sql.ColumnOrder{AfterColumn: "F"},
			newColumn: &sql.Column{Name: "i2", Type: types.Int64, Source: "mytable", Comment: "my comment", PrimaryKey: true},
			newSchema: sql.Schema{
				{Name: "f", Type: types.Float64, Source: "mytable"},
				{Name: "i2", Type: types.Int64, Source: "mytable", Comment: "my comment", PrimaryKey: true},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
			},
			projections: []sql.Expression{
				expression.NewGetField(1, types.Float64, "f", false),
				expression.NewGetField(0, types.Int64, "i", false),
				expression.NewGetField(2, varchar20, "s", false),
			},
		},
		{
			name:      "modify first, move to end",
			schema:    myTable,
			colName:   "i",
			order:     &sql.ColumnOrder{AfterColumn: "s"},
			newColumn: &sql.Column{Name: "i2", Type: types.Int64, Source: "mytable", Comment: "my comment", PrimaryKey: true},
			newSchema: sql.Schema{
				{Name: "f", Type: types.Float64, Source: "mytable"},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
				{Name: "i2", Type: types.Int64, Source: "mytable", Comment: "my comment", PrimaryKey: true},
			},
			projections: []sql.Expression{
				expression.NewGetField(1, types.Float64, "f", false),
				expression.NewGetField(2, varchar20, "s", false),
				expression.NewGetField(0, types.Int64, "i", false),
			},
		},
		{
			name:      "modify last, move first",
			schema:    myTable,
			colName:   "s",
			order:     &sql.ColumnOrder{First: true},
			newColumn: &sql.Column{Name: "s2", Type: types.Int64, Source: "mytable", Comment: "my comment"},
			newSchema: sql.Schema{
				{Name: "s2", Type: types.Int64, Source: "mytable", Comment: "my comment"},
				{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "f", Type: types.Float64, Source: "mytable"},
			},
			projections: []sql.Expression{
				expression.NewGetField(2, varchar20, "s", false),
				expression.NewGetField(0, types.Int64, "i", false),
				expression.NewGetField(1, types.Float64, "f", false),
			},
		},
		{
			name:      "modify middle, move first",
			schema:    myTable,
			colName:   "f",
			order:     &sql.ColumnOrder{First: true},
			newColumn: &sql.Column{Name: "f2", Type: types.Int64, Source: "mytable", Comment: "my comment"},
			newSchema: sql.Schema{
				{Name: "f2", Type: types.Int64, Source: "mytable", Comment: "my comment"},
				{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
			},
			projections: []sql.Expression{
				expression.NewGetField(1, types.Float64, "f", false),
				expression.NewGetField(0, types.Int64, "i", false),
				expression.NewGetField(2, varchar20, "s", false),
			},
		},
		{
			name:      "modify middle, move to middle",
			schema:    myTable,
			colName:   "f",
			order:     &sql.ColumnOrder{AfterColumn: "I"},
			newColumn: &sql.Column{Name: "f2", Type: types.Int64, Source: "mytable", Comment: "my comment"},
			newSchema: sql.Schema{
				{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "f2", Type: types.Int64, Source: "mytable", Comment: "my comment"},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
			},
			projections: []sql.Expression{
				expression.NewGetField(0, types.Int64, "i", false),
				expression.NewGetField(1, types.Float64, "f", false),
				expression.NewGetField(2, varchar20, "s", false),
			},
		},
		{
			name:      "modify last, move to middle",
			schema:    myTable,
			colName:   "s",
			order:     &sql.ColumnOrder{AfterColumn: "I"},
			newColumn: &sql.Column{Name: "s2", Type: types.Int64, Source: "mytable", Comment: "my comment"},
			newSchema: sql.Schema{
				{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "s2", Type: types.Int64, Source: "mytable", Comment: "my comment"},
				{Name: "f", Type: types.Float64, Source: "mytable"},
			},
			projections: []sql.Expression{
				expression.NewGetField(0, types.Int64, "i", false),
				expression.NewGetField(2, varchar20, "s", false),
				expression.NewGetField(1, types.Float64, "f", false),
			},
		},
		{
			name: "modify middle, move first with defaults",
			schema: sql.Schema{
				{Name: "one", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "two", Type: types.Int64, Source: "mytable"},
				{Name: "three", Type: types.Int64, Source: "mytable", Default: mustDefault(
					expression.NewGetFieldWithTable(1, types.Int64, "db", "mytable", "two", false),
					types.Int64, false, true, false),
				},
			},
			colName: "two",
			order:   &sql.ColumnOrder{First: true},
			newColumn: &sql.Column{Name: "two", Type: types.Int64, Source: "mytable", Default: mustDefault(
				expression.NewGetFieldWithTable(0, types.Int64, "db", "mytable", "one", false),
				types.Int64, false, true, false),
			},
			newSchema: sql.Schema{
				{Name: "two", Type: types.Int64, Source: "mytable", Default: mustDefault(
					expression.NewGetFieldWithTable(1, types.Int64, "db", "mytable", "one", false),
					types.Int64, false, true, false),
				},
				{Name: "one", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "three", Type: types.Int64, Source: "mytable", Default: mustDefault(
					expression.NewGetFieldWithTable(0, types.Int64, "db", "mytable", "two", false),
					types.Int64, false, true, false),
				},
			},
			projections: []sql.Expression{
				expression.NewGetField(1, types.Int64, "two", false),
				expression.NewGetField(0, types.Int64, "one", false),
				expression.NewGetField(2, types.Int64, "three", false),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			schema, projections, err := modifyColumnInSchema(tc.schema, tc.colName, tc.newColumn, tc.order)
			if err != nil {
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.newSchema, schema)
			assert.Equal(t, tc.projections, projections)
		})
	}
}

// mustDefault enforces that no error occurred when constructing the column default value.
func mustDefault(expr sql.Expression, outType sql.Type, representsLiteral bool, parenthesized bool, mayReturnNil bool) *sql.ColumnDefaultValue {
	colDef, err := sql.NewColumnDefaultValue(expr, outType, representsLiteral, parenthesized, mayReturnNil)
	if err != nil {
		panic(err)
	}
	return colDef
}
