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
				{Name: "i2", Type: types.Int64, Source: "mytable", Default: mustDefault(expression.NewGetField(1, types.Int64, "i", false), types.Int64, false, true, true)},
				{Name: "i", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
			},
			projections: []sql.Expression{
				plan.ColDefaultExpression{&sql.Column{
					Name:    "i2",
					Type:    types.Int64,
					Source:  "mytable",
					Default: mustDefault(expression.NewGetField(1, types.Int64, "i", false), types.Int64, false, true, true),
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
				{Name: "i2", Type: types.Int64, Source: "mytable", Default: mustDefault(expression.NewGetField(2, types.Int64, "s", false), types.Int64, false, true, true)},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
			},
			projections: []sql.Expression{
				expression.NewGetField(0, types.Int64, "i", false),
				plan.ColDefaultExpression{&sql.Column{
					Name:    "i2",
					Type:    types.Int64,
					Source:  "mytable",
					Default: mustDefault(expression.NewGetField(2, types.Int64, "s", false), types.Int64, false, true, true),
				}},
				expression.NewGetField(1, varchar20, "s", false),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			schema, projections, err := addColumnToSchema(sql.NewEmptyContext(), tc.schema, tc.newColumn, tc.order)
			if err != nil {
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.newSchema, schema)
			assert.Equal(t, tc.projections, projections)
		})
	}
}

// TestResolveGeneratedColumnsForIndexRewrite verifies persisted virtual expressions are bound before a table rewrite.
func TestResolveGeneratedColumnsForIndexRewrite(t *testing.T) {
	ctx := sql.NewEmptyContext()
	varchar20 := types.MustCreateStringWithDefaults(sqltypes.VarChar, 20)
	generated := sql.NewUnresolvedColumnDefaultValue("lower(fruit)")
	schema := sql.Schema{
		{Name: "fruit", Type: varchar20, Source: "fruits"},
		{
			Name:         "lower_fruit",
			Type:         varchar20,
			Source:       "fruits",
			Virtual:      true,
			HiddenSystem: true,
			Generated:    generated,
		},
	}

	resolved := resolveGeneratedColumns(ctx, sql.EngineOverrides{}, "mydb", "fruits", schema)
	require.False(t, schema[1].Generated.Resolved())
	require.True(t, resolved[1].Generated.Resolved())

	projections := virtualTableProjections(ctx, resolved, "fruits")
	row, err := ProjectRow(ctx, projections, sql.Row{"Apple"})
	require.NoError(t, err)
	require.Equal(t, sql.Row{"Apple", "apple"}, row)
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
					expression.NewGetFieldWithTable(1, 1, types.Int64, "", "mytable", "two", false),
					types.Int64, false, true, false),
				},
			},
			colName: "two",
			order:   &sql.ColumnOrder{First: true},
			newColumn: &sql.Column{Name: "two", Type: types.Int64, Source: "mytable", Default: mustDefault(
				expression.NewGetFieldWithTable(0, 1, types.Int64, "", "mytable", "one", false),
				types.Int64, false, true, false),
			},
			newSchema: sql.Schema{
				{Name: "two", Type: types.Int64, Source: "mytable", Default: mustDefault(
					expression.NewGetFieldWithTable(1, 1, types.Int64, "", "mytable", "one", false),
					types.Int64, false, true, false),
				},
				{Name: "one", Type: types.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "three", Type: types.Int64, Source: "mytable", Default: mustDefault(
					expression.NewGetFieldWithTable(0, 1, types.Int64, "", "mytable", "two", false),
					types.Int64, false, true, false),
				},
			},
			projections: []sql.Expression{
				expression.NewGetFieldWithTable(1, 0, types.Int64, "", "", "two", false),
				expression.NewGetFieldWithTable(0, 0, types.Int64, "", "", "one", false),
				expression.NewGetFieldWithTable(2, 0, types.Int64, "", "", "three", false),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			schema, projections, err := modifyColumnInSchema(sql.NewEmptyContext(), tc.schema, tc.colName, tc.newColumn, tc.order)
			if err != nil {
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.newSchema, schema)
			assert.Equal(t, tc.projections, projections)
		})
	}
}

func TestModifyStoredGeneratedColumnProjection(t *testing.T) {
	ctx := sql.NewEmptyContext()
	field := func(index int, name string) sql.Expression {
		return expression.NewGetField(index, types.Int64, name, false)
	}
	generated := func(left sql.Expression, op string, right sql.Expression) *sql.ColumnDefaultValue {
		return mustDefault(expression.NewArithmetic(left, right, op), types.Int64, false, true, false)
	}
	for _, name := range []string{"resolved", "persisted"} {
		t.Run(name, func(t *testing.T) {
			sch := sql.Schema{
				{Name: "x", Type: types.Int64},
				{Name: "y", Type: types.Int64},
				{Name: "z", Type: types.Int64, Generated: generated(field(0, "x"), "+", field(1, "y"))},
				{Name: "w", Type: types.Int64, Generated: generated(field(2, "z"), "+", expression.NewLiteral(int64(1), types.Int64))},
			}
			if name == "persisted" {
				sch[2].Generated = sql.NewUnresolvedColumnDefaultValue("(x + y)")
				sch[3].Generated = sql.NewUnresolvedColumnDefaultValue("(z + 1)")
				sch = resolveGeneratedColumns(ctx, sql.EngineOverrides{}, "mydb", "t", sch)
				require.True(t, sch[3].Generated.Resolved())
			}
			newCol := &sql.Column{Name: "product", Type: types.Int64, Generated: generated(field(0, "x"), "*", field(1, "y"))}
			_, projections, err := modifyColumnInSchema(ctx, sch, "z", newCol, &sql.ColumnOrder{First: true})
			require.NoError(t, err)
			row, err := ProjectRow(ctx, projections, sql.Row{int64(2), int64(3), int64(5), int64(6)})
			require.NoError(t, err)
			require.Equal(t, sql.Row{int64(6), int64(2), int64(3), int64(7)}, row)
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
