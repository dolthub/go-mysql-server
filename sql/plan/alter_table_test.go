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

package plan

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddColumnToSchema(t *testing.T) {
	myTable := sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
		{Name: "s", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "mytable", Comment: "column s"},
	}

	type testCase struct {
		name string
		schema sql.Schema
		newColumn *sql.Column
		order *sql.ColumnOrder
		newSchema sql.Schema
		projections []sql.Expression
	}

	varchar20 := sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20)
	testCases := []testCase{
		{
			name: "add at end",
			schema:      myTable,
			newColumn:   &sql.Column{Name: "i2", Type: sql.Int64, Source: "mytable"},
			newSchema:   sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
				{Name: "i2", Type: sql.Int64, Source: "mytable"},
			},
			projections: []sql.Expression{
				expression.NewGetField(0, sql.Int64, "i", false),
				expression.NewGetField(1, varchar20, "s", false),
				colDefaultExpression{&sql.Column{Name: "i2", Type: sql.Int64, Source: "mytable"}},
			},
		},
		{
			name: "add at end, with 'after'",
			schema:      myTable,
			newColumn:   &sql.Column{Name: "i2", Type: sql.Int64, Source: "mytable"},
			order: &sql.ColumnOrder{AfterColumn: "s"},
			newSchema:   sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
				{Name: "i2", Type: sql.Int64, Source: "mytable"},
			},
			projections: []sql.Expression{
				expression.NewGetField(0, sql.Int64, "i", false),
				expression.NewGetField(1, varchar20, "s", false),
				colDefaultExpression{&sql.Column{Name: "i2", Type: sql.Int64, Source: "mytable"}},
			},
		},
		{
			name: "add at beginning",
			schema:      myTable,
			newColumn:   &sql.Column{Name: "i2", Type: sql.Int64, Source: "mytable"},
			order: &sql.ColumnOrder{First: true},
			newSchema:   sql.Schema{
				{Name: "i2", Type: sql.Int64, Source: "mytable"},
				{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
			},
			projections: []sql.Expression{
				colDefaultExpression{&sql.Column{Name: "i2", Type: sql.Int64, Source: "mytable"}},
				expression.NewGetField(0, sql.Int64, "i", false),
				expression.NewGetField(1, varchar20, "s", false),
			},
		},
		{
			name: "add at beginning with default",
			schema:      myTable,
			newColumn:   &sql.Column{
				Name: "i2",
				Type: sql.Int64,
				Source: "mytable",
				Default: mustDefault(expression.NewGetField(1, sql.Int64, "i", false), sql.Int64, false, true),
			},
			order: &sql.ColumnOrder{First: true},
			newSchema:   sql.Schema{
				{Name: "i2", Type: sql.Int64, Source: "mytable", Default: mustDefault(expression.NewGetField(1, sql.Int64, "i", false), sql.Int64, false, true),},
				{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
			},
			projections: []sql.Expression{
				colDefaultExpression{&sql.Column{
					Name: "i2",
					Type: sql.Int64,
					Source: "mytable",
					Default: mustDefault(expression.NewGetField(0, sql.Int64, "i", false), sql.Int64, false, true),
				}},
				expression.NewGetField(0, sql.Int64, "i", false),
				expression.NewGetField(1, varchar20, "s", false),
			},
		},
		{
			name: "add in middle",
			schema:      myTable,
			newColumn:   &sql.Column{Name: "i2", Type: sql.Int64, Source: "mytable"},
			order: &sql.ColumnOrder{AfterColumn: "i"},
			newSchema:   sql.Schema{
				{Name: "i", Type: sql.Int64, Source: "mytable", PrimaryKey: true},
				{Name: "i2", Type: sql.Int64, Source: "mytable"},
				{Name: "s", Type: varchar20, Source: "mytable", Comment: "column s"},
			},
			projections: []sql.Expression{
				expression.NewGetField(0, sql.Int64, "i", false),
				colDefaultExpression{&sql.Column{Name: "i2", Type: sql.Int64, Source: "mytable"}},
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

// mustDefault enforces that no error occurred when constructing the column default value.
func mustDefault(expr sql.Expression, outType sql.Type, representsLiteral bool, mayReturnNil bool) *sql.ColumnDefaultValue {
	colDef, err := sql.NewColumnDefaultValue(expr, outType, representsLiteral, mayReturnNil)
	if err != nil {
		panic(err)
	}
	return colDef
}