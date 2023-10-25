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

package rowexec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	. "github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestShowIndexes(t *testing.T) {
	ctx := sql.NewEmptyContext()
	unresolved := NewShowIndexes(NewUnresolvedTable("table-test", ""))
	require.False(t, unresolved.Resolved())
	require.Equal(t, []sql.Node{NewUnresolvedTable("table-test", "")}, unresolved.Children())

	db := memory.NewDatabase("test")

	tests := []struct {
		name         string
		table        memory.MemTable
		isExpression bool
	}{
		{
			name: "test1",
			table: memory.NewTable(db, "test1", sql.NewPrimaryKeySchema(sql.Schema{
				&sql.Column{Name: "foo", Type: types.Int32, Source: "test1", Default: planbuilder.MustStringToColumnDefaultValue(ctx, "0", types.Int32, false), Nullable: false},
			}), db.GetForeignKeyCollection()),
		},
		{
			name: "test2",
			table: memory.NewTable(db, "test2", sql.NewPrimaryKeySchema(sql.Schema{
				&sql.Column{Name: "bar", Type: types.Int64, Source: "test2", Default: planbuilder.MustStringToColumnDefaultValue(ctx, "0", types.Int64, true), Nullable: true},
				&sql.Column{Name: "rab", Type: types.Int64, Source: "test2", Default: planbuilder.MustStringToColumnDefaultValue(ctx, "0", types.Int64, false), Nullable: false},
			}), db.GetForeignKeyCollection()),
		},
		{
			name: "test3",
			table: memory.NewTable(db, "test3", sql.NewPrimaryKeySchema(sql.Schema{
				&sql.Column{Name: "baz", Type: types.Text, Source: "test3", Default: planbuilder.MustStringToColumnDefaultValue(ctx, `""`, types.Text, false), Nullable: false},
				&sql.Column{Name: "zab", Type: types.Int32, Source: "test3", Default: planbuilder.MustStringToColumnDefaultValue(ctx, "0", types.Int32, true), Nullable: true},
				&sql.Column{Name: "bza", Type: types.Int64, Source: "test3", Default: planbuilder.MustStringToColumnDefaultValue(ctx, "0", types.Int64, true), Nullable: true},
			}), db.GetForeignKeyCollection()),
		},
		{
			name: "test4",
			table: memory.NewTable(db, "test4", sql.NewPrimaryKeySchema(sql.Schema{
				&sql.Column{Name: "oof", Type: types.Text, Source: "test4", Default: planbuilder.MustStringToColumnDefaultValue(ctx, `""`, types.Text, false), Nullable: false},
			}), db.GetForeignKeyCollection()),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db.AddTable(test.name, test.table)

			expressions := make([]sql.Expression, len(test.table.Schema()))
			for i, col := range test.table.Schema() {
				var ex sql.Expression = expression.NewGetFieldWithTable(
					i, col.Type, col.DatabaseSource, test.name, col.Name, col.Nullable,
				)

				if test.isExpression {
					ex = expression.NewEquals(ex, expression.NewLiteral("a", types.LongText))
				}

				expressions[i] = ex
			}

			idx := &mockIndex{
				db:    "test",
				table: test.name,
				id:    test.name + "_idx",
				exprs: expressions,
			}

			// Assigning tables and indexes manually. This mimics what happens during analysis
			showIdxs := NewShowIndexes(NewResolvedTable(test.table, nil, nil))
			showIdxs.IndexesToShow = []sql.Index{idx}

			rowIter, err := DefaultBuilder.Build(ctx, showIdxs, nil)
			assert.NoError(t, err)

			rows, err := sql.RowIterToRows(ctx, nil, rowIter)
			assert.NoError(t, err)
			assert.Len(t, rows, len(expressions))

			for i, row := range rows {
				var nullable string
				var columnName, ex interface{}
				columnName, ex = "NULL", expressions[i].String()
				if col := GetColumnFromIndexExpr(ex.(string), test.table); col != nil {
					columnName, ex = col.Name, nil
					if col.Nullable {
						nullable = "YES"
					}
				}

				expected := sql.NewRow(
					test.name,
					1,
					idx.ID(),
					i+1,
					columnName,
					nil,
					int64(0),
					nil,
					nil,
					nullable,
					"BTREE",
					"",
					"",
					"NO",
					ex,
				)

				assert.Equal(t, expected, row)
			}
		})
	}
}
