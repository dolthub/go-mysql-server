package plan_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/parse"
	. "github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func TestShowIndexes(t *testing.T) {
	ctx := sql.NewEmptyContext()
	unresolved := NewShowIndexes(NewUnresolvedTable("table-test", ""))
	require.False(t, unresolved.Resolved())
	require.Equal(t, []sql.Node{NewUnresolvedTable("table-test", "")}, unresolved.Children())

	db := memory.NewDatabase("test")

	tests := []struct {
		name         string
		table        sql.Table
		isExpression bool
	}{
		{
			name: "test1",
			table: memory.NewTable(
				"test1",
				sql.Schema{
					&sql.Column{Name: "foo", Type: sql.Int32, Source: "test1", Default: parse.MustStringToColumnDefaultValue(ctx, "0", sql.Int32, false), Nullable: false},
				},
			),
		},
		{
			name: "test2",
			table: memory.NewTable(
				"test2",
				sql.Schema{
					&sql.Column{Name: "bar", Type: sql.Int64, Source: "test2", Default: parse.MustStringToColumnDefaultValue(ctx, "0", sql.Int64, true), Nullable: true},
					&sql.Column{Name: "rab", Type: sql.Int64, Source: "test2", Default: parse.MustStringToColumnDefaultValue(ctx, "0", sql.Int64, false), Nullable: false},
				},
			),
		},
		{
			name: "test3",
			table: memory.NewTable(
				"test3",
				sql.Schema{
					&sql.Column{Name: "baz", Type: sql.Text, Source: "test3", Default: parse.MustStringToColumnDefaultValue(ctx, `""`, sql.Text, false), Nullable: false},
					&sql.Column{Name: "zab", Type: sql.Int32, Source: "test3", Default: parse.MustStringToColumnDefaultValue(ctx, "0", sql.Int32, true), Nullable: true},
					&sql.Column{Name: "bza", Type: sql.Int64, Source: "test3", Default: parse.MustStringToColumnDefaultValue(ctx, "0", sql.Int64, true), Nullable: true},
				},
			),
		},
		{
			name: "test4",
			table: memory.NewTable(
				"test4",
				sql.Schema{
					&sql.Column{Name: "oof", Type: sql.Text, Source: "test4", Default: parse.MustStringToColumnDefaultValue(ctx, `""`, sql.Text, false), Nullable: false},
				},
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db.AddTable(test.name, test.table)

			expressions := make([]sql.Expression, len(test.table.Schema()))
			for i, col := range test.table.Schema() {
				var ex sql.Expression = expression.NewGetFieldWithTable(
					i, col.Type, test.name, col.Name, col.Nullable,
				)

				if test.isExpression {
					ex = expression.NewEquals(ex, expression.NewLiteral("a", sql.LongText))
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
			showIdxs := NewShowIndexes(NewResolvedTable(test.table))
			showIdxs.(*ShowIndexes).IndexesToShow = []sql.Index{idx}

			rowIter, err := showIdxs.RowIter(ctx, nil)
			assert.NoError(t, err)

			rows, err := sql.RowIterToRows(rowIter)
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
