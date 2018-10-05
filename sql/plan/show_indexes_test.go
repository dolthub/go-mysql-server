package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestShowIndexes(t *testing.T) {
	var require = require.New(t)

	unresolved := NewShowIndexes(sql.UnresolvedDatabase(""), "table-test", nil)
	require.False(unresolved.Resolved())
	require.Nil(unresolved.Children())

	db := mem.NewDatabase("test")

	tests := []struct {
		name         string
		table        sql.Table
		isExpression bool
	}{
		{
			name: "test1",
			table: mem.NewTable(
				"test1",
				sql.Schema{
					&sql.Column{Name: "foo", Type: sql.Int32, Source: "test1", Default: int32(0), Nullable: false},
				},
			),
		},
		{
			name: "test2",
			table: mem.NewTable(
				"test2",
				sql.Schema{
					&sql.Column{Name: "bar", Type: sql.Int64, Source: "test2", Default: int64(0), Nullable: true},
					&sql.Column{Name: "rab", Type: sql.Int64, Source: "test2", Default: int32(0), Nullable: false},
				},
			),
		},
		{
			name: "test3",
			table: mem.NewTable(
				"test3",
				sql.Schema{
					&sql.Column{Name: "baz", Type: sql.Text, Source: "test3", Default: "", Nullable: false},
					&sql.Column{Name: "zab", Type: sql.Int32, Source: "test3", Default: int32(0), Nullable: true},
					&sql.Column{Name: "bza", Type: sql.Int64, Source: "test3", Default: int64(0), Nullable: true},
				},
			),
		},
		{
			name: "test4",
			table: mem.NewTable(
				"test4",
				sql.Schema{
					&sql.Column{Name: "oof", Type: sql.Text, Source: "test4", Default: "", Nullable: false},
				},
			),
			isExpression: true,
		},
	}

	r := sql.NewIndexRegistry()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db.AddTable(test.name, test.table)

			expressions := make([]sql.Expression, len(test.table.Schema()))
			for i, col := range test.table.Schema() {
				var ex sql.Expression = expression.NewGetFieldWithTable(
					i, col.Type, test.name, col.Name, col.Nullable,
				)

				if test.isExpression {
					ex = expression.NewEquals(ex, expression.NewLiteral("a", sql.Text))
				}

				expressions[i] = ex
			}

			idx := &mockIndex{
				db:    "test",
				table: test.name,
				id:    test.name + "_idx",
				exprs: expressions,
			}

			created, ready, err := r.AddIndex(idx)
			require.NoError(err)
			close(created)
			<-ready

			showIdxs := NewShowIndexes(db, test.name, r)

			ctx := sql.NewEmptyContext()
			rowIter, err := showIdxs.RowIter(ctx)
			require.NoError(err)

			rows, err := sql.RowIterToRows(rowIter)
			require.NoError(err)
			require.Len(rows, len(expressions))

			for i, row := range rows {
				var nullable string
				columnName, ex := "NULL", expressions[i].String()
				if ok, null := isColumn(ex, test.table); ok {
					columnName, ex = ex, columnName
					if null {
						nullable = "YES"
					}
				}

				expected := sql.NewRow(
					test.name,
					int32(1),
					idx.ID(),
					i+1,
					columnName,
					"NULL",
					int64(0),
					"NULL",
					"NULL",
					nullable,
					idx.Driver(),
					"",
					"",
					"YES",
					ex,
				)

				require.Equal(expected, row)
			}

		})
	}
}
