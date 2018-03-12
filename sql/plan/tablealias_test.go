package plan

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestTableAlias(t *testing.T) {
	require := require.New(t)
	session := sql.NewBaseSession(context.TODO())

	table := mem.NewTable("bar", sql.Schema{
		{Name: "a", Type: sql.Text, Nullable: true},
		{Name: "b", Type: sql.Text, Nullable: true},
	})
	alias := NewTableAlias("foo", table)

	var rows = []sql.Row{
		sql.NewRow("1", "2"),
		sql.NewRow("3", "4"),
		sql.NewRow("5", "6"),
	}

	for _, r := range rows {
		require.NoError(table.Insert(r))
	}

	require.Equal(table.Schema(), alias.Schema())
	iter, err := alias.RowIter(session)
	require.NoError(err)

	var i int
	for {
		row, err := iter.Next()
		if err == io.EOF {
			break
		}

		require.NoError(err)
		require.Equal(rows[i], row)
		i++
	}

	require.Equal(len(rows), i)
}

func TestTableAliasSchema(t *testing.T) {
	require := require.New(t)

	tableSchema := sql.Schema{
		{Name: "foo", Type: sql.Text, Nullable: false, Source: "bar"},
		{Name: "baz", Type: sql.Text, Nullable: false, Source: "bar"},
	}

	subquerySchema := sql.Schema{
		{Name: "foo", Type: sql.Text, Nullable: false, Source: "alias"},
		{Name: "baz", Type: sql.Text, Nullable: false, Source: "alias"},
	}

	table := mem.NewTable("bar", tableSchema)

	subquery := NewProject(
		[]sql.Expression{
			expression.NewGetField(0, sql.Text, "foo", false),
			expression.NewGetField(1, sql.Text, "baz", false),
		},
		nil,
	)

	require.Equal(
		tableSchema,
		NewTableAlias("alias", table).Schema(),
	)

	require.Equal(
		subquerySchema,
		NewTableAlias("alias", subquery).Schema(),
	)
}
