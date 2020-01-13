package plan

import (
	"io"
	"testing"

	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestDescribe(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	table := memory.NewTable("test", sql.Schema{
		{Name: "c1", Type: sql.Text},
		{Name: "c2", Type: sql.Int32},
	})

	d := NewDescribe(NewResolvedTable(table))
	iter, err := d.RowIter(ctx)
	require.NoError(err)
	require.NotNil(iter)

	n, err := iter.Next()
	require.NoError(err)
	require.Equal(sql.NewRow("c1", "TEXT"), n)

	n, err = iter.Next()
	require.NoError(err)
	require.Equal(sql.NewRow("c2", "INT"), n)

	n, err = iter.Next()
	require.Equal(io.EOF, err)
	require.Nil(n)
}

func TestDescribe_Empty(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	d := NewDescribe(NewUnresolvedTable("test_table", ""))

	iter, err := d.RowIter(ctx)
	require.NoError(err)
	require.NotNil(iter)

	n, err := iter.Next()
	require.Equal(io.EOF, err)
	require.Nil(n)
}

func TestDescribeQuery(t *testing.T) {
	require := require.New(t)

	table := memory.NewTable("foo", sql.Schema{
		{Source: "foo", Name: "a", Type: sql.Text},
		{Source: "foo", Name: "b", Type: sql.Text},
	})

	node := NewDescribeQuery("tree", NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Text, "foo", "a", false),
			expression.NewGetFieldWithTable(1, sql.Text, "foo", "b", false),
		},
		NewFilter(
			expression.NewEquals(
				expression.NewGetFieldWithTable(0, sql.Text, "foo", "a", false),
				expression.NewLiteral("foo", sql.LongText),
			),
			NewResolvedTable(table),
		),
	))

	iter, err := node.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		{"Project(foo.a, foo.b)"},
		{" └─ Filter(foo.a = \"foo\")"},
		{"     └─ Table(foo)"},
	}

	require.Equal(expected, rows)
}
