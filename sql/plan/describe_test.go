package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestDescribe(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	table := mem.NewTable("test", sql.Schema{
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
	require.Equal(sql.NewRow("c2", "INT32"), n)

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

	table := mem.NewTable("foo", sql.Schema{
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
				expression.NewLiteral("foo", sql.Text),
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
		{"         ├─ Column(a, TEXT, nullable=false)"},
		{"         └─ Column(b, TEXT, nullable=false)"},
	}

	require.Equal(expected, rows)
}
