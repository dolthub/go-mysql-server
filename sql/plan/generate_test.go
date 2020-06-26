package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

func TestGenerateRowIter(t *testing.T) {
	require := require.New(t)

	child := newFakeNode(
		sql.Schema{
			{Name: "a", Type: sql.Text, Source: "foo"},
			{Name: "b", Type: sql.CreateArray(sql.Text), Source: "foo"},
			{Name: "c", Type: sql.Int64, Source: "foo"},
		},
		sql.RowsToRowIter(
			sql.Row{"first", sql.NewArrayGenerator([]interface{}{"a", "b"}), int64(1)},
			sql.Row{"second", sql.NewArrayGenerator([]interface{}{"c", "d"}), int64(2)},
		),
	)

	iter, err := NewGenerate(
		child,
		expression.NewGetFieldWithTable(1, sql.CreateArray(sql.Text), "foo", "b", false),
	).RowIter(sql.NewEmptyContext())
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		{"first", "a", int64(1)},
		{"first", "b", int64(1)},
		{"second", "c", int64(2)},
		{"second", "d", int64(2)},
	}

	require.Equal(expected, rows)
}

func TestGenerateSchema(t *testing.T) {
	require := require.New(t)

	schema := NewGenerate(
		newFakeNode(
			sql.Schema{
				{Name: "a", Type: sql.Text, Source: "foo"},
				{Name: "b", Type: sql.CreateArray(sql.Text), Source: "foo"},
				{Name: "c", Type: sql.Int64, Source: "foo"},
			},
			nil,
		),
		expression.NewGetField(1, sql.CreateArray(sql.Text), "foobar", false),
	).Schema()

	expected := sql.Schema{
		{Name: "a", Type: sql.Text, Source: "foo"},
		{Name: "foobar", Type: sql.Text},
		{Name: "c", Type: sql.Int64, Source: "foo"},
	}

	require.Equal(expected, schema)
}

type fakeNode struct {
	schema sql.Schema
	iter   sql.RowIter
}

func newFakeNode(s sql.Schema, iter sql.RowIter) *fakeNode {
	return &fakeNode{s, iter}
}

func (n *fakeNode) Children() []sql.Node                      { return nil }
func (n *fakeNode) Resolved() bool                            { return true }
func (n *fakeNode) Schema() sql.Schema                        { return n.schema }
func (n *fakeNode) RowIter(*sql.Context) (sql.RowIter, error) { return n.iter, nil }
func (n *fakeNode) String() string                            { return "fakeNode" }
func (*fakeNode) WithChildren(children ...sql.Node) (sql.Node, error) {
	panic("placeholder")
}
