package plan

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestDescribe(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewContext(context.TODO(), sql.NewBaseSession())

	table := mem.NewTable("test", sql.Schema{
		{Name: "c1", Type: sql.Text},
		{Name: "c2", Type: sql.Int32},
	})

	d := NewDescribe(table)
	iter, err := d.RowIter(ctx)
	require.Nil(err)
	require.NotNil(iter)

	n, err := iter.Next()
	require.Nil(err)
	require.Equal(sql.NewRow("c1", "TEXT"), n)

	n, err = iter.Next()
	require.Nil(err)
	require.Equal(sql.NewRow("c2", "INT32"), n)

	n, err = iter.Next()
	require.Equal(io.EOF, err)
	require.Nil(n)
}

func TestDescribe_Empty(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewContext(context.TODO(), sql.NewBaseSession())

	d := NewDescribe(NewUnresolvedTable("test_table"))

	iter, err := d.RowIter(ctx)
	require.Nil(err)
	require.NotNil(iter)

	n, err := iter.Next()
	require.Equal(io.EOF, err)
	require.Nil(n)
}
