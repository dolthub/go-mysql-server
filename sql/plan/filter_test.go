package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestFilter(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	childSchema := sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
		{Name: "col2", Type: sql.Text, Nullable: true},
		{Name: "col3", Type: sql.Int32, Nullable: true},
		{Name: "col4", Type: sql.Int64, Nullable: true},
	}
	child := mem.NewTable("test", childSchema)

	rows := []sql.Row{
		sql.NewRow("col1_1", "col2_1", int32(1111), int64(2222)),
		sql.NewRow("col1_2", "col2_2", int32(3333), int64(4444)),
		sql.NewRow("col1_3", "col2_3", nil, int64(4444)),
	}

	for _, r := range rows {
		require.NoError(child.Insert(sql.NewEmptyContext(), r))
	}

	f := NewFilter(
		expression.NewEquals(
			expression.NewGetField(0, sql.Text, "col1", true),
			expression.NewLiteral("col1_1", sql.Text)),
		NewResolvedTable(child))

	require.Equal(1, len(f.Children()))

	iter, err := f.RowIter(ctx)
	require.NoError(err)
	require.NotNil(iter)

	row, err := iter.Next()
	require.NoError(err)
	require.NotNil(row)

	require.Equal("col1_1", row[0])
	require.Equal("col2_1", row[1])

	row, err = iter.Next()
	require.NotNil(err)
	require.Nil(row)

	f = NewFilter(expression.NewEquals(
		expression.NewGetField(2, sql.Int32, "col3", true),
		expression.NewLiteral(int32(1111),
			sql.Int32)), NewResolvedTable(child))

	iter, err = f.RowIter(ctx)
	require.NoError(err)
	require.NotNil(iter)

	row, err = iter.Next()
	require.NoError(err)
	require.NotNil(row)

	require.Equal(int32(1111), row[2])
	require.Equal(int64(2222), row[3])

	f = NewFilter(expression.NewEquals(
		expression.NewGetField(3, sql.Int64, "col4", true),
		expression.NewLiteral(int64(4444), sql.Int64)),
		NewResolvedTable(child))

	iter, err = f.RowIter(ctx)
	require.NoError(err)
	require.NotNil(iter)

	row, err = iter.Next()
	require.NoError(err)
	require.NotNil(row)

	require.Equal(int32(3333), row[2])
	require.Equal(int64(4444), row[3])
}
