package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var lSchema = sql.Schema{
	{Name: "lcol1", Type: sql.Text},
	{Name: "lcol2", Type: sql.Text},
	{Name: "lcol3", Type: sql.Int32},
	{Name: "lcol4", Type: sql.Int64},
}

var rSchema = sql.Schema{
	{Name: "rcol1", Type: sql.Text},
	{Name: "rcol2", Type: sql.Text},
	{Name: "rcol3", Type: sql.Int32},
	{Name: "rcol4", Type: sql.Int64},
}

func TestCrossJoin(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	resultSchema := sql.Schema{
		{Name: "lcol1", Type: sql.Text},
		{Name: "lcol2", Type: sql.Text},
		{Name: "lcol3", Type: sql.Int32},
		{Name: "lcol4", Type: sql.Int64},
		{Name: "rcol1", Type: sql.Text},
		{Name: "rcol2", Type: sql.Text},
		{Name: "rcol3", Type: sql.Int32},
		{Name: "rcol4", Type: sql.Int64},
	}

	ltable := mem.NewTable("left", lSchema)
	rtable := mem.NewTable("right", rSchema)
	insertData(t, ltable)
	insertData(t, rtable)

	j := NewCrossJoin(
		NewResolvedTable(ltable),
		NewResolvedTable(rtable),
	)

	require.Equal(resultSchema, j.Schema())

	iter, err := j.RowIter(ctx)
	require.NoError(err)
	require.NotNil(iter)

	row, err := iter.Next()
	require.NoError(err)
	require.NotNil(row)

	require.Equal(8, len(row))

	require.Equal("col1_1", row[0])
	require.Equal("col2_1", row[1])
	require.Equal(int32(1111), row[2])
	require.Equal(int64(2222), row[3])
	require.Equal("col1_1", row[4])
	require.Equal("col2_1", row[5])
	require.Equal(int32(1111), row[6])
	require.Equal(int64(2222), row[7])

	row, err = iter.Next()
	require.NoError(err)
	require.NotNil(row)

	require.Equal("col1_1", row[0])
	require.Equal("col2_1", row[1])
	require.Equal(int32(1111), row[2])
	require.Equal(int64(2222), row[3])
	require.Equal("col1_2", row[4])
	require.Equal("col2_2", row[5])
	require.Equal(int32(3333), row[6])
	require.Equal(int64(4444), row[7])

	for i := 0; i < 2; i++ {
		row, err = iter.Next()
		require.NoError(err)
		require.NotNil(row)
	}

	// total: 4 rows
	row, err = iter.Next()
	require.NotNil(err)
	require.Equal(err, io.EOF)
	require.Nil(row)
}

func TestCrossJoin_Empty(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	ltable := mem.NewTable("left", lSchema)
	rtable := mem.NewTable("right", rSchema)
	insertData(t, ltable)

	j := NewCrossJoin(
		NewResolvedTable(ltable),
		NewResolvedTable(rtable),
	)

	iter, err := j.RowIter(ctx)
	require.NoError(err)
	require.NotNil(iter)

	row, err := iter.Next()
	require.Equal(io.EOF, err)
	require.Nil(row)

	ltable = mem.NewTable("left", lSchema)
	rtable = mem.NewTable("right", rSchema)
	insertData(t, rtable)

	j = NewCrossJoin(
		NewResolvedTable(ltable),
		NewResolvedTable(rtable),
	)

	iter, err = j.RowIter(ctx)
	require.NoError(err)
	require.NotNil(iter)

	row, err = iter.Next()
	require.Equal(io.EOF, err)
	require.Nil(row)
}

func insertData(t *testing.T, table *mem.Table) {
	t.Helper()
	require := require.New(t)

	rows := []sql.Row{
		sql.NewRow("col1_1", "col2_1", int32(1111), int64(2222)),
		sql.NewRow("col1_2", "col2_2", int32(3333), int64(4444)),
	}

	for _, r := range rows {
		require.NoError(table.Insert(sql.NewEmptyContext(), r))
	}
}
