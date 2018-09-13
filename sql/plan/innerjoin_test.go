package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestInnerJoin(t *testing.T) {
	require := require.New(t)
	finalSchema := append(lSchema, rSchema...)

	ltable := mem.NewTable("left", lSchema)
	rtable := mem.NewTable("right", rSchema)
	insertData(t, ltable)
	insertData(t, rtable)

	j := NewInnerJoin(
		NewResolvedTable(ltable),
		NewResolvedTable(rtable),
		expression.NewEquals(
			expression.NewGetField(0, sql.Text, "lcol1", false),
			expression.NewGetField(4, sql.Text, "rcol1", false),
		))

	require.Equal(finalSchema, j.Schema())

	rows := collectRows(t, j)
	require.Len(rows, 2)

	require.Equal([]sql.Row{
		{"col1_1", "col2_1", int32(1111), int64(2222), "col1_1", "col2_1", int32(1111), int64(2222)},
		{"col1_2", "col2_2", int32(3333), int64(4444), "col1_2", "col2_2", int32(3333), int64(4444)},
	}, rows)
}

func TestInnerJoinEmpty(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	ltable := mem.NewTable("left", lSchema)
	rtable := mem.NewTable("right", rSchema)

	j := NewInnerJoin(
		NewResolvedTable(ltable),
		NewResolvedTable(rtable),
		expression.NewEquals(
			expression.NewGetField(0, sql.Text, "lcol1", false),
			expression.NewGetField(4, sql.Text, "rcol1", false),
		))

	iter, err := j.RowIter(ctx)
	require.NoError(err)

	assertRows(t, iter, 0)
}
