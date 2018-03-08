package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestOffsetPlan(t *testing.T) {
	require := require.New(t)
	session := sql.NewBaseSession(context.TODO())

	table, _ := getTestingTable()
	offset := NewOffset(0, table)
	require.Equal(1, len(offset.Children()))

	iter, err := offset.RowIter(session)
	require.NoError(err)
	require.NotNil(iter)
}

func TestOffset(t *testing.T) {
	require := require.New(t)
	session := sql.NewBaseSession(context.TODO())

	table, n := getTestingTable()
	offset := NewOffset(1, table)

	iter, err := offset.RowIter(session)
	require.NoError(err)
	assertRows(t, iter, int64(n-1))
}
