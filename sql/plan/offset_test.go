package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOffsetPlan(t *testing.T) {
	require := require.New(t)
	table, _ := getTestingTable()
	offset := NewOffset(0, table)
	require.Equal(1, len(offset.Children()))

	iter, err := offset.RowIter()
	require.NoError(err)
	require.NotNil(iter)
}

func TestOffset(t *testing.T) {
	require := require.New(t)
	table, n := getTestingTable()
	offset := NewOffset(1, table)

	iter, err := offset.RowIter()
	require.NoError(err)
	assertRows(t, iter, int64(n-1))
}
