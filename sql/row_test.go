package sql

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRowsToRowIterEmpty(t *testing.T) {
	require := require.New(t)

	iter := RowsToRowIter()
	r, err := iter.Next()
	require.Equal(io.EOF, err)
	require.Nil(r)

	r, err = iter.Next()
	require.Equal(io.EOF, err)
	require.Nil(r)

	err = iter.Close()
	require.NoError(err)
}

func TestRowsToRowIter(t *testing.T) {
	require := require.New(t)

	iter := RowsToRowIter(NewRow(1), NewRow(2), NewRow(3))
	r, err := iter.Next()
	require.NoError(err)
	require.Equal(NewRow(1), r)

	r, err = iter.Next()
	require.NoError(err)
	require.Equal(NewRow(2), r)

	r, err = iter.Next()
	require.NoError(err)
	require.Equal(NewRow(3), r)

	r, err = iter.Next()
	require.Equal(io.EOF, err)
	require.Nil(r)

	r, err = iter.Next()
	require.Equal(io.EOF, err)
	require.Nil(r)

	err = iter.Close()
	require.NoError(err)
}
