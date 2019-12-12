package sql

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/sqltypes"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTuple(t *testing.T) {
	require := require.New(t)

	typ := CreateTuple(Int32, Text, Int64)
	_, err := typ.Convert("foo")
	require.Error(err)
	require.True(ErrNotTuple.Is(err))

	_, err = typ.Convert([]interface{}{1, 2})
	require.Error(err)
	require.True(ErrInvalidColumnNumber.Is(err))

	conVal, err := typ.Convert([]interface{}{1, 2, 3})
	require.NoError( err)
	assert.Equal(t, []interface{}{int32(1), "2", int64(3)}, conVal)

	_, err = typ.SQL(nil)
	require.Error(err)

	require.Equal(sqltypes.Expression, typ.Type())

	comparisons := []struct{
		val1 []interface{}
		val2 []interface{}
		expectedCmp int
	}{
		{[]interface{}{1, 2, 3}, []interface{}{2, 2, 3}, -1},
		{[]interface{}{1, 2, 3}, []interface{}{1, 3, 3}, -1},
		{[]interface{}{1, 2, 3}, []interface{}{1, 2, 4}, -1},
		{[]interface{}{1, 2, 3}, []interface{}{1, 2, 3}, 0},
		{[]interface{}{2, 2, 3}, []interface{}{1, 2, 3}, 1},
		{[]interface{}{1, 3, 3}, []interface{}{1, 2, 3}, 1},
		{[]interface{}{1, 2, 4}, []interface{}{1, 2, 3}, 1},
	}

	for _, comparison := range comparisons {
		t.Run(fmt.Sprintf("%v %v", comparison.val1, comparison.val2), func(t *testing.T) {
			cmp, err := typ.Compare(comparison.val1, comparison.val2)
			require.NoError( err)
			assert.Equal(t, comparison.expectedCmp, cmp)
		})
	}
}
