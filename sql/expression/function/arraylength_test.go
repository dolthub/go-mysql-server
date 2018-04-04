package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestArrayLength(t *testing.T) {
	f := NewArrayLength(expression.NewGetField(0, sql.Array(sql.Int64), "", false))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"array is nil", sql.NewRow(nil), nil, nil},
		{"array is not of right type", sql.NewRow(5), nil, sql.ErrInvalidType},
		{"array is ok", sql.NewRow([]interface{}{1, 2, 3}), int32(3), nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}

	f = NewArrayLength(expression.NewGetField(0, sql.Tuple(sql.Int64, sql.Int64), "", false))
	require := require.New(t)
	_, err := f.Eval(sql.NewEmptyContext(), []interface{}{int64(1), int64(2)})
	require.Error(err)
	require.True(sql.ErrInvalidType.Is(err))
}
