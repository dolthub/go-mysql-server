package function

import (
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestLength(t *testing.T) {
	testCases := []struct {
		name      string
		input     interface{}
		inputType sql.Type
		fn        func(sql.Expression) sql.Expression
		expected  interface{}
	}{
		{
			"length string",
			"fóo",
			sql.Text,
			NewLength,
			int32(4),
		},
		{
			"length binary",
			[]byte("fóo"),
			sql.Blob,
			NewLength,
			int32(4),
		},
		{
			"length empty",
			"",
			sql.Blob,
			NewLength,
			int32(0),
		},
		{
			"length empty binary",
			[]byte{},
			sql.Blob,
			NewLength,
			int32(0),
		},
		{
			"length nil",
			nil,
			sql.Blob,
			NewLength,
			nil,
		},
		{
			"char_length string",
			"fóo",
			sql.LongText,
			NewCharLength,
			int32(3),
		},
		{
			"char_length binary",
			[]byte("fóo"),
			sql.Blob,
			NewCharLength,
			int32(3),
		},
		{
			"char_length empty",
			"",
			sql.Blob,
			NewCharLength,
			int32(0),
		},
		{
			"char_length empty binary",
			[]byte{},
			sql.Blob,
			NewCharLength,
			int32(0),
		},
		{
			"char_length nil",
			nil,
			sql.Blob,
			NewCharLength,
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := tt.fn(expression.NewGetField(0, tt.inputType, "foo", false)).Eval(
				sql.NewEmptyContext(),
				sql.Row{tt.input},
			)

			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}
