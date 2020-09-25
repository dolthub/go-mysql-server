package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestIfNull(t *testing.T) {
	testCases := []struct {
		expression interface{}
		value      interface{}
		expected   interface{}
	}{
		{"foo", "bar", "foo"},
		{"foo", "foo", "foo"},
		{nil, "foo", "foo"},
		{"foo", nil, "foo"},
		{nil, nil, nil},
		{"", nil, ""},
	}

	f := NewIfNull(
		expression.NewGetField(0, sql.LongText, "expression", true),
		expression.NewGetField(1, sql.LongText, "value", true),
	)
	require.Equal(t, sql.LongText, f.Type())

	for _, tc := range testCases {
		v, err := f.Eval(sql.NewEmptyContext(), sql.NewRow(tc.expression, tc.value))
		require.NoError(t, err)
		require.Equal(t, tc.expected, v)
	}
}
