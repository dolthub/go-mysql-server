package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
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
		expression.NewGetField(0, sql.Text, "expression", true),
		expression.NewGetField(1, sql.Text, "value", true),
	)
	require.Equal(t, sql.Text, f.Type())

	for _, tc := range testCases {
		v, err := f.Eval(sql.NewEmptyContext(), sql.NewRow(tc.expression, tc.value))
		require.NoError(t, err)
		require.Equal(t, tc.expected, v)
	}
}
