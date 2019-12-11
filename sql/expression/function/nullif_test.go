package function

import (
	"testing"
	"vitess.io/vitess/go/sqltypes"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestNullIf(t *testing.T) {
	testCases := []struct {
		ex1      interface{}
		ex2      interface{}
		expected interface{}
	}{
		{"foo", "bar", "foo"},
		{"foo", "foo", sql.Null},
		{nil, "foo", nil},
		{"foo", nil, "foo"},
		{nil, nil, nil},
		{"", nil, ""},
	}

	f := NewNullIf(
		expression.NewGetField(0, sql.Text, "ex1", true),
		expression.NewGetField(1, sql.Text, "ex2", true),
	)
	require.Equal(t, sql.Text, f.Type())

	var3 := sql.MustCreateStringWithDefaults(sqltypes.VarChar, 3)
	f = NewNullIf(
		expression.NewGetField(0, var3, "ex1", true),
		expression.NewGetField(1, var3, "ex2", true),
	)
	require.Equal(t, var3, f.Type())

	for _, tc := range testCases {
		v, err := f.Eval(sql.NewEmptyContext(), sql.NewRow(tc.ex1, tc.ex2))
		require.NoError(t, err)
		require.Equal(t, tc.expected, v)
	}
}
