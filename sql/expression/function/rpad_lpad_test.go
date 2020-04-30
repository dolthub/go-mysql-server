package function

import (
	"testing"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestLPad(t *testing.T) {
	f, err := NewPad(
		lPadType,
		expression.NewGetField(0, sql.LongText, "str", false),
		expression.NewGetField(1, sql.Int64, "len", false),
		expression.NewGetField(2, sql.LongText, "padStr", false),
	)
	require.NoError(t, err)
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null string", sql.NewRow(nil, 1, "bar"), nil, false},
		{"null len", sql.NewRow("foo", nil, "bar"), nil, false},
		{"null padStr", sql.NewRow("foo", 1, nil), nil, false},

		{"negative length", sql.NewRow("foo", -1, "bar"), "", false},
		{"length 0", sql.NewRow("foo", 0, "bar"), "", false},
		{"invalid length", sql.NewRow("foo", "a", "bar"), "", true},

		{"empty padStr and len < len(str)", sql.NewRow("foo", 1, ""), "f", false},
		{"empty padStr and len > len(str)", sql.NewRow("foo", 4, ""), "", false},
		{"empty padStr and len == len(str)", sql.NewRow("foo", 3, ""), "foo", false},

		{"non empty padStr and len < len(str)", sql.NewRow("foo", 1, "abcd"), "f", false},
		{"non empty padStr and len == len(str)", sql.NewRow("foo", 3, "abcd"), "foo", false},

		{"padStr repeats exactly once", sql.NewRow("foo", 6, "abc"), "abcfoo", false},
		{"padStr does not repeat once", sql.NewRow("foo", 5, "abc"), "abfoo", false},
		{"padStr repeats many times", sql.NewRow("foo", 10, "abc"), "abcabcafoo", false},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			v, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)
			}
		})
	}
}

func TestRPad(t *testing.T) {
	f, err := NewPad(
		rPadType,
		expression.NewGetField(0, sql.LongText, "str", false),
		expression.NewGetField(1, sql.Int64, "len", false),
		expression.NewGetField(2, sql.LongText, "padStr", false),
	)
	require.NoError(t, err)
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null string", sql.NewRow(nil, 1, "bar"), nil, false},
		{"null len", sql.NewRow("foo", nil, "bar"), nil, false},
		{"null padStr", sql.NewRow("foo", 1, nil), nil, false},

		{"negative length", sql.NewRow("foo", -1, "bar"), "", false},
		{"length 0", sql.NewRow("foo", 0, "bar"), "", false},
		{"invalid length", sql.NewRow("foo", "a", "bar"), "", true},

		{"empty padStr and len < len(str)", sql.NewRow("foo", 1, ""), "f", false},
		{"empty padStr and len > len(str)", sql.NewRow("foo", 4, ""), "", false},
		{"empty padStr and len == len(str)", sql.NewRow("foo", 3, ""), "foo", false},

		{"non empty padStr and len < len(str)", sql.NewRow("foo", 1, "abcd"), "f", false},
		{"non empty padStr and len == len(str)", sql.NewRow("foo", 3, "abcd"), "foo", false},

		{"padStr repeats exactly once", sql.NewRow("foo", 6, "abc"), "fooabc", false},
		{"padStr does not repeat once", sql.NewRow("foo", 5, "abc"), "fooab", false},
		{"padStr repeats many times", sql.NewRow("foo", 10, "abc"), "fooabcabca", false},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			v, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)
			}
		})
	}
}
