package function

import (
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestTrim(t *testing.T) {
	f := NewTrimFunc(bTrimType)(expression.NewGetField(0, sql.LongText, "", false))
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"trimmed string", sql.NewRow("foo"), "foo", false},
		{"spaces in both sides", sql.NewRow("  foo    "), "foo", false},
		{"spaces in left side", sql.NewRow("  foo"), "foo", false},
		{"spaces in right side", sql.NewRow("foo    "), "foo", false},
		{"two words with spaces", sql.NewRow(" foo   bar "), "foo   bar", false},
		{"different kinds of spaces", sql.NewRow("\r\tfoo   bar \v"), "foo   bar", false},
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

func TestLTrim(t *testing.T) {
	f := NewTrimFunc(lTrimType)(expression.NewGetField(0, sql.LongText, "", false))
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"trimmed string", sql.NewRow("foo"), "foo", false},
		{"spaces in both sides", sql.NewRow("  foo    "), "foo    ", false},
		{"spaces in left side", sql.NewRow("  foo"), "foo", false},
		{"spaces in right side", sql.NewRow("foo    "), "foo    ", false},
		{"two words with spaces", sql.NewRow(" foo   bar "), "foo   bar ", false},
		{"different kinds of spaces", sql.NewRow("\r\tfoo   bar \v"), "foo   bar \v", false},
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

func TestRTrim(t *testing.T) {
	f := NewTrimFunc(rTrimType)(expression.NewGetField(0, sql.LongText, "", false))
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, false},
		{"trimmed string", sql.NewRow("foo"), "foo", false},
		{"spaces in both sides", sql.NewRow("  foo    "), "  foo", false},
		{"spaces in left side", sql.NewRow("  foo"), "  foo", false},
		{"spaces in right side", sql.NewRow("foo    "), "foo", false},
		{"two words with spaces", sql.NewRow(" foo   bar "), " foo   bar", false},
		{"different kinds of spaces", sql.NewRow("\r\tfoo   bar \v"), "\r\tfoo   bar", false},
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
