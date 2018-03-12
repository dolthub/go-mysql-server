package expression

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestIsBinary(t *testing.T) {
	f := NewIsBinary(NewGetField(0, sql.Blob, "blob", true))

	testCases := []struct {
		name     string
		row      sql.Row
		expected bool
	}{
		{"binary", sql.NewRow([]byte{0, 1, 2}), true},
		{"not binary", sql.NewRow([]byte{1, 2, 3}), false},
		{"null", sql.NewRow(nil), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, f, tt.row))
		})
	}
}

func TestSubstringArity(t *testing.T) {
	expr := NewGetField(0, sql.Int64, "foo", false)
	testCases := []struct {
		name string
		args []sql.Expression
		ok   bool
	}{
		{"0 args", nil, false},
		{"1 args", []sql.Expression{expr}, false},
		{"2 args", []sql.Expression{expr, expr}, true},
		{"3 args", []sql.Expression{expr, expr, expr}, true},
		{"4 args", []sql.Expression{expr, expr, expr, expr}, false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			f, err := NewSubstring(tt.args...)
			if tt.ok {
				require.NotNil(f)
				require.NoError(err)
			} else {
				require.Error(err)
			}
		})
	}
}

func TestSubstring(t *testing.T) {
	f, err := NewSubstring(
		NewGetField(0, sql.Text, "str", true),
		NewGetField(1, sql.Int32, "start", false),
		NewGetField(2, sql.Int64, "len", false),
	)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null string", sql.NewRow(nil, 1, 1), nil, false},
		{"null start", sql.NewRow("foo", nil, 1), nil, false},
		{"null len", sql.NewRow("foo", 1, nil), nil, false},
		{"negative start", sql.NewRow("foo", -1, 10), "o", false},
		{"negative length", sql.NewRow("foo", 1, -1), "", false},
		{"length 0", sql.NewRow("foo", 1, 0), "", false},
		{"start bigger than string", sql.NewRow("foo", 50, 10), "", false},
		{"negative start bigger than string", sql.NewRow("foo", -4, 10), "", false},
		{"length overflows", sql.NewRow("foo", 2, 10), "oo", false},
		{"length overflows by one", sql.NewRow("foo", 2, 2), "oo", false},
		{"substring contained", sql.NewRow("foo", 1, 2), "fo", false},
		{"negative start until str beginning", sql.NewRow("foo", -3, 2), "fo", false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			session := sql.NewBaseSession(context.TODO())

			v, err := f.Eval(session, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)
			}
		})
	}
}
