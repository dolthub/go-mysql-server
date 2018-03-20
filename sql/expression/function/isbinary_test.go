package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestIsBinary(t *testing.T) {
	f := NewIsBinary(expression.NewGetField(0, sql.Blob, "blob", true))

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
	expr := expression.NewGetField(0, sql.Int64, "foo", false)
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
