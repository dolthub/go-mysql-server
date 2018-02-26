package expression

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestAnd(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right bool
		expected    bool
	}{
		{"left is true, right is false", true, false, false},
		{"left is false, right is true", false, true, false},
		{"both true", true, true, true},
		{"both false", false, false, false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := NewAnd(
				NewLiteral(tt.left, sql.Boolean),
				NewLiteral(tt.right, sql.Boolean),
			).Eval(sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestOr(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right bool
		expected    bool
	}{
		{"left is true, right is false", true, false, true},
		{"left is false, right is true", false, true, true},
		{"both true", true, true, true},
		{"both false", false, false, false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := NewOr(
				NewLiteral(tt.left, sql.Boolean),
				NewLiteral(tt.right, sql.Boolean),
			).Eval(sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}
