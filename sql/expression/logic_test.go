package expression

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestAnd(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right interface{}
		expected    interface{}
	}{
		{"left is true, right is false", true, false, false},
		{"left is true, right is null", true, nil, nil},
		{"left is false, right is true", false, true, false},
		{"left is null, right is true", nil, true, nil},
		{"left is false, right is null", false, nil, false},
		{"left is null, right is false", nil, false, false},
		{"both true", true, true, true},
		{"both false", false, false, false},
		{"both nil", nil, nil, nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewAnd(
				NewLiteral(tt.left, sql.Boolean),
				NewLiteral(tt.right, sql.Boolean),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestOr(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right interface{}
		expected    interface{}
	}{
		{"left is true, right is false", true, false, true},
		{"left is null, right is not", nil, true, true},
		{"left is false, right is true", false, true, true},
		{"right is null, left is not", true, nil, true},
		{"both true", true, true, true},
		{"both false", false, false, false},
		{"both null", nil, nil, nil},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewOr(
				NewLiteral(tt.left, sql.Boolean),
				NewLiteral(tt.right, sql.Boolean),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestJoinAnd(t *testing.T) {
	require := require.New(t)

	require.Nil(JoinAnd())

	require.Equal(
		NewNot(nil),
		JoinAnd(NewNot(nil)),
	)

	require.Equal(
		NewAnd(
			NewAnd(
				NewIsNull(nil),
				NewEquals(nil, nil),
			),
			NewNot(nil),
		),
		JoinAnd(
			NewIsNull(nil),
			NewEquals(nil, nil),
			NewNot(nil),
		),
	)
}
