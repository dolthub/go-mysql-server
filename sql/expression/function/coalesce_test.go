package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestEmptyCoalesce(t *testing.T) {
	_, err := NewCoalesce()
	require.True(t, sql.ErrInvalidArgumentNumber.Is(err))
}

func TestCoalesce(t *testing.T) {
	testCases := []struct {
		name     string
		input    []sql.Expression
		expected interface{}
		typ      sql.Type
		nullable bool
	}{
		{"coalesce(1, 2, 3)", []sql.Expression{expression.NewLiteral(1, sql.Int32), expression.NewLiteral(2, sql.Int32), expression.NewLiteral(3, sql.Int32)}, 1, sql.Int32, false},
		{"coalesce(NULL, NULL, 3)", []sql.Expression{nil, nil, expression.NewLiteral(3, sql.Int32)}, 3, sql.Int32, false},
		{"coalesce(NULL, NULL, '3')", []sql.Expression{nil, nil, expression.NewLiteral("3", sql.Text)}, "3", sql.Text, false},
		{"coalesce(NULL, '2', 3)", []sql.Expression{nil, expression.NewLiteral("2", sql.Text), expression.NewLiteral(3, sql.Int32)}, "2", sql.Text, false},
		{"coalesce(NULL, NULL, NULL)", []sql.Expression{nil, nil, nil}, nil, nil, true},
	}

	for _, tt := range testCases {
		c, err := NewCoalesce(tt.input...)
		require.NoError(t, err)

		require.Equal(t, tt.typ, c.Type())
		require.Equal(t, tt.nullable, c.IsNullable())
		v, err := c.Eval(sql.NewEmptyContext(), nil)
		require.NoError(t, err)
		require.Equal(t, tt.expected, v)
	}
}

func TestComposeCoalasce(t *testing.T) {
	c1, err := NewCoalesce(nil)
	require.NoError(t, err)
	require.Equal(t, nil, c1.Type())
	v, err := c1.Eval(sql.NewEmptyContext(), nil)
	require.NoError(t, err)
	require.Equal(t, nil, v)

	c2, err := NewCoalesce(nil, expression.NewLiteral(1, sql.Int32))
	require.NoError(t, err)
	require.Equal(t, sql.Int32, c2.Type())
	v, err = c2.Eval(sql.NewEmptyContext(), nil)
	require.NoError(t, err)
	require.Equal(t, 1, v)

	c, err := NewCoalesce(nil, c1, c2)
	require.NoError(t, err)
	require.Equal(t, sql.Int32, c.Type())
	v, err = c.Eval(sql.NewEmptyContext(), nil)
	require.NoError(t, err)
	require.Equal(t, 1, v)
}
