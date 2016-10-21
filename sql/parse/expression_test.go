package parse

import (
	"testing"

	"github.com/mvader/gitql/sql"
	"github.com/mvader/gitql/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestAssembleExpression(t *testing.T) {
	cases := []struct {
		input  []*Token
		result sql.Expression
	}{
		{
			[]*Token{tk(IdentifierToken, "foo")},
			expression.NewUnresolvedColumn("foo"),
		},
		{
			[]*Token{tk(StringToken, `"foo"`)},
			expression.NewLiteral("foo", sql.String),
		},
		{
			[]*Token{tk(StringToken, `'foo'`)},
			expression.NewLiteral("foo", sql.String),
		},
		{
			[]*Token{tk(IntToken, `42`)},
			expression.NewLiteral(int64(42), sql.BigInteger),
		},
		// FIXME equals operator not working
		//{
		//	[]*Token{tk(FloatToken, `42.42`)},
		//	expression.NewLiteral(float32(42.42), sql.Float),
		//},
		{
			[]*Token{tk(IdentifierToken, `true`)},
			expression.NewLiteral(true, sql.Boolean),
		},
		{
			[]*Token{tk(IdentifierToken, `false`)},
			expression.NewLiteral(false, sql.Boolean),
		},
		{
			[]*Token{
				tk(IdentifierToken, "foo"),
				tk(OpToken, "not"),
			},
			expression.NewNot(expression.NewUnresolvedColumn("foo")),
		},
		{
			[]*Token{
				tk(IntToken, "42"),
				tk(IdentifierToken, "foo"),
				tk(OpToken, "="),
			},
			expression.NewEquals(
				expression.NewLiteral(int64(42), sql.BigInteger),
				expression.NewUnresolvedColumn("foo"),
			),
		},
	}

	for _, c := range cases {
		var stack = tokenStack(c.input)
		require.Equal(t, c.result, noErr(assembleExpression(&stack)))
	}
}

func tk(typ TokenType, val string) *Token {
	return &Token{
		Value: val,
		Type:  typ,
	}
}

func noErr(expr sql.Expression, err error) sql.Expression {
	return expr
}
