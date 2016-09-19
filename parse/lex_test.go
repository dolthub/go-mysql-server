package parse

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLexNumber(t *testing.T) {
	cases := []lexCase{
		{"12", "12", IntToken},
		{"12.45", "12.45", FloatToken},
		{"12.45.", "", ErrorToken},
		{"1dkejrw", "", ErrorToken},
	}

	testLex(t, cases, lexNumber)
}

func TestLexIdentifier(t *testing.T) {
	cases := []lexCase{
		{"select *", "select", KeywordToken},
		{"p.name", "p", IdentifierToken},
	}

	testLex(t, cases, lexIdentifier)
}

func TestLexOp(t *testing.T) {
	cases := []lexCase{
		{"= 5", "=", OpToken},
		{"AND 5", "", ErrorToken},
		{">= foo", ">=", OpToken},
		{"***", "", ErrorToken},
	}

	testLex(t, cases, lexOp)
}

func TestLexQuote(t *testing.T) {
	cases := []lexCase{
		{`foo bar", `, `foo bar"`, StringToken},
		{`foo \tar", `, `foo \tar"`, StringToken},
		{`foo \"\"bar", `, `foo \"\"bar"`, StringToken},
		{`foo bar`, ``, EOFToken},
	}

	testLex(t, cases, lexQuote)
}

func TestLexSingleQuote(t *testing.T) {
	cases := []lexCase{
		{`foo bar', `, `foo bar'`, StringToken},
		{`foo \tar', `, `foo \tar'`, StringToken},
		{`foo \'\'bar', `, `foo \'\'bar'`, StringToken},
		{`foo bar`, ``, EOFToken},
	}

	testLex(t, cases, lexSingleQuote)
}

const line = `
SELECT b.foo, b.bar
FROM baz AS b
WHERE (b.a = 'foo') AND (b.c > 1) ORDER BY id DESC;
`

func TestLexLine(t *testing.T) {
	expected := []struct {
		typ TokenType
		val string
	}{
		{KeywordToken, "SELECT"},
		{IdentifierToken, "b"},
		{DotToken, "."},
		{IdentifierToken, "foo"},
		{CommaToken, ","},
		{IdentifierToken, "b"},
		{DotToken, "."},
		{IdentifierToken, "bar"},
		{KeywordToken, "FROM"},
		{IdentifierToken, "baz"},
		{KeywordToken, "AS"},
		{IdentifierToken, "b"},
		{KeywordToken, "WHERE"},
		{LeftParenToken, "("},
		{IdentifierToken, "b"},
		{DotToken, "."},
		{IdentifierToken, "a"},
		{OpToken, "="},
		{StringToken, "'foo'"},
		{RightParenToken, ")"},
		{KeywordToken, "AND"},
		{LeftParenToken, "("},
		{IdentifierToken, "b"},
		{DotToken, "."},
		{IdentifierToken, "c"},
		{OpToken, ">"},
		{IntToken, "1"},
		{RightParenToken, ")"},
		{KeywordToken, "ORDER"},
		{KeywordToken, "BY"},
		{IdentifierToken, "id"},
		{KeywordToken, "DESC"},
		{EOFToken, ";"},
	}

	l := NewLexer(strings.NewReader(line))
	assert.Nil(t, l.Run())

	for _, e := range expected {
		tk := l.Next()
		assert.NotNil(t, tk)
		assert.Equal(t, e.typ, tk.Type)
		assert.Equal(t, e.val, tk.Value)
	}
}

type lexCase struct {
	input    string
	expected string
	typ      TokenType
}

func testLex(t *testing.T, cases []lexCase, fn stateFunc) {
	for _, c := range cases {
		l := NewLexer(strings.NewReader(c.input + " "))
		_, err := fn(l)

		if c.typ == EOFToken {
			assert.Equal(t, io.EOF, err)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, 1, len(l.tokens))
			tk := l.Next()
			assert.NotNil(t, tk)
			assert.Equal(t, c.typ, tk.Type)

			if c.typ != ErrorToken {
				assert.Equal(t, c.expected, tk.Value)
			}
		}
	}
}
