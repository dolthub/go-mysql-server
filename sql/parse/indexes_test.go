package parse

import (
	"bufio"
	"strings"
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"

	"github.com/stretchr/testify/require"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestParseCreateIndex(t *testing.T) {
	testCases := []struct {
		query  string
		result sql.Node
		err    *errors.Kind
	}{
		{
			"CREATE INDEX 1nd3x ON foo(bar)",
			nil,
			errUnexpectedSyntax,
		},
		{
			"CREATE INDEX index ON foo-bar(baz)",
			nil,
			errUnexpectedSyntax,
		},
		{
			"CREATE INDEX idx ON foo(*)",
			nil,
			errInvalidIndexExpression,
		},
		{
			"CREATE INDEX idx ON foo(foo, fn(bar, baz))",
			nil,
			errUnexpectedSyntax,
		},
		{
			"CREATE INDEX idx ON foo(fn(bar, baz), foo)",
			nil,
			errUnexpectedSyntax,
		},
		{
			"CREATE INDEX idx ON foo(fn(bar, baz))",
			plan.NewCreateIndex(
				"idx",
				plan.NewUnresolvedTable("foo"),
				[]sql.Expression{
					expression.NewUnresolvedFunction(
						"fn", false,
						expression.NewUnresolvedColumn("bar"),
						expression.NewUnresolvedColumn("baz"),
					),
				},
				"",
				make(map[string]string),
			),
			nil,
		},
		{
			"CREATE INDEX idx ON foo(bar)",
			plan.NewCreateIndex(
				"idx",
				plan.NewUnresolvedTable("foo"),
				[]sql.Expression{expression.NewUnresolvedColumn("bar")},
				"",
				make(map[string]string),
			),
			nil,
		},
		{
			"CREATE INDEX idx ON foo(bar, baz)",
			plan.NewCreateIndex(
				"idx",
				plan.NewUnresolvedTable("foo"),
				[]sql.Expression{
					expression.NewUnresolvedColumn("bar"),
					expression.NewUnresolvedColumn("baz"),
				},
				"",
				make(map[string]string),
			),
			nil,
		},
		{
			"CREATE INDEX idx ON foo USING bar (baz)",
			plan.NewCreateIndex(
				"idx",
				plan.NewUnresolvedTable("foo"),
				[]sql.Expression{
					expression.NewUnresolvedColumn("baz"),
				},
				"bar",
				make(map[string]string),
			),
			nil,
		},
		{
			"CREATE INDEX idx ON foo USING bar",
			nil,
			errUnexpectedSyntax,
		},
		{
			"CREATE INDEX idx ON foo USING bar (baz) WITH (foo = bar)",
			plan.NewCreateIndex(
				"idx",
				plan.NewUnresolvedTable("foo"),
				[]sql.Expression{
					expression.NewUnresolvedColumn("baz"),
				},
				"bar",
				map[string]string{"foo": "bar"},
			),
			nil,
		},
		{
			"CREATE INDEX idx ON foo USING bar (baz) WITH (foo = bar, qux = 'mux')",
			plan.NewCreateIndex(
				"idx",
				plan.NewUnresolvedTable("foo"),
				[]sql.Expression{
					expression.NewUnresolvedColumn("baz"),
				},
				"bar",
				map[string]string{"foo": "bar", "qux": "mux"},
			),
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.query, func(t *testing.T) {
			require := require.New(t)

			result, err := parseCreateIndex(strings.ToLower(tt.query))
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.result, result)
			}
		})
	}
}

func TestReadValue(t *testing.T) {
	testCases := []struct {
		str      string
		expected string
		err      bool
	}{
		{`"foo bar"`, `foo bar`, false},
		{`"foo \" foo \" bar"`, `foo " foo " bar`, false},
		{`"foo \" foo " bar"`, `foo " foo `, false},
		{`'foo bar'`, `foo bar`, false},
		{`'foo \' foo \' bar'`, `foo ' foo ' bar`, false},
		{`'foo \' foo ' bar'`, `foo ' foo `, false},
		{`1234   `, `1234`, false},
		{`off   `, `off`, false},
		{`123off`, `123off`, false},
		{`{}`, ``, false},
	}

	for _, tt := range testCases {
		t.Run(tt.str, func(t *testing.T) {
			var value string
			err := readValue(&value)(bufio.NewReader(strings.NewReader(tt.str)))
			if !tt.err {
				require.NoError(t, err)
				require.Equal(t, tt.expected, value)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestReadExprs(t *testing.T) {
	testCases := []struct {
		str      string
		expected []string
		err      bool
	}{
		{`(foo('bar'))`, []string{"foo('bar')"}, false},
		{`(foo("bar"))`, []string{`foo("bar")`}, false},
		{`(foo('(()bar'))`, []string{"foo('(()bar')"}, false},
		{`(foo("(()bar"))`, []string{`foo("(()bar")`}, false},
		{`(foo("\""))`, []string{`foo("\"")`}, false},
		{`(foo("""))`, nil, true},
		{`(foo('\''))`, []string{`foo('\'')`}, false},
		{`(foo('''))`, nil, true},
	}

	for _, tt := range testCases {
		t.Run(tt.str, func(t *testing.T) {
			require := require.New(t)
			var exprs []string
			err := readExprs(&exprs)(bufio.NewReader(strings.NewReader(tt.str)))
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, exprs)
			}
		})
	}
}
