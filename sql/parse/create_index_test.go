package parse

import (
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
