package parse

import (
	"testing"

	"github.com/stretchr/testify/require"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestParseDescribeQuery(t *testing.T) {
	testCases := []struct {
		query  string
		result sql.Node
		err    *errors.Kind
	}{
		{
			"DESCRIBE TABLE foo",
			nil,
			errUnexpectedSyntax,
		},
		{
			"DESCRIBE something",
			nil,
			errUnexpectedSyntax,
		},
		{
			"DESCRIBE FORMAT=pretty SELECT * FROM foo",
			nil,
			errInvalidDescribeFormat,
		},
		{
			"DESCRIBE FORMAT=tree SELECT * FROM foo",
			plan.NewDescribeQuery("tree", plan.NewProject(
				[]sql.Expression{expression.NewStar()},
				plan.NewUnresolvedTable("foo", "")),
			),
			nil,
		},
		{
			"DESC FORMAT=tree SELECT * FROM foo",
			plan.NewDescribeQuery("tree", plan.NewProject(
				[]sql.Expression{expression.NewStar()},
				plan.NewUnresolvedTable("foo", "")),
			),
			nil,
		},
		{
			"EXPLAIN FORMAT=tree SELECT * FROM foo",
			plan.NewDescribeQuery("tree", plan.NewProject(
				[]sql.Expression{expression.NewStar()},
				plan.NewUnresolvedTable("foo", "")),
			),
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.query, func(t *testing.T) {
			require := require.New(t)

			result, err := parseDescribeQuery(sql.NewEmptyContext(), tt.query)
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
