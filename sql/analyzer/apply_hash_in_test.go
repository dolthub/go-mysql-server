package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestApplyHashIn(t *testing.T) {
	require := require.New(t)

	rule := getRule("apply_hash_in")

	table := memory.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
		{Name: "b", Type: sql.Int64, Source: "foo"},
		{Name: "c", Type: sql.Int64, Source: "foo"},
	})

	hit, _ := expression.NewHashInTuple(
		expression.NewGetField(0, sql.Int64, "foo", false),
		expression.NewTuple(
			expression.NewLiteral(int64(2), sql.Int64),
			expression.NewLiteral(int64(1), sql.Int64),
			expression.NewLiteral(int64(0), sql.Int64),
		),
	)

	child := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
			expression.NewGetFieldWithTable(1, sql.Int64, "foo", "b", false),
			expression.NewGetFieldWithTable(2, sql.Int64, "foo", "c", false),
		},
		plan.NewResolvedTable(table, nil, nil),
	)

	tests := []struct {
		name     string
		node     sql.Node
		expected sql.Node
	}{
		{
			name: "filter with literals converted to hash in",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewGetField(0, sql.Int64, "foo", false),
					expression.NewTuple(
						expression.NewLiteral(int64(2), sql.Int64),
						expression.NewLiteral(int64(1), sql.Int64),
						expression.NewLiteral(int64(0), sql.Int64),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				hit,
				child,
			),
		},
		{
			name: "hash in preserves sibling expressions",
			node: plan.NewFilter(
				expression.NewAnd(
					expression.NewInTuple(
						expression.NewGetField(0, sql.Int64, "foo", false),
						expression.NewTuple(
							expression.NewLiteral(int64(2), sql.Int64),
							expression.NewLiteral(int64(1), sql.Int64),
							expression.NewLiteral(int64(0), sql.Int64),
						),
					),
					expression.NewEquals(
						expression.NewBindVar("foo_id"),
						expression.NewLiteral(int8(2), sql.Int8),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				expression.NewAnd(
					hit,
					expression.NewEquals(
						expression.NewBindVar("foo_id"),
						expression.NewLiteral(int8(2), sql.Int8),
					),
				),
				child,
			),
		},
		{
			name: "filter with tuple expression not selected",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewGetField(0, sql.Int64, "foo", false),
					expression.NewTuple(
						expression.NewTuple(expression.NewLiteral(int64(2), sql.Int64), expression.NewLiteral(int64(1), sql.Int64)),
						expression.NewTuple(expression.NewLiteral(int64(1), sql.Int64), expression.NewLiteral(int64(0), sql.Int64)),
						expression.NewTuple(expression.NewLiteral(int64(0), sql.Int64), expression.NewLiteral(int64(0), sql.Int64)),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				expression.NewInTuple(
					expression.NewGetField(0, sql.Int64, "foo", false),
					expression.NewTuple(
						expression.NewTuple(expression.NewLiteral(int64(2), sql.Int64), expression.NewLiteral(int64(1), sql.Int64)),
						expression.NewTuple(expression.NewLiteral(int64(1), sql.Int64), expression.NewLiteral(int64(0), sql.Int64)),
						expression.NewTuple(expression.NewLiteral(int64(0), sql.Int64), expression.NewLiteral(int64(0), sql.Int64)),
					),
				),
				child,
			),
		},
		{
			name: "filter with binding expression not selected",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewGetField(0, sql.Int64, "foo", false),
					expression.NewTuple(
						expression.NewLiteral(int64(2), sql.Int64),
						expression.NewLiteral(int64(1), sql.Int64),
						expression.NewBindVar("foo_id"),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				expression.NewInTuple(
					expression.NewGetField(0, sql.Int64, "foo", false),
					expression.NewTuple(
						expression.NewLiteral(int64(2), sql.Int64),
						expression.NewLiteral(int64(1), sql.Int64),
						expression.NewBindVar("foo_id"),
					),
				),
				child,
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := rule.Apply(sql.NewEmptyContext(), NewDefault(nil), test.node, nil)
			require.NoError(err)
			if test.expected != nil {
				require.Equal(test.expected, result)
			} else {
				require.Equal(test.node, result)
			}
		})
	}
}
