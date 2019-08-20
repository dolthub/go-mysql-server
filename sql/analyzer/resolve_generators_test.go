package analyzer

import (
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/src-d/go-mysql-server/sql/expression/function"
	"github.com/src-d/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"
)

func TestResolveGenerators(t *testing.T) {
	testCases := []struct {
		name     string
		node     sql.Node
		expected sql.Node
		err      *errors.Kind
	}{
		{
			name: "regular explode",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(0, sql.Int64, "a", false),
					function.NewExplode(expression.NewGetField(1, sql.Array(sql.Int64), "b", false)),
					expression.NewGetField(2, sql.Int64, "c", false),
				},
				plan.NewUnresolvedTable("foo", ""),
			),
			expected: plan.NewGenerate(
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, sql.Int64, "a", false),
						function.NewGenerate(expression.NewGetField(1, sql.Array(sql.Int64), "b", false)),
						expression.NewGetField(2, sql.Int64, "c", false),
					},
					plan.NewUnresolvedTable("foo", ""),
				),
				expression.NewGetField(1, sql.Array(sql.Int64), "EXPLODE(b)", false),
			),
			err: nil,
		},
		{
			name: "explode with alias",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(0, sql.Int64, "a", false),
					expression.NewAlias(
						function.NewExplode(
							expression.NewGetField(1, sql.Array(sql.Int64), "b", false),
						),
						"x",
					),
					expression.NewGetField(2, sql.Int64, "c", false),
				},
				plan.NewUnresolvedTable("foo", ""),
			),
			expected: plan.NewGenerate(
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, sql.Int64, "a", false),
						expression.NewAlias(
							function.NewGenerate(
								expression.NewGetField(1, sql.Array(sql.Int64), "b", false),
							),
							"x",
						),
						expression.NewGetField(2, sql.Int64, "c", false),
					},
					plan.NewUnresolvedTable("foo", ""),
				),
				expression.NewGetField(1, sql.Array(sql.Int64), "x", false),
			),
			err: nil,
		},
		{
			name: "non array type on explode",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(0, sql.Int64, "a", false),
					function.NewExplode(expression.NewGetField(1, sql.Int64, "b", false)),
				},
				plan.NewUnresolvedTable("foo", ""),
			),
			expected: nil,
			err:      errExplodeNotArray,
		},
		{
			name: "more than one generator",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(0, sql.Int64, "a", false),
					function.NewExplode(expression.NewGetField(1, sql.Array(sql.Int64), "b", false)),
					function.NewExplode(expression.NewGetField(2, sql.Array(sql.Int64), "c", false)),
				},
				plan.NewUnresolvedTable("foo", ""),
			),
			expected: nil,
			err:      errMultipleGenerators,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := resolveGenerators(sql.NewEmptyContext(), nil, tt.node)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}
}
