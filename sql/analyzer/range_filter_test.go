package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestGetRangeFilters(t *testing.T) {
	tests := []struct {
		name              string
		filterExpressions []sql.Expression
		expectedRanges    []rangeFilter
	}{
		{
			name: "simple Between test",
			filterExpressions: []sql.Expression{
				expression.NewBetween(
					expression.NewBindVar("x"),
					expression.NewLiteral(0, types.Int64),
					expression.NewLiteral(10, types.Int64),
				),
			},
			expectedRanges: []rangeFilter{
				{
					value:              expression.NewBindVar("x"),
					min:                expression.NewLiteral(0, types.Int64),
					max:                expression.NewLiteral(10, types.Int64),
					closedOnLowerBound: true,
					closedOnUpperBound: true,
				},
			},
		},
		{
			name: "simple less than / greater than test",
			filterExpressions: []sql.Expression{
				expression.NewGreaterThan(
					expression.NewBindVar("y"),
					expression.NewLiteral(11, types.Int64),
				),
				expression.NewLessThan(
					expression.NewBindVar("y"),
					expression.NewLiteral(20, types.Int64),
				),
			},
			expectedRanges: []rangeFilter{
				{
					value:              expression.NewBindVar("y"),
					min:                expression.NewLiteral(11, types.Int64),
					max:                expression.NewLiteral(20, types.Int64),
					closedOnLowerBound: false,
					closedOnUpperBound: false,
				},
			},
		},
		{
			name: "simple greater than / greater than test",
			filterExpressions: []sql.Expression{
				expression.NewGreaterThanOrEqual(
					expression.NewBindVar("z"),
					expression.NewLiteral(21, types.Int64),
				),
				expression.NewGreaterThanOrEqual(
					expression.NewLiteral(30, types.Int64),
					expression.NewBindVar("z"),
				),
			},
			expectedRanges: []rangeFilter{
				{
					value:              expression.NewBindVar("z"),
					min:                expression.NewLiteral(21, types.Int64),
					max:                expression.NewLiteral(30, types.Int64),
					closedOnLowerBound: true,
					closedOnUpperBound: true,
				},
			},
		},
		{
			name: "multiple ranges",
			filterExpressions: []sql.Expression{
				expression.NewLessThanOrEqual(
					expression.NewBindVar("a"),
					expression.NewLiteral(40, types.Int64),
				),
				expression.NewLessThan(
					expression.NewLiteral(31, types.Int64),
					expression.NewBindVar("a"),
				),
				expression.NewLessThan(
					expression.NewBindVar("b"),
					expression.NewLiteral(50, types.Int64),
				),
				expression.NewLessThanOrEqual(
					expression.NewLiteral(41, types.Int64),
					expression.NewBindVar("b"),
				),
			},
			expectedRanges: []rangeFilter{
				{
					value:              expression.NewBindVar("a"),
					min:                expression.NewLiteral(31, types.Int64),
					max:                expression.NewLiteral(40, types.Int64),
					closedOnLowerBound: false,
					closedOnUpperBound: true,
				},
				{
					value:              expression.NewBindVar("b"),
					min:                expression.NewLiteral(41, types.Int64),
					max:                expression.NewLiteral(50, types.Int64),
					closedOnLowerBound: true,
					closedOnUpperBound: false,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := getRangeFilters(tt.filterExpressions)
			require.ElementsMatch(t, tt.expectedRanges, res)
		})
	}
}
