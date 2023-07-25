package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql/memo"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func variable(name string) *memo.ExprGroup {
	return &memo.ExprGroup{Scalar: &memo.Bindvar{
		Name: name,
		Typ:  types.Int8,
	}}
}

func literal(value int) *memo.ExprGroup {
	return &memo.ExprGroup{Scalar: &memo.Literal{
		Val: value,
		Typ: types.Int8,
	}}
}

func TestHintParsing(t *testing.T) {
	tests := []struct {
		name              string
		filterExpressions []memo.ScalarExpr
		expectedRanges    []rangeFilter
	}{
		{
			name: "simple Between test",
			filterExpressions: []memo.ScalarExpr{
				&memo.Between{
					Value: variable("x"),
					Min:   literal(0),
					Max:   literal(10),
				},
			},
			expectedRanges: []rangeFilter{
				{
					value:              variable("x"),
					min:                literal(0),
					max:                literal(10),
					closedOnLowerBound: true,
					closedOnUpperBound: true,
				},
			},
		},
		{
			name: "simple less than / greater than test",
			filterExpressions: []memo.ScalarExpr{
				&memo.Gt{
					Left:  variable("y"),
					Right: literal(11),
				},
				&memo.Lt{
					Left:  variable("y"),
					Right: literal(20),
				},
			},
			expectedRanges: []rangeFilter{
				{
					value:              variable("y"),
					min:                literal(11),
					max:                literal(20),
					closedOnLowerBound: false,
					closedOnUpperBound: false,
				},
			},
		},
		{
			name: "simple greater than / greater than test",
			filterExpressions: []memo.ScalarExpr{
				&memo.Geq{
					Left:  variable("z"),
					Right: literal(21),
				},
				&memo.Geq{
					Left:  literal(30),
					Right: variable("z"),
				},
			},
			expectedRanges: []rangeFilter{
				{
					value:              variable("z"),
					min:                literal(21),
					max:                literal(30),
					closedOnLowerBound: true,
					closedOnUpperBound: true,
				},
			},
		},
		{
			name: "multiple ranges",
			filterExpressions: []memo.ScalarExpr{
				&memo.Leq{
					Left:  variable("a"),
					Right: literal(40),
				},
				&memo.Lt{
					Left:  literal(31),
					Right: variable("a"),
				},
				&memo.Lt{
					Left:  variable("b"),
					Right: literal(50),
				},
				&memo.Leq{
					Left:  literal(41),
					Right: variable("b"),
				},
			},
			expectedRanges: []rangeFilter{
				{
					value:              variable("a"),
					min:                literal(31),
					max:                literal(40),
					closedOnLowerBound: false,
					closedOnUpperBound: true,
				},
				{
					value:              variable("b"),
					min:                literal(41),
					max:                literal(50),
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
