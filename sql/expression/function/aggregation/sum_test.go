package aggregation

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestSum(t *testing.T) {
	sum := NewSum(expression.NewGetField(0, nil, "", false))

	testCases := []struct {
		name     string
		rows     []sql.Row
		expected interface{}
	}{
		{
			"string int values",
			[]sql.Row{{"1"}, {"2"}, {"3"}, {"4"}},
			float64(10),
		},
		{
			"string float values",
			[]sql.Row{{"1.5"}, {"2"}, {"3"}, {"4"}},
			float64(10.5),
		},
		{
			"string non-int values",
			[]sql.Row{{"a"}, {"b"}, {"c"}, {"d"}},
			float64(0),
		},
		{
			"float values",
			[]sql.Row{{1.}, {2.5}, {3.}, {4.}},
			float64(10.5),
		},
		{
			"no rows",
			[]sql.Row{},
			nil,
		},
		{
			"nil values",
			[]sql.Row{{nil}, {nil}},
			nil,
		},
		{
			"int64 values",
			[]sql.Row{{int64(1)}, {int64(3)}},
			float64(4),
		},
		{
			"int32 values",
			[]sql.Row{{int32(1)}, {int32(3)}},
			float64(4),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			buf := sum.NewBuffer()
			for _, row := range tt.rows {
				require.NoError(sum.Update(sql.NewEmptyContext(), buf, row))
			}

			result, err := sum.Eval(sql.NewEmptyContext(), buf)
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}
