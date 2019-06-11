package aggregation

import (
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestLast(t *testing.T) {
	testCases := []struct {
		name     string
		rows     []sql.Row
		expected interface{}
	}{
		{"no rows", nil, nil},
		{"one row", []sql.Row{{"first"}}, "first"},
		{"three rows", []sql.Row{{"first"}, {"second"}, {"last"}}, "last"},
	}

	agg := NewLast(expression.NewGetField(0, sql.Text, "", false))
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			result := aggregate(t, agg, tt.rows...)
			require.Equal(t, tt.expected, result)
		})
	}
}
