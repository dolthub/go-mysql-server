package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

func TestIf(t *testing.T) {
	testCases := []struct {
		expr     sql.Expression
		row      sql.Row
		expected interface{}
	}{
		{eq(lit(1, sql.Int64), lit(1, sql.Int64)), sql.Row{"a", "b"}, "a"},
		{eq(lit(1, sql.Int64), lit(0, sql.Int64)), sql.Row{"a", "b"}, "b"},
		{eq(lit(1, sql.Int64), lit(1, sql.Int64)), sql.Row{1, 2}, 1},
		{eq(lit(1, sql.Int64), lit(0, sql.Int64)), sql.Row{1, 2}, 2},
		{eq(lit(nil, sql.Int64), lit(1, sql.Int64)), sql.Row{"a", "b"}, "b"},
		{eq(lit(1, sql.Int64), lit(1, sql.Int64)), sql.Row{nil, "b"}, nil},
	}

	for _, tc := range testCases {
		f := NewIf(
			tc.expr,
			expression.NewGetField(0, sql.LongText, "true", true),
			expression.NewGetField(1, sql.LongText, "false", true),
		)

		v, err := f.Eval(sql.NewEmptyContext(), tc.row)
		require.NoError(t, err)
		require.Equal(t, tc.expected, v)
	}
}

func eq(left, right sql.Expression) sql.Expression {
	return expression.NewEquals(left, right)
}

func lit(n interface{}, typ sql.Type) sql.Expression {
	return expression.NewLiteral(n, typ)
}

func col(idx int, typ sql.Type, table, col string) sql.Expression {
	return expression.NewGetFieldWithTable(idx, typ, table, col, false)
}
