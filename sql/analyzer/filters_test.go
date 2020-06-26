package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

func TestFiltersMerge(t *testing.T) {
	f1 := filtersByTable{
		"1": []sql.Expression{
			expression.NewLiteral("1", sql.LongText),
		},
		"2": []sql.Expression{
			expression.NewLiteral("2", sql.LongText),
		},
	}

	f2 := filtersByTable{
		"2": []sql.Expression{
			expression.NewLiteral("2.2", sql.LongText),
		},
		"3": []sql.Expression{
			expression.NewLiteral("3", sql.LongText),
		},
	}

	f1.merge(f2)

	require.Equal(t,
		filtersByTable{
			"1": []sql.Expression{
				expression.NewLiteral("1", sql.LongText),
			},
			"2": []sql.Expression{
				expression.NewLiteral("2", sql.LongText),
				expression.NewLiteral("2.2", sql.LongText),
			},
			"3": []sql.Expression{
				expression.NewLiteral("3", sql.LongText),
			},
		},
		f1,
	)
}

func TestSplitExpression(t *testing.T) {
	e := expression.NewAnd(
		expression.NewAnd(
			expression.NewIsNull(expression.NewUnresolvedColumn("foo")),
			expression.NewNot(expression.NewUnresolvedColumn("foo")),
		),
		expression.NewAnd(
			expression.NewOr(
				expression.NewIsNull(expression.NewUnresolvedColumn("bar")),
				expression.NewNot(expression.NewUnresolvedColumn("bar")),
			),
			expression.NewEquals(
				expression.NewUnresolvedColumn("foo"),
				expression.NewLiteral("foo", sql.LongText),
			),
		),
	)

	expected := []sql.Expression{
		expression.NewIsNull(expression.NewUnresolvedColumn("foo")),
		expression.NewNot(expression.NewUnresolvedColumn("foo")),
		expression.NewOr(
			expression.NewIsNull(expression.NewUnresolvedColumn("bar")),
			expression.NewNot(expression.NewUnresolvedColumn("bar")),
		),
		expression.NewEquals(
			expression.NewUnresolvedColumn("foo"),
			expression.NewLiteral("foo", sql.LongText),
		),
	}

	require.Equal(t,
		expected,
		splitConjunction(e),
	)
}

func TestSubtractExprSet(t *testing.T) {
	filters := []sql.Expression{
		expression.NewIsNull(nil),
		expression.NewNot(nil),
		expression.NewEquals(nil, nil),
		expression.NewGreaterThan(nil, nil),
	}

	handled := []sql.Expression{
		filters[1],
		filters[3],
	}

	unhandled := subtractExprSet(filters, handled)

	require.Equal(t,
		[]sql.Expression{filters[0], filters[2]},
		unhandled,
	)
}

func TestExprToTableFilters(t *testing.T) {
	require := require.New(t)
	expr := expression.NewAnd(
		expression.NewAnd(
			expression.NewAnd(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
					expression.NewLiteral(3.14, sql.Float64),
				),
				expression.NewGreaterThan(
					expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
					expression.NewLiteral(3., sql.Float64),
				),
			),
			expression.NewIsNull(
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable2", "i2", false),
			),
		),
		expression.NewOr(
			expression.NewEquals(
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
				expression.NewLiteral(3.14, sql.Float64),
			),
			expression.NewGreaterThan(
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
				expression.NewLiteral(3., sql.Float64),
			),
		),
	)

	expected := filtersByTable{
		"mytable": []sql.Expression{
			expression.NewEquals(
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
				expression.NewLiteral(3.14, sql.Float64),
			),
			expression.NewGreaterThan(
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
				expression.NewLiteral(3., sql.Float64),
			),
			expression.NewOr(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
					expression.NewLiteral(3.14, sql.Float64),
				),
				expression.NewGreaterThan(
					expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
					expression.NewLiteral(3., sql.Float64),
				),
			),
		},
		"mytable2": []sql.Expression{
			expression.NewIsNull(
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable2", "i2", false),
			),
		},
	}

	require.Equal(expected, exprToTableFilters(expr))
}
