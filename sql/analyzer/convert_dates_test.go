package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/memory"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/expression/function"
	"github.com/liquidata-inc/go-mysql-server/sql/expression/function/aggregation"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func TestConvertDates(t *testing.T) {
	testCases := []struct {
		name string
		in   sql.Expression
		out  sql.Expression
	}{
		{
			"arithmetic with dates",
			expression.NewPlus(expression.NewLiteral("", sql.Timestamp), expression.NewLiteral("", sql.Timestamp)),
			expression.NewPlus(
				expression.NewConvert(
					expression.NewLiteral("", sql.Timestamp),
					expression.ConvertToDatetime,
				),
				expression.NewConvert(
					expression.NewLiteral("", sql.Timestamp),
					expression.ConvertToDatetime,
				),
			),
		},
		{
			"star",
			expression.NewStar(),
			expression.NewStar(),
		},
		{
			"default column",
			expression.NewDefaultColumn("foo"),
			expression.NewDefaultColumn("foo"),
		},
		{
			"convert to date",
			expression.NewConvert(
				expression.NewPlus(
					expression.NewLiteral("", sql.Timestamp),
					expression.NewLiteral("", sql.Timestamp),
				),
				expression.ConvertToDatetime,
			),
			expression.NewConvert(
				expression.NewPlus(
					expression.NewConvert(
						expression.NewLiteral("", sql.Timestamp),
						expression.ConvertToDatetime,
					),
					expression.NewConvert(
						expression.NewLiteral("", sql.Timestamp),
						expression.ConvertToDatetime,
					),
				),
				expression.ConvertToDatetime,
			),
		},
		{
			"min aggregation",
			aggregation.NewMin(
				expression.NewGetField(0, sql.Timestamp, "foo", false),
			),
			aggregation.NewMin(
				expression.NewConvert(
					expression.NewGetField(0, sql.Timestamp, "foo", false),
					expression.ConvertToDatetime,
				),
			),
		},
		{
			"max aggregation",
			aggregation.NewMax(
				expression.NewGetField(0, sql.Timestamp, "foo", false),
			),
			aggregation.NewMax(
				expression.NewConvert(
					expression.NewGetField(0, sql.Timestamp, "foo", false),
					expression.ConvertToDatetime,
				),
			),
		},
		{
			"convert to other type",
			expression.NewConvert(
				expression.NewLiteral("", sql.LongText),
				expression.ConvertToBinary,
			),
			expression.NewConvert(
				expression.NewLiteral("", sql.LongText),
				expression.ConvertToBinary,
			),
		},
		{
			"datetime col in alias",
			expression.NewAlias(
				expression.NewLiteral("", sql.Timestamp),
				"foo",
			),
			expression.NewAlias(
				expression.NewConvert(
					expression.NewLiteral("", sql.Timestamp),
					expression.ConvertToDatetime,
				),
				"foo",
			),
		},
		{
			"date col in alias",
			expression.NewAlias(
				expression.NewLiteral("", sql.Date),
				"foo",
			),
			expression.NewAlias(
				expression.NewConvert(
					expression.NewLiteral("", sql.Date),
					expression.ConvertToDate,
				),
				"foo",
			),
		},
		{
			"date add",
			newDateAdd(
				expression.NewLiteral("", sql.Timestamp),
				expression.NewInterval(expression.NewLiteral(int64(1), sql.Int64), "DAY"),
			),
			newDateAdd(
				expression.NewConvert(
					expression.NewLiteral("", sql.Timestamp),
					expression.ConvertToDatetime,
				),
				expression.NewInterval(expression.NewLiteral(int64(1), sql.Int64), "DAY"),
			),
		},
		{
			"date sub",
			newDateSub(
				expression.NewLiteral("", sql.Timestamp),
				expression.NewInterval(expression.NewLiteral(int64(1), sql.Int64), "DAY"),
			),
			newDateSub(
				expression.NewConvert(
					expression.NewLiteral("", sql.Timestamp),
					expression.ConvertToDatetime,
				),
				expression.NewInterval(expression.NewLiteral(int64(1), sql.Int64), "DAY"),
			),
		},
	}

	table := plan.NewResolvedTable(memory.NewTable("t", nil))

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			input := plan.NewProject([]sql.Expression{tt.in}, table)
			expected := plan.NewProject([]sql.Expression{tt.out}, table)
			result, err := convertDates(sql.NewEmptyContext(), nil, input)
			require.NoError(t, err)
			require.Equal(t, expected, result)
		})
	}
}

func TestConvertDatesProject(t *testing.T) {
	table := plan.NewResolvedTable(memory.NewTable("t", nil))
	input := plan.NewFilter(
		expression.NewEquals(
			expression.NewGetField(0, sql.Int64, "foo", false),
			expression.NewLiteral("2019-06-06 00:00:00", sql.LongText),
		),
		plan.NewProject([]sql.Expression{
			expression.NewGetField(0, sql.Timestamp, "foo", false),
		}, table),
	)
	expected := plan.NewFilter(
		expression.NewEquals(
			expression.NewGetField(0, sql.Int64, "foo", false),
			expression.NewLiteral("2019-06-06 00:00:00", sql.LongText),
		),
		plan.NewProject([]sql.Expression{
			expression.NewAlias(
				expression.NewConvert(
					expression.NewGetField(0, sql.Timestamp, "foo", false),
					expression.ConvertToDatetime,
				),
				"foo",
			),
		}, table),
	)

	result, err := convertDates(sql.NewEmptyContext(), nil, input)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestConvertDatesGroupBy(t *testing.T) {
	table := plan.NewResolvedTable(memory.NewTable("t", nil))
	input := plan.NewFilter(
		expression.NewEquals(
			expression.NewGetField(0, sql.Int64, "foo", false),
			expression.NewLiteral("2019-06-06 00:00:00", sql.LongText),
		),
		plan.NewGroupBy(
			[]sql.Expression{
				expression.NewGetField(0, sql.Timestamp, "foo", false),
			},
			[]sql.Expression{
				expression.NewGetField(0, sql.Timestamp, "foo", false),
			}, table,
		),
	)
	expected := plan.NewFilter(
		expression.NewEquals(
			expression.NewGetField(0, sql.Int64, "foo", false),
			expression.NewLiteral("2019-06-06 00:00:00", sql.LongText),
		),
		plan.NewGroupBy(
			[]sql.Expression{
				expression.NewAlias(
					expression.NewConvert(
						expression.NewGetField(0, sql.Timestamp, "foo", false),
						expression.ConvertToDatetime,
					),
					"foo",
				),
			},
			[]sql.Expression{
				expression.NewConvert(
					expression.NewGetField(0, sql.Timestamp, "foo", false),
					expression.ConvertToDatetime,
				),
			},
			table,
		),
	)

	result, err := convertDates(sql.NewEmptyContext(), nil, input)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func TestConvertDatesFieldReference(t *testing.T) {
	table := plan.NewResolvedTable(memory.NewTable("t", nil))
	input := plan.NewFilter(
		expression.NewEquals(
			expression.NewGetField(0, sql.Int64, "DAYOFWEEK(foo)", false),
			expression.NewLiteral("2019-06-06 00:00:00", sql.LongText),
		),
		plan.NewProject([]sql.Expression{
			function.NewDayOfWeek(
				expression.NewGetField(0, sql.Timestamp, "foo", false),
			),
		}, table),
	)
	expected := plan.NewFilter(
		expression.NewEquals(
			expression.NewGetField(0, sql.Int64, "DAYOFWEEK(convert(foo, datetime))", false),
			expression.NewLiteral("2019-06-06 00:00:00", sql.LongText),
		),
		plan.NewProject([]sql.Expression{
			function.NewDayOfWeek(
				expression.NewConvert(
					expression.NewGetField(0, sql.Timestamp, "foo", false),
					expression.ConvertToDatetime,
				),
			),
		}, table),
	)

	result, err := convertDates(sql.NewEmptyContext(), nil, input)
	require.NoError(t, err)
	require.Equal(t, expected, result)
}

func newDateAdd(l, r sql.Expression) sql.Expression {
	e, _ := function.NewDateAdd(l, r)
	return e
}

func newDateSub(l, r sql.Expression) sql.Expression {
	e, _ := function.NewDateSub(l, r)
	return e
}
