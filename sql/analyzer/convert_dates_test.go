package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression/function"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
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
			"convert to other type",
			expression.NewConvert(
				expression.NewLiteral("", sql.Text),
				expression.ConvertToBinary,
			),
			expression.NewConvert(
				expression.NewLiteral("", sql.Text),
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

	table := plan.NewResolvedTable(mem.NewTable("t", nil))

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

func newDateAdd(l, r sql.Expression) sql.Expression {
	e, _ := function.NewDateAdd(l, r)
	return e
}

func newDateSub(l, r sql.Expression) sql.Expression {
	e, _ := function.NewDateSub(l, r)
	return e
}
