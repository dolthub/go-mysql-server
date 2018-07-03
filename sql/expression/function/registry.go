package function

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression/function/aggregation"
)

// Defaults is the function map with all the default functions.
var Defaults = sql.Functions{
	"count": sql.Function1(func(e sql.Expression) sql.Expression {
		return aggregation.NewCount(e)
	}),
	"min": sql.Function1(func(e sql.Expression) sql.Expression {
		return aggregation.NewMin(e)
	}),
	"max": sql.Function1(func(e sql.Expression) sql.Expression {
		return aggregation.NewMax(e)
	}),
	"avg": sql.Function1(func(e sql.Expression) sql.Expression {
		return aggregation.NewAvg(e)
	}),
	"sum": sql.Function1(func(e sql.Expression) sql.Expression {
		return aggregation.NewSum(e)
	}),
	"is_binary":    sql.Function1(NewIsBinary),
	"substring":    sql.FunctionN(NewSubstring),
	"year":         sql.Function1(NewYear),
	"month":        sql.Function1(NewMonth),
	"day":          sql.Function1(NewDay),
	"hour":         sql.Function1(NewHour),
	"minute":       sql.Function1(NewMinute),
	"second":       sql.Function1(NewSecond),
	"dayofyear":    sql.Function1(NewDayOfYear),
	"array_length": sql.Function1(NewArrayLength),
	"split":        sql.Function2(NewSplit),
	"concat":       sql.FunctionN(NewConcat),
}
