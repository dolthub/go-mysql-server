package function

import (
	"math"

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
	"is_binary":     sql.Function1(NewIsBinary),
	"substring":     sql.FunctionN(NewSubstring),
	"year":          sql.Function1(NewYear),
	"month":         sql.Function1(NewMonth),
	"day":           sql.Function1(NewDay),
	"weekday":       sql.Function1(NewWeekday),
	"hour":          sql.Function1(NewHour),
	"minute":        sql.Function1(NewMinute),
	"second":        sql.Function1(NewSecond),
	"dayofweek":     sql.Function1(NewDayOfWeek),
	"dayofyear":     sql.Function1(NewDayOfYear),
	"array_length":  sql.Function1(NewArrayLength),
	"split":         sql.Function2(NewSplit),
	"concat":        sql.FunctionN(NewConcat),
	"concat_ws":     sql.FunctionN(NewConcatWithSeparator),
	"lower":         sql.Function1(NewLower),
	"upper":         sql.Function1(NewUpper),
	"ceiling":       sql.Function1(NewCeil),
	"ceil":          sql.Function1(NewCeil),
	"floor":         sql.Function1(NewFloor),
	"round":         sql.FunctionN(NewRound),
	"coalesce":      sql.FunctionN(NewCoalesce),
	"json_extract":  sql.FunctionN(NewJSONExtract),
	"connection_id": sql.Function0(NewConnectionID),
	"soundex":       sql.Function1(NewSoundex),
	"ln":            sql.Function1(NewLogBaseFunc(float64(math.E))),
	"log2":          sql.Function1(NewLogBaseFunc(float64(2))),
	"log10":         sql.Function1(NewLogBaseFunc(float64(10))),
	"log":           sql.FunctionN(NewLog),
	"rpad":          sql.FunctionN(NewPadFunc(rPadType)),
	"lpad":          sql.FunctionN(NewPadFunc(lPadType)),
	"sqrt":          sql.Function1(NewSqrt),
	"pow":           sql.Function2(NewPower),
	"power":         sql.Function2(NewPower),
	"ltrim":         sql.Function1(NewTrimFunc(lTrimType)),
	"rtrim":         sql.Function1(NewTrimFunc(rTrimType)),
	"trim":          sql.Function1(NewTrimFunc(bTrimType)),
	"reverse":       sql.Function1(NewReverse),
	"repeat":        sql.Function2(NewRepeat),
	"replace":       sql.Function3(NewReplace),
}
