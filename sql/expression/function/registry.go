package function

import (
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression/function/aggregation"
)

var defaultFunctions = map[string]sql.Function{
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
	"is_binary": sql.Function1(NewIsBinary),
	"substring": sql.FunctionN(NewSubstring),
	"year":      sql.Function1(NewYear),
	"month":     sql.Function1(NewMonth),
	"day":       sql.Function1(NewDay),
	"hour":      sql.Function1(NewHour),
	"minute":    sql.Function1(NewMinute),
	"second":    sql.Function1(NewSecond),
	"dayofyear": sql.Function1(NewDayOfYear),
}

// RegisterDefaults registers the aggregations in the catalog.
func RegisterDefaults(c *sql.Catalog) error {
	for k, v := range defaultFunctions {
		c.RegisterFunction(k, v)
	}

	return nil
}
