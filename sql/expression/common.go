package expression

import "gopkg.in/src-d/go-mysql-server.v0/sql"

// UnaryExpression is an expression that has only one children.
type UnaryExpression struct {
	Child sql.Expression
}

// Resolved implements the Expression interface.
func (p UnaryExpression) Resolved() bool {
	return p.Child.Resolved()
}

// IsNullable returns whether the expression can be null.
func (p UnaryExpression) IsNullable() bool {
	return p.Child.IsNullable()
}

// BinaryExpression is an expression that has two children.
type BinaryExpression struct {
	Left  sql.Expression
	Right sql.Expression
}

// Resolved implements the Expression interface.
func (p BinaryExpression) Resolved() bool {
	return p.Left.Resolved() && p.Right.Resolved()
}

// IsNullable returns whether the expression can be null.
func (p BinaryExpression) IsNullable() bool {
	return p.Left.IsNullable() || p.Right.IsNullable()
}

var defaultFunctions = map[string]sql.Function{
	"count": sql.Function1(func(e sql.Expression) sql.Expression {
		return NewCount(e)
	}),
	"min": sql.Function1(func(e sql.Expression) sql.Expression {
		return NewMin(e)
	}),
	"max": sql.Function1(func(e sql.Expression) sql.Expression {
		return NewMax(e)
	}),
	"avg": sql.Function1(func(e sql.Expression) sql.Expression {
		return NewAvg(e)
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
