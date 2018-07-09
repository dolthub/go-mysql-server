package aggregation // import "gopkg.in/src-d/go-mysql-server.v0/sql/expression/function/aggregation"

import (
	"fmt"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// Avg node to calculate the average from numeric column
type Avg struct {
	expression.UnaryExpression
}

// NewAvg creates a new Avg node.
func NewAvg(e sql.Expression) *Avg {
	return &Avg{expression.UnaryExpression{Child: e}}
}

func (a *Avg) String() string {
	return fmt.Sprintf("AVG(%s)", a.Child)
}

// Resolved implements AggregationExpression interface. (AggregationExpression[Expression[Resolvable]]])
func (a *Avg) Resolved() bool {
	return true
}

// Type implements AggregationExpression interface. (AggregationExpression[Expression]])
func (a *Avg) Type() sql.Type {
	return sql.Float64
}

// IsNullable implements AggregationExpression interface. (AggregationExpression[Expression]])
func (a *Avg) IsNullable() bool {
	return true
}

// Eval implements AggregationExpression interface. (AggregationExpression[Expression]])
func (a *Avg) Eval(ctx *sql.Context, buffer sql.Row) (interface{}, error) {
	nulls := buffer[2].(bool)
	if nulls {
		return nil, nil
	}

	sum := buffer[0].(float64)
	rows := buffer[1].(int64)

	if rows == 0 {
		return float64(0), nil
	}

	return sum / float64(rows), nil
}

// TransformUp implements AggregationExpression interface.
func (a *Avg) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := a.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewAvg(child))
}

// NewBuffer implements AggregationExpression interface. (AggregationExpression)
func (a *Avg) NewBuffer() sql.Row {
	const (
		sum   = float64(0)
		rows  = int64(0)
		nulls = false
	)

	return sql.NewRow(sum, rows, nulls)
}

// Update implements AggregationExpression interface. (AggregationExpression)
func (a *Avg) Update(ctx *sql.Context, buffer, row sql.Row) error {
	// if there are nulls already skip all the remainiing rows
	if buffer[2].(bool) {
		return nil
	}

	v, err := a.Child.Eval(ctx, row)
	if err != nil {
		return err
	}

	if v == nil {
		buffer[2] = true
		return nil
	}

	v, err = sql.Float64.Convert(v)
	if err != nil {
		v = float64(0)
	}

	buffer[0] = buffer[0].(float64) + v.(float64)
	buffer[1] = buffer[1].(int64) + 1

	return nil
}

// Merge implements AggregationExpression interface. (AggregationExpression)
func (a *Avg) Merge(ctx *sql.Context, buffer, partial sql.Row) error {
	bsum := buffer[0].(float64)
	brows := buffer[1].(int64)
	bnulls := buffer[2].(bool)

	psum := partial[0].(float64)
	prows := partial[1].(int64)
	pnulls := buffer[2].(bool)

	buffer[0] = bsum + psum
	buffer[1] = brows + prows
	buffer[2] = bnulls || pnulls

	return nil
}
