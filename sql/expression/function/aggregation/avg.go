package aggregation

import (
	"fmt"
	"reflect"

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

func (a Avg) String() string {
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
func (a *Avg) Eval(session sql.Session, buffer sql.Row) (interface{}, error) {
	isNoNum := buffer[2].(bool)
	if isNoNum {
		return float64(0), nil
	}

	noNullRows := buffer[1].(float64)
	if noNullRows == 0 {
		return nil, nil
	}

	avg := buffer[0]
	return avg, nil
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
		currentAvg = float64(0)
		rowsCount  = float64(0)
		noNum      = false
	)

	return sql.NewRow(currentAvg, rowsCount, noNum)
}

// Update implements AggregationExpression interface. (AggregationExpression)
func (a *Avg) Update(session sql.Session, buffer, row sql.Row) error {
	v, err := a.Child.Eval(session, row)
	if err != nil {
		return err
	}

	if reflect.TypeOf(v) == nil {
		return nil
	}

	var num float64
	switch n := row[0].(type) {
	case int, int16, int32, int64:
		num = float64(reflect.ValueOf(n).Int())
	case uint, uint8, uint16, uint32, uint64:
		num = float64(reflect.ValueOf(n).Uint())
	case float32, float64:
		num = float64(reflect.ValueOf(n).Float())
	default:
		buffer[2] = true
		return nil
	}

	prevAvg := buffer[0].(float64)
	numRows := buffer[1].(float64)
	nextAvg := (prevAvg*numRows + num) / (numRows + 1)
	buffer[0] = nextAvg
	buffer[1] = numRows + 1

	return nil

}

// Merge implements AggregationExpression interface. (AggregationExpression)
func (a *Avg) Merge(session sql.Session, buffer, partial sql.Row) error {
	bufferAvg := buffer[0].(float64)
	bufferRows := buffer[1].(float64)

	partialAvg := partial[0].(float64)
	partialRows := partial[1].(float64)

	totalRows := bufferRows + partialRows
	nextAvg := ((bufferAvg * bufferRows) + (partialAvg * partialRows)) / totalRows

	buffer[0] = nextAvg
	buffer[1] = totalRows

	return nil
}
