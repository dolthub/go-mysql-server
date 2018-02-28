package expression

import (
	"fmt"
	"reflect"

	errors "gopkg.in/src-d/go-errors.v0"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// Count node to count how many rows are in the result set.
type Count struct {
	UnaryExpression
}

// NewCount creates a new Count node.
func NewCount(e sql.Expression) *Count {
	return &Count{UnaryExpression{e}}
}

// NewBuffer creates a new buffer for the aggregation.
func (c *Count) NewBuffer() sql.Row {
	return sql.NewRow(int32(0))
}

// Type returns the type of the result.
func (c *Count) Type() sql.Type {
	return sql.Int32
}

// IsNullable returns whether the return value can be null.
func (c *Count) IsNullable() bool {
	return false
}

// Resolved implements the Expression interface.
func (c *Count) Resolved() bool {
	if _, ok := c.Child.(*Star); ok {
		return true
	}

	return c.Child.Resolved()
}

// Name returns the name of the node.
func (c *Count) Name() string {
	return fmt.Sprintf("count(%s)", c.Child.Name())
}

// TransformUp implements the Expression interface.
func (c *Count) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(NewCount(c.Child.TransformUp(f)))
}

// Update implements the Aggregation interface.
func (c *Count) Update(buffer, row sql.Row) error {
	var inc bool
	if _, ok := c.Child.(*Star); ok {
		inc = true
	} else {
		v, err := c.Child.Eval(row)
		if v != nil {
			inc = true
		}

		if err != nil {
			return err
		}
	}

	if inc {
		buffer[0] = buffer[0].(int32) + int32(1)
	}

	return nil
}

// Merge implements the Aggregation interface.
func (c *Count) Merge(buffer, partial sql.Row) error {
	buffer[0] = buffer[0].(int32) + partial[0].(int32)
	return nil
}

// Eval implements the Aggregation interface.
func (c *Count) Eval(buffer sql.Row) (interface{}, error) {
	return buffer[0], nil
}

// Min aggregation returns the smallest value of the selected column.
// It implements the AggregationExpression interface
type Min struct {
	UnaryExpression
}

// NewMin creates a new Min node.
func NewMin(e sql.Expression) *Min {
	return &Min{UnaryExpression{e}}
}

// Resolved implements the Resolvable interface.
func (m *Min) Resolved() bool {
	return m.Child.Resolved()
}

// Type returns the resultant type of the aggregation.
func (m *Min) Type() sql.Type {
	return m.Child.Type()
}

// Name returns the name of the node.
func (m *Min) Name() string {
	return fmt.Sprintf("min(%s)", m.Child.Name())
}

// IsNullable returns whether the return value can be null.
func (m *Min) IsNullable() bool {
	return true
}

// TransformUp implements the Transformable interface.
func (m *Min) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(NewMin(m.Child.TransformUp(f)))
}

// NewBuffer creates a new buffer to compute the result.
func (m *Min) NewBuffer() sql.Row {
	return sql.NewRow(nil)
}

// Update implements the Aggregation interface.
func (m *Min) Update(buffer, row sql.Row) error {
	v, err := m.Child.Eval(row)
	if err != nil {
		return err
	}

	if reflect.TypeOf(v) == nil {
		return nil
	}

	if buffer[0] == nil {
		buffer[0] = v
	}

	if m.Child.Type().Compare(v, buffer[0]) == -1 {
		buffer[0] = v
	}

	return nil
}

// Merge implements the Aggregation interface.
func (m *Min) Merge(buffer, partial sql.Row) error {
	return m.Update(buffer, partial)
}

// Eval implements the Aggregation interface
func (m *Min) Eval(buffer sql.Row) (interface{}, error) {
	return buffer[0], nil
}

// Max agregation returns the greatest value of the selected column.
// It implements the AggregationExpression interface
type Max struct {
	UnaryExpression
}

// NewMax returns a new Max node.
func NewMax(e sql.Expression) *Max {
	return &Max{UnaryExpression{e}}
}

// Resolved implements the Resolvable interface.
func (m *Max) Resolved() bool {
	return m.Child.Resolved()
}

// Type returns the resultant type of the aggregation.
func (m *Max) Type() sql.Type {
	return m.Child.Type()
}

// Name returns the name of the node.
func (m *Max) Name() string {
	return fmt.Sprintf("max(%s)", m.Child.Name())
}

// IsNullable returns whether the return value can be null.
func (m *Max) IsNullable() bool {
	return false
}

// TransformUp implements the Transformable interface.
func (m *Max) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(NewMax(m.Child.TransformUp(f)))
}

// NewBuffer creates a new buffer to compute the result.
func (m *Max) NewBuffer() sql.Row {
	return sql.NewRow(nil)
}

// Update implements the Aggregation interface.
func (m *Max) Update(buffer, row sql.Row) error {
	v, err := m.Child.Eval(row)
	if err != nil {
		return err
	}

	if reflect.TypeOf(v) == nil {
		return nil
	}

	if buffer[0] == nil {
		buffer[0] = v
	}

	if m.Child.Type().Compare(v, buffer[0]) == 1 {
		buffer[0] = v
	}

	return nil
}

// Merge implements the Aggregation interface.
func (m *Max) Merge(buffer, partial sql.Row) error {
	return m.Update(buffer, partial)
}

// Eval implements the Aggregation interface.
func (m *Max) Eval(buffer sql.Row) (interface{}, error) {
	return buffer[0], nil
}

// Avg node to calculate the average from numeric column
type Avg struct {
	UnaryExpression
}

var AverageNoNumericErr = errors.NewKind("avg aggregation can only be applied on numeric columns")

// NewAvg creates a new Avg node.
func NewAvg(e sql.Expression) *Avg {
	return &Avg{UnaryExpression{e}}
}

// Name implements Nameable interface.
func (a *Avg) Name() string {
	return fmt.Sprintf("avg(%s)", a.Child.Name())
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
func (a *Avg) Eval(buffer sql.Row) (interface{}, error) {
	if buffer[1].(float64) == 0 {
		return nil, nil
	}

	return buffer[0], nil
}

// TransformUp implements AggregationExpression interface. (AggregationExpression[Expression]])
func (a *Avg) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(NewAvg(a.Child.TransformUp(f)))
}

// NewBuffer implements AggregationExpression interface. (AggregationExpression)
func (a *Avg) NewBuffer() sql.Row {
	const (
		currentAvg = float64(0)
		rowsCount  = float64(0)
	)

	return sql.NewRow(currentAvg, rowsCount)
}

// Update implements AggregationExpression interface. (AggregationExpression)
func (a *Avg) Update(buffer, row sql.Row) error {
	v, err := a.Child.Eval(row)
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
		return AverageNoNumericErr.New()
	}

	prevAvg := buffer[0].(float64)
	numRows := buffer[1].(float64)
	nextAvg := (prevAvg*numRows + num) / (numRows + 1)
	buffer[0] = nextAvg
	buffer[1] = numRows + 1

	return nil

}

// Merge implements AggregationExpression interface. (AggregationExpression)
func (a *Avg) Merge(buffer, partial sql.Row) error {
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
