package expression

import (
	"fmt"
	"reflect"

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

func (c Count) String() string {
	return fmt.Sprintf("COUNT(%s)", c.Child)
}

// TransformUp implements the Expression interface.
func (c *Count) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
	child, err := c.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewCount(child))
}

// Update implements the Aggregation interface.
func (c *Count) Update(session sql.Session, buffer, row sql.Row) error {
	var inc bool
	if _, ok := c.Child.(*Star); ok {
		inc = true
	} else {
		v, err := c.Child.Eval(session, row)
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
func (c *Count) Merge(session sql.Session, buffer, partial sql.Row) error {
	buffer[0] = buffer[0].(int32) + partial[0].(int32)
	return nil
}

// Eval implements the Aggregation interface.
func (c *Count) Eval(session sql.Session, buffer sql.Row) (interface{}, error) {
	return buffer[0], nil
}

// Min aggregation returns the smallest value of the selected column.
// It implements the Aggregation interface
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

func (m Min) String() string {
	return fmt.Sprintf("MIN(%s)", m.Child)
}

// IsNullable returns whether the return value can be null.
func (m *Min) IsNullable() bool {
	return true
}

// TransformUp implements the Transformable interface.
func (m *Min) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
	child, err := m.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewMin(child))
}

// NewBuffer creates a new buffer to compute the result.
func (m *Min) NewBuffer() sql.Row {
	return sql.NewRow(nil)
}

// Update implements the Aggregation interface.
func (m *Min) Update(session sql.Session, buffer, row sql.Row) error {
	v, err := m.Child.Eval(session, row)
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
func (m *Min) Merge(session sql.Session, buffer, partial sql.Row) error {
	return m.Update(session, buffer, partial)
}

// Eval implements the Aggregation interface
func (m *Min) Eval(session sql.Session, buffer sql.Row) (interface{}, error) {
	return buffer[0], nil
}

// Max agregation returns the greatest value of the selected column.
// It implements the Aggregation interface
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

func (m Max) String() string {
	return fmt.Sprintf("MAX(%s)", m.Child)
}

// IsNullable returns whether the return value can be null.
func (m *Max) IsNullable() bool {
	return false
}

// TransformUp implements the Transformable interface.
func (m *Max) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
	child, err := m.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewMax(child))
}

// NewBuffer creates a new buffer to compute the result.
func (m *Max) NewBuffer() sql.Row {
	return sql.NewRow(nil)
}

// Update implements the Aggregation interface.
func (m *Max) Update(session sql.Session, buffer, row sql.Row) error {
	v, err := m.Child.Eval(session, row)
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
func (m *Max) Merge(session sql.Session, buffer, partial sql.Row) error {
	return m.Update(session, buffer, partial)
}

// Eval implements the Aggregation interface.
func (m *Max) Eval(session sql.Session, buffer sql.Row) (interface{}, error) {
	return buffer[0], nil
}

// Avg node to calculate the average from numeric column
type Avg struct {
	UnaryExpression
}

// NewAvg creates a new Avg node.
func NewAvg(e sql.Expression) *Avg {
	return &Avg{UnaryExpression{e}}
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

// TransformUp implements AggregationExpression interface. (AggregationExpression[Expression]])
func (a *Avg) TransformUp(f func(sql.Expression) (sql.Expression, error)) (sql.Expression, error) {
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
