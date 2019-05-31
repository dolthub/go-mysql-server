package function

import (
	"fmt"
	"time"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

// DateAdd adds an interval to a date.
type DateAdd struct {
	Date     sql.Expression
	Interval *expression.Interval
}

// NewDateAdd creates a new date add function.
func NewDateAdd(args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("DATE_ADD", 2, len(args))
	}

	i, ok := args[1].(*expression.Interval)
	if !ok {
		return nil, fmt.Errorf("DATE_ADD expects an interval as second parameter")
	}

	return &DateAdd{args[0], i}, nil
}

// Children implements the sql.Expression interface.
func (d *DateAdd) Children() []sql.Expression {
	return []sql.Expression{d.Date, d.Interval}
}

// Resolved implements the sql.Expression interface.
func (d *DateAdd) Resolved() bool {
	return d.Date.Resolved() && d.Interval.Resolved()
}

// IsNullable implements the sql.Expression interface.
func (d *DateAdd) IsNullable() bool {
	return true
}

// Type implements the sql.Expression interface.
func (d *DateAdd) Type() sql.Type { return sql.Date }

// TransformUp implements the sql.Expression interface.
func (d *DateAdd) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	date, err := d.Date.TransformUp(f)
	if err != nil {
		return nil, err
	}
	interval, err := d.Interval.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return &DateAdd{date, interval.(*expression.Interval)}, nil
}

// Eval implements the sql.Expression interface.
func (d *DateAdd) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	date, err := d.Date.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if date == nil {
		return nil, nil
	}

	date, err = sql.Timestamp.Convert(date)
	if err != nil {
		return nil, err
	}

	delta, err := d.Interval.EvalDelta(ctx, row)
	if err != nil {
		return nil, err
	}

	if delta == nil {
		return nil, nil
	}

	return sql.ValidateTime(delta.Add(date.(time.Time))), nil
}

func (d *DateAdd) String() string {
	return fmt.Sprintf("DATE_ADD(%s, %s)", d.Date, d.Interval)
}

// DateSub subtracts an interval from a date.
type DateSub struct {
	Date     sql.Expression
	Interval *expression.Interval
}

// NewDateSub creates a new date add function.
func NewDateSub(args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("DATE_SUB", 2, len(args))
	}

	i, ok := args[1].(*expression.Interval)
	if !ok {
		return nil, fmt.Errorf("DATE_SUB expects an interval as second parameter")
	}

	return &DateSub{args[0], i}, nil
}

// Children implements the sql.Expression interface.
func (d *DateSub) Children() []sql.Expression {
	return []sql.Expression{d.Date, d.Interval}
}

// Resolved implements the sql.Expression interface.
func (d *DateSub) Resolved() bool {
	return d.Date.Resolved() && d.Interval.Resolved()
}

// IsNullable implements the sql.Expression interface.
func (d *DateSub) IsNullable() bool {
	return true
}

// Type implements the sql.Expression interface.
func (d *DateSub) Type() sql.Type { return sql.Date }

// TransformUp implements the sql.Expression interface.
func (d *DateSub) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	date, err := d.Date.TransformUp(f)
	if err != nil {
		return nil, err
	}
	interval, err := d.Interval.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return &DateSub{date, interval.(*expression.Interval)}, nil
}

// Eval implements the sql.Expression interface.
func (d *DateSub) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	date, err := d.Date.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if date == nil {
		return nil, nil
	}

	date, err = sql.Timestamp.Convert(date)
	if err != nil {
		return nil, err
	}

	delta, err := d.Interval.EvalDelta(ctx, row)
	if err != nil {
		return nil, err
	}

	if delta == nil {
		return nil, nil
	}

	return sql.ValidateTime(delta.Sub(date.(time.Time))), nil
}

func (d *DateSub) String() string {
	return fmt.Sprintf("DATE_SUB(%s, %s)", d.Date, d.Interval)
}
