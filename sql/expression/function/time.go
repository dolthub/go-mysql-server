package function

import (
	"fmt"
	"time"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func getDatePart(
	ctx *sql.Context,
	u expression.UnaryExpression,
	row sql.Row,
	f func(interface{}) interface{},
) (interface{}, error) {
	val, err := u.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	date, err := sql.Timestamp.Convert(val)
	if err != nil {
		date, err = sql.Date.Convert(val)
		if err != nil {
			date = nil
		}
	}

	return f(date), nil
}

// Year is a function that returns the year of a date.
type Year struct {
	expression.UnaryExpression
}

// NewYear creates a new Year UDF.
func NewYear(date sql.Expression) sql.Expression {
	return &Year{expression.UnaryExpression{Child: date}}
}

func (y *Year) String() string { return fmt.Sprintf("YEAR(%s)", y.Child) }

// Type implements the Expression interface.
func (y *Year) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (y *Year) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, y.UnaryExpression, row, year)
}

// TransformUp implements the Expression interface.
func (y *Year) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := y.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewYear(child))
}

// Month is a function that returns the month of a date.
type Month struct {
	expression.UnaryExpression
}

// NewMonth creates a new Month UDF.
func NewMonth(date sql.Expression) sql.Expression {
	return &Month{expression.UnaryExpression{Child: date}}
}

func (m *Month) String() string { return fmt.Sprintf("MONTH(%s)", m.Child) }

// Type implements the Expression interface.
func (m *Month) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (m *Month) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, m.UnaryExpression, row, month)
}

// TransformUp implements the Expression interface.
func (m *Month) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := m.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewMonth(child))
}

// Day is a function that returns the day of a date.
type Day struct {
	expression.UnaryExpression
}

// NewDay creates a new Day UDF.
func NewDay(date sql.Expression) sql.Expression {
	return &Day{expression.UnaryExpression{Child: date}}
}

func (d *Day) String() string { return fmt.Sprintf("DAY(%s)", d.Child) }

// Type implements the Expression interface.
func (d *Day) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (d *Day) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, d.UnaryExpression, row, day)
}

// TransformUp implements the Expression interface.
func (d *Day) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := d.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewDay(child))
}

// Hour is a function that returns the hour of a date.
type Hour struct {
	expression.UnaryExpression
}

// NewHour creates a new Hour UDF.
func NewHour(date sql.Expression) sql.Expression {
	return &Hour{expression.UnaryExpression{Child: date}}
}

func (h *Hour) String() string { return fmt.Sprintf("HOUR(%s)", h.Child) }

// Type implements the Expression interface.
func (h *Hour) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (h *Hour) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, h.UnaryExpression, row, hour)
}

// TransformUp implements the Expression interface.
func (h *Hour) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := h.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewHour(child))
}

// Minute is a function that returns the minute of a date.
type Minute struct {
	expression.UnaryExpression
}

// NewMinute creates a new Minute UDF.
func NewMinute(date sql.Expression) sql.Expression {
	return &Minute{expression.UnaryExpression{Child: date}}
}

func (m *Minute) String() string { return fmt.Sprintf("MINUTE(%d)", m.Child) }

// Type implements the Expression interface.
func (m *Minute) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (m *Minute) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, m.UnaryExpression, row, minute)
}

// TransformUp implements the Expression interface.
func (m *Minute) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := m.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewMinute(child))
}

// Second is a function that returns the second of a date.
type Second struct {
	expression.UnaryExpression
}

// NewSecond creates a new Second UDF.
func NewSecond(date sql.Expression) sql.Expression {
	return &Second{expression.UnaryExpression{Child: date}}
}

func (s *Second) String() string { return fmt.Sprintf("SECOND(%s)", s.Child) }

// Type implements the Expression interface.
func (s *Second) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (s *Second) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, s.UnaryExpression, row, second)
}

// TransformUp implements the Expression interface.
func (s *Second) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := s.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewSecond(child))
}

// DayOfYear is a function that returns the day of the year from a date.
type DayOfYear struct {
	expression.UnaryExpression
}

// NewDayOfYear creates a new DayOfYear UDF.
func NewDayOfYear(date sql.Expression) sql.Expression {
	return &DayOfYear{expression.UnaryExpression{Child: date}}
}

func (d *DayOfYear) String() string { return fmt.Sprintf("DAYOFYEAR(%s)", d.Child) }

// Type implements the Expression interface.
func (d *DayOfYear) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (d *DayOfYear) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, d.UnaryExpression, row, dayOfYear)
}

// TransformUp implements the Expression interface.
func (d *DayOfYear) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := d.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewDayOfYear(child))
}

func datePartFunc(fn func(time.Time) int) func(interface{}) interface{} {
	return func(v interface{}) interface{} {
		if v == nil {
			return nil
		}

		return int32(fn(v.(time.Time)))
	}
}

var (
	year      = datePartFunc((time.Time).Year)
	month     = datePartFunc(func(t time.Time) int { return int(t.Month()) })
	day       = datePartFunc((time.Time).Day)
	hour      = datePartFunc((time.Time).Hour)
	minute    = datePartFunc((time.Time).Minute)
	second    = datePartFunc((time.Time).Second)
	dayOfYear = datePartFunc((time.Time).YearDay)
)
