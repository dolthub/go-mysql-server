package expression

import (
	"time"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func getDatePart(u UnaryExpression, row sql.Row, f func(time.Time) int) (interface{}, error) {
	val, err := u.Child.Eval(row)
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
			return nil, err
		}
	}

	return int32(f(date.(time.Time))), nil
}

// Year is a function that returns the year of a date.
type Year struct {
	UnaryExpression
}

// NewYear creates a new Year UDF.
func NewYear(date sql.Expression) sql.Expression {
	return &Year{UnaryExpression{Child: date}}
}

// Name implements the Expression interface.
func (y *Year) Name() string { return "year" }

// Type implements the Expression interface.
func (y *Year) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (y *Year) Eval(row sql.Row) (interface{}, error) {
	return getDatePart(y.UnaryExpression, row, (time.Time).Year)
}

// TransformUp implements the Expression interface.
func (y *Year) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(NewYear(y.Child.TransformUp(f)))
}

// Month is a function that returns the month of a date.
type Month struct {
	UnaryExpression
}

// NewMonth creates a new Month UDF.
func NewMonth(date sql.Expression) sql.Expression {
	return &Month{UnaryExpression{Child: date}}
}

// Name implements the Expression interface.
func (m *Month) Name() string { return "month" }

// Type implements the Expression interface.
func (m *Month) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (m *Month) Eval(row sql.Row) (interface{}, error) {
	monthFunc := func(t time.Time) int {
		return int(t.Month())
	}

	return getDatePart(m.UnaryExpression, row, monthFunc)
}

// TransformUp implements the Expression interface.
func (m *Month) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(NewMonth(m.Child.TransformUp(f)))
}

// Day is a function that returns the day of a date.
type Day struct {
	UnaryExpression
}

// NewDay creates a new Day UDF.
func NewDay(date sql.Expression) sql.Expression {
	return &Day{UnaryExpression{Child: date}}
}

// Name implements the Expression interface.
func (d *Day) Name() string { return "day" }

// Type implements the Expression interface.
func (d *Day) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (d *Day) Eval(row sql.Row) (interface{}, error) {
	return getDatePart(d.UnaryExpression, row, (time.Time).Day)
}

// TransformUp implements the Expression interface.
func (d *Day) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(NewDay(d.Child.TransformUp(f)))
}

// Hour is a function that returns the hour of a date.
type Hour struct {
	UnaryExpression
}

// NewHour creates a new Hour UDF.
func NewHour(date sql.Expression) sql.Expression {
	return &Hour{UnaryExpression{Child: date}}
}

// Name implements the Expression interface.
func (h *Hour) Name() string { return "hour" }

// Type implements the Expression interface.
func (h *Hour) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (h *Hour) Eval(row sql.Row) (interface{}, error) {
	return getDatePart(h.UnaryExpression, row, (time.Time).Hour)
}

// TransformUp implements the Expression interface.
func (h *Hour) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(NewHour(h.Child.TransformUp(f)))
}

// Minute is a function that returns the minute of a date.
type Minute struct {
	UnaryExpression
}

// NewMinute creates a new Minute UDF.
func NewMinute(date sql.Expression) sql.Expression {
	return &Minute{UnaryExpression{Child: date}}
}

// Name implements the Expression interface.
func (m *Minute) Name() string { return "minute" }

// Type implements the Expression interface.
func (m *Minute) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (m *Minute) Eval(row sql.Row) (interface{}, error) {
	return getDatePart(m.UnaryExpression, row, (time.Time).Minute)
}

// TransformUp implements the Expression interface.
func (m *Minute) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(NewMinute(m.Child.TransformUp(f)))
}

// Second is a function that returns the second of a date.
type Second struct {
	UnaryExpression
}

// NewSecond creates a new Second UDF.
func NewSecond(date sql.Expression) sql.Expression {
	return &Second{UnaryExpression{Child: date}}
}

// Name implements the Expression interface.
func (s *Second) Name() string { return "second" }

// Type implements the Expression interface.
func (s *Second) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (s *Second) Eval(row sql.Row) (interface{}, error) {
	return getDatePart(s.UnaryExpression, row, (time.Time).Second)
}

// TransformUp implements the Expression interface.
func (s *Second) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(NewSecond(s.Child.TransformUp(f)))
}

// DayOfYear is a function that returns the day of the year from a date.
type DayOfYear struct {
	UnaryExpression
}

// NewDayOfYear creates a new DayOfYear UDF.
func NewDayOfYear(date sql.Expression) sql.Expression {
	return &DayOfYear{UnaryExpression{Child: date}}
}

// Name implements the Expression interface.
func (d *DayOfYear) Name() string { return "dayofyear" }

// Type implements the Expression interface.
func (d *DayOfYear) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (d *DayOfYear) Eval(row sql.Row) (interface{}, error) {
	return getDatePart(d.UnaryExpression, row, (time.Time).YearDay)
}

// TransformUp implements the Expression interface.
func (d *DayOfYear) TransformUp(f func(sql.Expression) sql.Expression) sql.Expression {
	return f(NewDayOfYear(d.Child.TransformUp(f)))
}
