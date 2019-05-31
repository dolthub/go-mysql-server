package function

import (
	"errors"
	"fmt"
	"time"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

func getDate(ctx *sql.Context,
	u expression.UnaryExpression,
	row sql.Row) (interface{}, error) {

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

	return date, nil
}

func getDatePart(ctx *sql.Context,
	u expression.UnaryExpression,
	row sql.Row,
	f func(interface{}) interface{}) (interface{}, error) {

	date, err := getDate(ctx, u, row)
	if err != nil {
		return nil, err
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

// Weekday is a function that returns the weekday of a date where 0 = Monday,
// ..., 6 = Sunday.
type Weekday struct {
	expression.UnaryExpression
}

// NewWeekday creates a new Weekday UDF.
func NewWeekday(date sql.Expression) sql.Expression {
	return &Weekday{expression.UnaryExpression{Child: date}}
}

func (d *Weekday) String() string { return fmt.Sprintf("WEEKDAY(%s)", d.Child) }

// Type implements the Expression interface.
func (d *Weekday) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (d *Weekday) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, d.UnaryExpression, row, weekday)
}

// TransformUp implements the Expression interface.
func (d *Weekday) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := d.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewWeekday(child))
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

// DayOfWeek is a function that returns the day of the week from a date where
// 1 = Sunday, ..., 7 = Saturday.
type DayOfWeek struct {
	expression.UnaryExpression
}

// NewDayOfWeek creates a new DayOfWeek UDF.
func NewDayOfWeek(date sql.Expression) sql.Expression {
	return &DayOfWeek{expression.UnaryExpression{Child: date}}
}

func (d *DayOfWeek) String() string { return fmt.Sprintf("DAYOFWEEK(%s)", d.Child) }

// Type implements the Expression interface.
func (d *DayOfWeek) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (d *DayOfWeek) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, d.UnaryExpression, row, dayOfWeek)
}

// TransformUp implements the Expression interface.
func (d *DayOfWeek) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := d.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewDayOfWeek(child))
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

// YearWeek is a function that returns year and week for a date.
// The year in the result may be different from the year in the date argument for the first and the last week of the year.
// Details: https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_yearweek
type YearWeek struct {
	date sql.Expression
	mode sql.Expression
}

// NewYearWeek creates a new YearWeek UDF
func NewYearWeek(args ...sql.Expression) (sql.Expression, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("YEARWEEK", "1 or more", 0)
	}

	yw := &YearWeek{date: args[0]}
	if len(args) > 1 && args[1].Resolved() && sql.IsInteger(args[1].Type()) {
		yw.mode = args[1]
	} else {
		yw.mode = expression.NewLiteral(0, sql.Int64)
	}
	return yw, nil
}

func (d *YearWeek) String() string { return fmt.Sprintf("YEARWEEK(%s, %d)", d.date, d.mode) }

// Type implements the Expression interface.
func (d *YearWeek) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (d *YearWeek) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	date, err := getDate(ctx, expression.UnaryExpression{Child: d.date}, row)
	if err != nil {
		return nil, err
	}
	yyyy, ok := year(date).(int32)
	if !ok {
		return nil, errors.New("YEARWEEK: invalid year")
	}
	mm, ok := month(date).(int32)
	if !ok {
		return nil, errors.New("YEARWEEK: invalid month")
	}
	dd, ok := day(date).(int32)
	if !ok {
		return nil, errors.New("YEARWEEK: invalid day")
	}

	mode := int64(0)
	val, err := d.mode.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if val != nil {
		if mode, ok = val.(int64); ok {
			mode %= 8 // mode in [0, 7]
		}
	}
	yyyy, week := calcWeek(yyyy, mm, dd, weekMode(mode)|weekBehaviourYear)

	return (yyyy * 100) + week, nil
}

// Resolved implements the Expression interface.
func (d *YearWeek) Resolved() bool {
	return d.date.Resolved() && d.mode.Resolved()
}

// Children implements the Expression interface.
func (d *YearWeek) Children() []sql.Expression { return []sql.Expression{d.date, d.mode} }

// IsNullable implements the Expression interface.
func (d *YearWeek) IsNullable() bool {
	return d.date.IsNullable()
}

// TransformUp implements the Expression interface.
func (d *YearWeek) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	date, err := d.date.TransformUp(f)
	if err != nil {
		return nil, err
	}

	mode, err := d.mode.TransformUp(f)
	if err != nil {
		return nil, err
	}

	yw, err := NewYearWeek(date, mode)
	if err != nil {
		return nil, err
	}
	return f(yw)
}

// Following solution of YearWeek was taken from tidb: https://github.com/pingcap/tidb/blob/master/types/mytime.go
type weekBehaviour int64

const (
	// weekBehaviourMondayFirst set Monday as first day of week; otherwise Sunday is first day of week
	weekBehaviourMondayFirst weekBehaviour = 1 << iota
	// If set, Week is in range 1-53, otherwise Week is in range 0-53.
	// Note that this flag is only relevant if WEEK_JANUARY is not set.
	weekBehaviourYear
	// If not set, Weeks are numbered according to ISO 8601:1988.
	// If set, the week that contains the first 'first-day-of-week' is week 1.
	weekBehaviourFirstWeekday
)

func (v weekBehaviour) test(flag weekBehaviour) bool {
	return (v & flag) != 0
}

func weekMode(mode int64) weekBehaviour {
	weekFormat := weekBehaviour(mode & 7)
	if (weekFormat & weekBehaviourMondayFirst) == 0 {
		weekFormat ^= weekBehaviourFirstWeekday
	}
	return weekFormat
}

// calcWeekday calculates weekday from daynr, returns 0 for Monday, 1 for Tuesday ...
func calcWeekday(daynr int32, sundayFirstDayOfWeek bool) int32 {
	daynr += 5
	if sundayFirstDayOfWeek {
		daynr++
	}
	return daynr % 7
}

// calcWeek calculates week and year for the time.
func calcWeek(yyyy, mm, dd int32, wb weekBehaviour) (int32, int32) {
	daynr := calcDaynr(yyyy, mm, dd)
	firstDaynr := calcDaynr(yyyy, 1, 1)
	mondayFirst := wb.test(weekBehaviourMondayFirst)
	weekYear := wb.test(weekBehaviourYear)
	firstWeekday := wb.test(weekBehaviourFirstWeekday)
	weekday := calcWeekday(firstDaynr, !mondayFirst)

	week, days := int32(0), int32(0)
	if mm == 1 && dd <= 7-weekday {
		if !weekYear &&
			((firstWeekday && weekday != 0) || (!firstWeekday && weekday >= 4)) {
			return yyyy, week
		}
		weekYear = true
		yyyy--
		days = calcDaysInYear(yyyy)
		firstDaynr -= days
		weekday = (weekday + 53*7 - days) % 7
	}

	if (firstWeekday && weekday != 0) ||
		(!firstWeekday && weekday >= 4) {
		days = daynr - (firstDaynr + 7 - weekday)
	} else {
		days = daynr - (firstDaynr - weekday)
	}

	if weekYear && days >= 52*7 {
		weekday = (weekday + calcDaysInYear(yyyy)) % 7
		if (!firstWeekday && weekday < 4) ||
			(firstWeekday && weekday == 0) {
			yyyy++
			week = 1
			return yyyy, week
		}
	}
	week = days/7 + 1
	return yyyy, week
}

// calcDaysInYear calculates days in one year, it works with 0 <= yyyy <= 99.
func calcDaysInYear(yyyy int32) int32 {
	if (yyyy&3) == 0 && (yyyy%100 != 0 || (yyyy%400 == 0 && (yyyy != 0))) {
		return 366
	}
	return 365
}

// calcDaynr calculates days since 0000-00-00.
func calcDaynr(yyyy, mm, dd int32) int32 {
	if yyyy == 0 && mm == 0 {
		return 0
	}

	delsum := 365*yyyy + 31*(mm-1) + dd
	if mm <= 2 {
		yyyy--
	} else {
		delsum -= (mm*4 + 23) / 10
	}
	return delsum + yyyy/4 - ((yyyy/100+1)*3)/4
}

var (
	year      = datePartFunc((time.Time).Year)
	month     = datePartFunc(func(t time.Time) int { return int(t.Month()) })
	day       = datePartFunc((time.Time).Day)
	weekday   = datePartFunc(func(t time.Time) int { return (int(t.Weekday()) + 6) % 7 })
	hour      = datePartFunc((time.Time).Hour)
	minute    = datePartFunc((time.Time).Minute)
	second    = datePartFunc((time.Time).Second)
	dayOfWeek = datePartFunc(func(t time.Time) int { return int(t.Weekday()) + 1 })
	dayOfYear = datePartFunc((time.Time).YearDay)
)

type clock func() time.Time

var defaultClock = time.Now

// Now is a function that returns the current time.
type Now struct {
	clock
}

// NewNow returns a new Now node.
func NewNow() sql.Expression {
	return &Now{defaultClock}
}

// Type implements the sql.Expression interface.
func (*Now) Type() sql.Type { return sql.Timestamp }

func (*Now) String() string { return "NOW()" }

// IsNullable implements the sql.Expression interface.
func (*Now) IsNullable() bool { return false }

// Resolved implements the sql.Expression interface.
func (*Now) Resolved() bool { return true }

// Children implements the sql.Expression interface.
func (*Now) Children() []sql.Expression { return nil }

// Eval implements the sql.Expression interface.
func (n *Now) Eval(*sql.Context, sql.Row) (interface{}, error) {
	return n.clock(), nil
}

// TransformUp implements the sql.Expression interface.
func (n *Now) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	return f(n)
}

// Date a function takes the DATE part out from a datetime expression.
type Date struct {
	expression.UnaryExpression
}

// NewDate returns a new Date node.
func NewDate(date sql.Expression) sql.Expression {
	return &Date{expression.UnaryExpression{Child: date}}
}

func (d *Date) String() string { return fmt.Sprintf("DATE(%s)", d.Child) }

// Type implements the Expression interface.
func (d *Date) Type() sql.Type { return sql.Text }

// Eval implements the Expression interface.
func (d *Date) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, d.UnaryExpression, row, func(v interface{}) interface{} {
		if v == nil {
			return nil
		}

		return v.(time.Time).Format("2006-01-02")
	})
}

// TransformUp implements the sql.Expression interface.
func (d *Date) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := d.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewDate(child))
}
