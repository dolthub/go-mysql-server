// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

var ErrInvalidArgument = errors.NewKind("invalid argument to function %s. %s.")

// ErrInvalidArgumentType is thrown when a function receives invalid argument types
var ErrInvalidArgumentType = errors.NewKind("function '%s' received invalid argument types")

// ErrTimeUnexpectedlyNil is thrown when a function encounters and unexpectedly nil time
var ErrTimeUnexpectedlyNil = errors.NewKind("time in function '%s' unexpectedly nil")

// ErrUnknownType is thrown when a function encounters and unknown type
var ErrUnknownType = errors.NewKind("function '%s' encountered unknown type %T")

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

	date, err := sql.Datetime.ConvertWithoutRangeCheck(val)
	if err != nil {
		date = sql.Datetime.Zero().(time.Time)
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

var _ sql.FunctionExpression = (*Year)(nil)

// NewYear creates a new Year UDF.
func NewYear(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Year{expression.UnaryExpression{Child: date}}
}

// FunctionName implements sql.FunctionExpression
func (y *Year) FunctionName() string {
	return "year"
}

func (y *Year) String() string { return fmt.Sprintf("YEAR(%s)", y.Child) }

// Type implements the Expression interface.
func (y *Year) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (y *Year) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, y.UnaryExpression, row, year)
}

// WithChildren implements the Expression interface.
func (y *Year) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(y, len(children), 1)
	}
	return NewYear(ctx, children[0]), nil
}

// Month is a function that returns the month of a date.
type Month struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Month)(nil)

// NewMonth creates a new Month UDF.
func NewMonth(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Month{expression.UnaryExpression{Child: date}}
}

// FunctionName implements sql.FunctionExpression
func (m *Month) FunctionName() string {
	return "month"
}

func (m *Month) String() string { return fmt.Sprintf("MONTH(%s)", m.Child) }

// Type implements the Expression interface.
func (m *Month) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (m *Month) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, m.UnaryExpression, row, month)
}

// WithChildren implements the Expression interface.
func (m *Month) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewMonth(ctx, children[0]), nil
}

// Day is a function that returns the day of a date.
type Day struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Day)(nil)

// NewDay creates a new Day UDF.
func NewDay(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Day{expression.UnaryExpression{Child: date}}
}

// FunctionName implements sql.FunctionExpression
func (d *Day) FunctionName() string {
	return "day"
}

func (d *Day) String() string { return fmt.Sprintf("DAY(%s)", d.Child) }

// Type implements the Expression interface.
func (d *Day) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (d *Day) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, d.UnaryExpression, row, day)
}

// WithChildren implements the Expression interface.
func (d *Day) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewDay(ctx, children[0]), nil
}

// Weekday is a function that returns the weekday of a date where 0 = Monday,
// ..., 6 = Sunday.
type Weekday struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Weekday)(nil)

// NewWeekday creates a new Weekday UDF.
func NewWeekday(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Weekday{expression.UnaryExpression{Child: date}}
}

// FunctionName implements sql.FunctionExpression
func (d *Weekday) FunctionName() string {
	return "weekday"
}

func (d *Weekday) String() string { return fmt.Sprintf("WEEKDAY(%s)", d.Child) }

// Type implements the Expression interface.
func (d *Weekday) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (d *Weekday) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, d.UnaryExpression, row, weekday)
}

// WithChildren implements the Expression interface.
func (d *Weekday) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewWeekday(ctx, children[0]), nil
}

// Hour is a function that returns the hour of a date.
type Hour struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Hour)(nil)

// NewHour creates a new Hour UDF.
func NewHour(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Hour{expression.UnaryExpression{Child: date}}
}

// FunctionName implements sql.FunctionExpression
func (h *Hour) FunctionName() string {
	return "hour"
}

func (h *Hour) String() string { return fmt.Sprintf("HOUR(%s)", h.Child) }

// Type implements the Expression interface.
func (h *Hour) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (h *Hour) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, h.UnaryExpression, row, hour)
}

// WithChildren implements the Expression interface.
func (h *Hour) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(h, len(children), 1)
	}
	return NewHour(ctx, children[0]), nil
}

// Minute is a function that returns the minute of a date.
type Minute struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Minute)(nil)

// NewMinute creates a new Minute UDF.
func NewMinute(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Minute{expression.UnaryExpression{Child: date}}
}

// FunctionName implements sql.FunctionExpression
func (m *Minute) FunctionName() string {
	return "minute"
}

func (m *Minute) String() string { return fmt.Sprintf("MINUTE(%d)", m.Child) }

// Type implements the Expression interface.
func (m *Minute) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (m *Minute) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, m.UnaryExpression, row, minute)
}

// WithChildren implements the Expression interface.
func (m *Minute) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewMinute(ctx, children[0]), nil
}

// Second is a function that returns the second of a date.
type Second struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Second)(nil)

// NewSecond creates a new Second UDF.
func NewSecond(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Second{expression.UnaryExpression{Child: date}}
}

// FunctionName implements sql.FunctionExpression
func (s *Second) FunctionName() string {
	return "second"
}

func (s *Second) String() string { return fmt.Sprintf("SECOND(%s)", s.Child) }

// Type implements the Expression interface.
func (s *Second) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (s *Second) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, s.UnaryExpression, row, second)
}

// WithChildren implements the Expression interface.
func (s *Second) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}
	return NewSecond(ctx, children[0]), nil
}

// DayOfWeek is a function that returns the day of the week from a date where
// 1 = Sunday, ..., 7 = Saturday.
type DayOfWeek struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*DayOfWeek)(nil)

// NewDayOfWeek creates a new DayOfWeek UDF.
func NewDayOfWeek(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &DayOfWeek{expression.UnaryExpression{Child: date}}
}

// FunctionName implements sql.FunctionExpression
func (d *DayOfWeek) FunctionName() string {
	return "dayofweek"
}

func (d *DayOfWeek) String() string { return fmt.Sprintf("DAYOFWEEK(%s)", d.Child) }

// Type implements the Expression interface.
func (d *DayOfWeek) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (d *DayOfWeek) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, d.UnaryExpression, row, dayOfWeek)
}

// WithChildren implements the Expression interface.
func (d *DayOfWeek) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewDayOfWeek(ctx, children[0]), nil
}

// DayOfYear is a function that returns the day of the year from a date.
type DayOfYear struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*DayOfYear)(nil)

// NewDayOfYear creates a new DayOfYear UDF.
func NewDayOfYear(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &DayOfYear{expression.UnaryExpression{Child: date}}
}

// FunctionName implements sql.FunctionExpression
func (d *DayOfYear) FunctionName() string {
	return "dayofyear"
}

func (d *DayOfYear) String() string { return fmt.Sprintf("DAYOFYEAR(%s)", d.Child) }

// Type implements the Expression interface.
func (d *DayOfYear) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (d *DayOfYear) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, d.UnaryExpression, row, dayOfYear)
}

// WithChildren implements the Expression interface.
func (d *DayOfYear) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewDayOfYear(ctx, children[0]), nil
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

var _ sql.FunctionExpression = (*YearWeek)(nil)

// NewYearWeek creates a new YearWeek UDF
func NewYearWeek(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
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

// FunctionName implements sql.FunctionExpression
func (d *YearWeek) FunctionName() string {
	return "yearweek"
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
		return nil, ErrInvalidArgument.New("YEARWEEK", "invalid year")
	}
	mm, ok := month(date).(int32)
	if !ok {
		return nil, ErrInvalidArgument.New("YEARWEEK", "invalid month")
	}
	dd, ok := day(date).(int32)
	if !ok {
		return nil, ErrInvalidArgument.New("YEARWEEK", "invalid day")
	}

	mode := int64(0)
	val, err := d.mode.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if val != nil {
		if i64, err := sql.Int64.Convert(val); err == nil {
			if mode, ok = i64.(int64); ok {
				mode %= 8 // mode in [0, 7]
			}
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

// WithChildren implements the Expression interface.
func (*YearWeek) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewYearWeek(ctx, children...)
}

// Week is a function that returns year and week for a date.
// The year in the result may be different from the year in the date argument for the first and the last week of the year.
// Details: https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_yearweek
type Week struct {
	date sql.Expression
	mode sql.Expression
}

var _ sql.FunctionExpression = (*Week)(nil)

// NewWeek creates a new Week UDF
func NewWeek(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("YEARWEEK", "1 or more", 0)
	}

	w := &Week{date: args[0]}
	if len(args) > 1 && args[1].Resolved() && sql.IsInteger(args[1].Type()) {
		w.mode = args[1]
	} else {
		w.mode = expression.NewLiteral(0, sql.Int64)
	}

	return w, nil
}

// FunctionName implements sql.FunctionExpression
func (d *Week) FunctionName() string {
	return "week"
}

func (d *Week) String() string { return fmt.Sprintf("WEEK(%s, %d)", d.date, d.mode) }

// Type implements the Expression interface.
func (d *Week) Type() sql.Type { return sql.Int32 }

// Eval implements the Expression interface.
func (d *Week) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	date, err := getDate(ctx, expression.UnaryExpression{Child: d.date}, row)
	if err != nil {
		return nil, err
	}

	yyyy, ok := year(date).(int32)
	if !ok {
		return nil, ErrInvalidArgument.New("WEEK", "invalid year")
	}
	mm, ok := month(date).(int32)
	if !ok {
		return nil, ErrInvalidArgument.New("WEEK", "invalid month")
	}
	dd, ok := day(date).(int32)
	if !ok {
		return nil, ErrInvalidArgument.New("WEEK", "invalid day")
	}

	mode := int64(0)
	val, err := d.mode.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if val != nil {
		if i64, err := sql.Int64.Convert(val); err == nil {
			if mode, ok = i64.(int64); ok {
				mode %= 8 // mode in [0, 7]
			}
		}
	}

	yearForWeek, week := calcWeek(yyyy, mm, dd, weekMode(mode)|weekBehaviourYear)

	if yearForWeek < yyyy {
		week = 0
	} else if yearForWeek > yyyy {
		week = 53
	}

	return week, nil
}

// Resolved implements the Expression interface.
func (d *Week) Resolved() bool {
	return d.date.Resolved() && d.mode.Resolved()
}

// Children implements the Expression interface.
func (d *Week) Children() []sql.Expression { return []sql.Expression{d.date, d.mode} }

// IsNullable implements the Expression interface.
func (d *Week) IsNullable() bool {
	return d.date.IsNullable()
}

// WithChildren implements the Expression interface.
func (*Week) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewWeek(ctx, children...)
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

// Now is a function that returns the current time.
type Now struct {
	precision *int
}

var _ sql.FunctionExpression = (*Now)(nil)

// NewNow returns a new Now node.
func NewNow(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	var precision *int
	if len(args) > 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("TIMESTAMP", 1, len(args))
	} else if len(args) == 1 {
		argType := args[0].Type().Promote()
		if argType != sql.Int64 && argType != sql.Uint64 {
			return nil, sql.ErrInvalidType.New(args[0].Type().String())
		}
		val, err := args[0].Eval(sql.NewEmptyContext(), nil)
		if err != nil {
			return nil, err
		}
		precisionArg, err := sql.Int32.Convert(val)

		if err != nil {
			return nil, err
		}

		n := int(precisionArg.(int32))
		if n < 0 || n > 6 {
			return nil, sql.ErrOutOfRange.New("precision", "now")
		}
		precision = &n
	}

	return &Now{precision}, nil
}

func subSecondPrecision(t time.Time, precision int) string {
	if precision == 0 {
		return ""
	}

	s := fmt.Sprintf(".%09d", t.Nanosecond())
	return s[:precision+1]
}

func fractionOfSecString(t time.Time) string {
	s := fmt.Sprintf("%09d", t.Nanosecond())
	s = s[:6]

	for i := len(s) - 1; i >= 0; i-- {
		if s[i] != '0' {
			break
		}

		s = s[:i]
	}

	if len(s) == 0 {
		return ""
	}

	return "." + s
}

// FunctionName implements sql.FunctionExpression
func (n *Now) FunctionName() string {
	return "now"
}

// Type implements the sql.Expression interface.
func (n *Now) Type() sql.Type {
	return sql.Datetime
}

func (n *Now) String() string {
	if n.precision == nil {
		return "NOW()"
	}

	return fmt.Sprintf("NOW(%d)", *n.precision)
}

// IsNullable implements the sql.Expression interface.
func (n *Now) IsNullable() bool { return false }

// Resolved implements the sql.Expression interface.
func (n *Now) Resolved() bool { return true }

// Children implements the sql.Expression interface.
func (n *Now) Children() []sql.Expression { return nil }

// Eval implements the sql.Expression interface.
func (n *Now) Eval(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	t := ctx.QueryTime()
	// TODO: Now should return a string formatted depending on context.  This code handles string formatting
	// and should be enabled at the time we fix the return type
	/*s, err := formatDate("%Y-%m-%d %H:%i:%s", t)

	if err != nil {
		return nil, err
	}

	if n.precision != nil {
		s += subSecondPrecision(t, *n.precision)
	}*/

	return t, nil
}

// WithChildren implements the Expression interface.
func (n *Now) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewNow(ctx, children...)
}

// UTCTimestamp is a function that returns the current time.
type UTCTimestamp struct {
	precision *int
}

var _ sql.FunctionExpression = (*UTCTimestamp)(nil)

// NewUTCTimestamp returns a new UTCTimestamp node.
func NewUTCTimestamp(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	var precision *int
	if len(args) > 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("UTC_TIMESTAMP", 1, len(args))
	} else if len(args) == 1 {
		argType := args[0].Type().Promote()
		if argType != sql.Int64 && argType != sql.Uint64 {
			return nil, sql.ErrInvalidType.New(args[0].Type().String())
		}
		val, err := args[0].Eval(sql.NewEmptyContext(), nil)
		if err != nil {
			return nil, err
		}
		precisionArg, err := sql.Int32.Convert(val)

		if err != nil {
			return nil, err
		}

		n := int(precisionArg.(int32))
		if n < 0 || n > 6 {
			return nil, sql.ErrOutOfRange.New("precision", "utc_timestamp")
		}
		precision = &n
	}

	return &UTCTimestamp{precision}, nil
}

func (ut *UTCTimestamp) FunctionName() string {
	return "utc_timestamp"
}

// Type implements the sql.Expression interface.
func (ut *UTCTimestamp) Type() sql.Type {
	return sql.Datetime
}

func (ut *UTCTimestamp) String() string {
	if ut.precision == nil {
		return "UTC_TIMESTAMP()"
	}

	return fmt.Sprintf("UTC_TIMESTAMP(%d)", *ut.precision)
}

// IsNullable implements the sql.Expression interface.
func (ut *UTCTimestamp) IsNullable() bool { return false }

// Resolved implements the sql.Expression interface.
func (ut *UTCTimestamp) Resolved() bool { return true }

// Children implements the sql.Expression interface.
func (ut *UTCTimestamp) Children() []sql.Expression { return nil }

// Eval implements the sql.Expression interface.
func (ut *UTCTimestamp) Eval(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	t := ctx.QueryTime()
	// TODO: Now should return a string formatted depending on context.  This code handles string formatting
	return t.UTC(), nil
}

// WithChildren implements the Expression interface.
func (ut *UTCTimestamp) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewUTCTimestamp(ctx, children...)
}

// Date a function takes the DATE part out from a datetime expression.
type Date struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Date)(nil)

// NewDate returns a new Date node.
func NewDate(ctx *sql.Context, date sql.Expression) sql.Expression {
	return &Date{expression.UnaryExpression{Child: date}}
}

// FunctionName implements sql.FunctionExpression
func (d *Date) FunctionName() string {
	return "date"
}

func (d *Date) String() string { return fmt.Sprintf("DATE(%s)", d.Child) }

// Type implements the Expression interface.
func (d *Date) Type() sql.Type { return sql.LongText }

// Eval implements the Expression interface.
func (d *Date) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return getDatePart(ctx, d.UnaryExpression, row, func(v interface{}) interface{} {
		if v == nil {
			return nil
		}

		return v.(time.Time).Format("2006-01-02")
	})
}

// WithChildren implements the Expression interface.
func (d *Date) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewDate(ctx, children[0]), nil
}

// UnaryDatetimeFunc is a sql.Function which takes a single datetime argument
type UnaryDatetimeFunc struct {
	expression.UnaryExpression
	// Name is the name of the function
	Name string
	// SQLType is the return type of the function
	SQLType sql.Type
}

func NewUnaryDatetimeFunc(arg sql.Expression, name string, sqlType sql.Type) *UnaryDatetimeFunc {
	return &UnaryDatetimeFunc{expression.UnaryExpression{Child: arg}, name, sqlType}
}

// FunctionName implements sql.FunctionExpression
func (dtf *UnaryDatetimeFunc) FunctionName() string {
	return dtf.Name
}

func (dtf *UnaryDatetimeFunc) EvalChild(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := dtf.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	return sql.Datetime.Convert(val)
}

// String implements the fmt.Stringer interface.
func (dtf *UnaryDatetimeFunc) String() string {
	return fmt.Sprintf("%s(%s)", strings.ToUpper(dtf.Name), dtf.Child.String())
}

// Type implements the Expression interface.
func (dtf *UnaryDatetimeFunc) Type() sql.Type {
	return dtf.SQLType
}

// DayName implements the DAYNAME function
type DayName struct {
	*UnaryDatetimeFunc
}

var _ sql.FunctionExpression = (*DayName)(nil)

func NewDayName(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &DayName{NewUnaryDatetimeFunc(arg, "DAYNAME", sql.Text)}
}

func (d *DayName) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := d.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	t := val.(time.Time)
	return t.Weekday().String(), nil
}

func (d *DayName) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewDayName(ctx, children[0]), nil
}

// Microsecond implements the MICROSECOND function
type Microsecond struct {
	*UnaryDatetimeFunc
}

var _ sql.FunctionExpression = (*Microsecond)(nil)

func NewMicrosecond(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &Microsecond{NewUnaryDatetimeFunc(arg, "MICROSECOND", sql.Uint64)}
}

func (m *Microsecond) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := m.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	t := val.(time.Time)
	return uint64(t.Nanosecond()) / uint64(time.Microsecond), nil
}

func (m *Microsecond) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewMicrosecond(ctx, children[0]), nil
}

// MonthName implements the MONTHNAME function
type MonthName struct {
	*UnaryDatetimeFunc
}

var _ sql.FunctionExpression = (*MonthName)(nil)

func NewMonthName(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &MonthName{NewUnaryDatetimeFunc(arg, "MONTHNAME", sql.Text)}
}

func (d *MonthName) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := d.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	t := val.(time.Time)
	return t.Month().String(), nil
}

func (d *MonthName) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}
	return NewMonthName(ctx, children[0]), nil
}

// TimeToSec implements the time_to_sec function
type TimeToSec struct {
	*UnaryDatetimeFunc
}

var _ sql.FunctionExpression = (*TimeToSec)(nil)

func NewTimeToSec(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &TimeToSec{NewUnaryDatetimeFunc(arg, "TIME_TO_SEC", sql.Uint64)}
}

func (m *TimeToSec) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := m.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	t := val.(time.Time)
	return uint64(t.Hour()*3600 + t.Minute()*60 + t.Second()), nil
}

func (m *TimeToSec) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewTimeToSec(ctx, children[0]), nil
}

// WeekOfYear implements the weekofyear function
type WeekOfYear struct {
	*UnaryDatetimeFunc
}

var _ sql.FunctionExpression = (*WeekOfYear)(nil)

func NewWeekOfYear(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &WeekOfYear{NewUnaryDatetimeFunc(arg, "WEEKOFYEAR", sql.Uint64)}
}

func (m *WeekOfYear) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := m.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	t := val.(time.Time)
	_, wk := t.ISOWeek()
	return wk, nil
}

func (m *WeekOfYear) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 1)
	}
	return NewWeekOfYear(ctx, children[0]), nil
}

// TimeDiff subtracts the second argument from the first expressed as a time value.
type TimeDiff struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*TimeDiff)(nil)

// NewTimeDiff creates a new NewTimeDiff expression.
func NewTimeDiff(ctx *sql.Context, e1, e2 sql.Expression) sql.Expression {
	return &TimeDiff{
		expression.BinaryExpression{
			Left:  e1,
			Right: e2,
		},
	}
}

func (td *TimeDiff) FunctionName() string {
	return "timediff"
}

// Type implements the Expression interface.
func (td *TimeDiff) Type() sql.Type { return sql.Time }

// IsNullable implements the Expression interface.
func (td *TimeDiff) IsNullable() bool { return false }

func (td *TimeDiff) String() string {
	return fmt.Sprintf("TIMEDIFF(%s, %s)", td.Left, td.Right)
}

// WithChildren implements the Expression interface.
func (td *TimeDiff) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(td, len(children), 2)
	}
	return NewTimeDiff(ctx, children[0], children[1]), nil
}

// Eval implements the Expression interface.
func (td *TimeDiff) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	left, err := td.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	right, err := td.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil || right == nil {
		return nil, ErrTimeUnexpectedlyNil.New("TIMEDIFF")
	}

	if leftDatetimeInt, err := sql.Datetime.Convert(left); err == nil {
		rightDatetimeInt, err := sql.Datetime.Convert(right)
		if err != nil {
			return nil, err
		}
		leftDatetime := leftDatetimeInt.(time.Time)
		rightDatetime := rightDatetimeInt.(time.Time)
		if leftDatetime.Location() != rightDatetime.Location() {
			rightDatetime = rightDatetime.In(leftDatetime.Location())
		}
		return sql.Time.Convert(leftDatetime.Sub(rightDatetime))
	} else if leftTime, err := sql.Time.ConvertToTimeDuration(left); err == nil {
		rightTime, err := sql.Time.ConvertToTimeDuration(right)
		if err != nil {
			return nil, err
		}
		resTime := leftTime - rightTime
		return sql.Time.Convert(resTime)
	} else {
		return nil, ErrInvalidArgumentType.New("timediff")
	}
}

type CurrTime struct {
	NoArgFunc
}

var _ sql.FunctionExpression = CurrTime{}

func NewCurrTime(ctx *sql.Context) sql.Expression {
	return CurrTime{
		NoArgFunc: NoArgFunc{"curtime", sql.LongText},
	}
}

func NewCurrentTime(ctx *sql.Context) sql.Expression {
	return CurrTime{
		NoArgFunc: NoArgFunc{"current_time", sql.LongText},
	}
}

func currTimeLogic(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	t := ctx.QueryTime()
	return fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second()), nil
}

// Eval implements sql.Expression
func (c CurrTime) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return currTimeLogic(ctx, row)
}

// WithChildren implements sql.Expression
func (c CurrTime) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NoArgFuncWithChildren(ctx, c, children)
}

type CurrTimestamp struct {
	NoArgFunc
}

var _ sql.FunctionExpression = CurrTimestamp{}

func NewCurrTimestamp(ctx *sql.Context) sql.Expression {
	return CurrTimestamp{
		NoArgFunc: NoArgFunc{"current_timestamp", sql.Datetime},
	}
}

func currDatetimeLogic(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	return ctx.QueryTime(), nil
}

// Eval implements sql.Expression
func (c CurrTimestamp) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return currDatetimeLogic(ctx, row)
}

// WithChildren implements sql.Expression
func (c CurrTimestamp) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NoArgFuncWithChildren(ctx, c, children)
}
