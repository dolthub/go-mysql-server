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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// DateAdd adds an interval to a date.
type DateAdd struct {
	Date     sql.Expression
	Interval *expression.Interval
}

var _ sql.FunctionExpression = (*DateAdd)(nil)

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

// FunctionName implements sql.FunctionExpression
func (d *DateAdd) FunctionName() string {
	return "date_add"
}

// Description implements sql.FunctionExpression
func (d *DateAdd) Description() string {
	return "adds the interval to the given date."
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
func (d *DateAdd) Type() sql.Type {
	sqlType := dateOffsetType(d.Date, d.Interval)
	return sqlType
}

// WithChildren implements the Expression interface.
func (d *DateAdd) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDateAdd(children...)
}

// Eval implements the sql.Expression interface.
func (d *DateAdd) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := d.Date.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	delta, err := d.Interval.EvalDelta(ctx, row)
	if err != nil {
		return nil, err
	}

	if delta == nil {
		return nil, nil
	}

	date, err := sql.Datetime.Convert(val)
	if err != nil {
		ctx.Warn(1292, err.Error())
		return nil, nil
	}

	// return appropriate type
	res := sql.ValidateTime(delta.Add(date.(time.Time)))
	resType := d.Type()
	if sql.IsText(resType) {
		return res, nil
	}
	return resType.Convert(res)
}

func (d *DateAdd) String() string {
	return fmt.Sprintf("%s(%s,%s)", d.FunctionName(), d.Date, d.Interval)
}

// DateSub subtracts an interval from a date.
type DateSub struct {
	Date     sql.Expression
	Interval *expression.Interval
}

var _ sql.FunctionExpression = (*DateSub)(nil)

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

// FunctionName implements sql.FunctionExpression
func (d *DateSub) FunctionName() string {
	return "date_sub"
}

// Description implements sql.FunctionExpression
func (d *DateSub) Description() string {
	return "subtracts the interval from the given date."
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
func (d *DateSub) Type() sql.Type {
	sqlType := dateOffsetType(d.Date, d.Interval)
	return sqlType
}

// WithChildren implements the Expression interface.
func (d *DateSub) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDateSub(children...)
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

	date, err = sql.Datetime.Convert(date)
	if err != nil {
		ctx.Warn(1292, err.Error())
		return nil, nil
	}

	delta, err := d.Interval.EvalDelta(ctx, row)
	if err != nil {
		return nil, err
	}

	if delta == nil {
		return nil, nil
	}

	// return appropriate type
	res := sql.ValidateTime(delta.Sub(date.(time.Time)))
	resType := d.Type()
	if sql.IsText(resType) {
		return res, nil
	}
	return resType.Convert(res)
}

func (d *DateSub) String() string {
	return fmt.Sprintf("%s(%s,%s)", d.FunctionName(), d.Date, d.Interval)
}

// TimestampConversion is a shorthand function for CONVERT(expr, TIMESTAMP)
type TimestampConversion struct {
	Date sql.Expression
}

var _ sql.FunctionExpression = (*TimestampConversion)(nil)

// FunctionName implements sql.FunctionExpression
func (t *TimestampConversion) FunctionName() string {
	return "timestamp"
}

// Description implements sql.FunctionExpression
func (t *TimestampConversion) Description() string {
	return "returns a timestamp value for the expression given (e.g. the string '2020-01-02')."
}

func (t *TimestampConversion) Resolved() bool {
	return t.Date == nil || t.Date.Resolved()
}

func (t *TimestampConversion) String() string {
	return fmt.Sprintf("%s(%s)", t.FunctionName(), t.Date)
}

func (t *TimestampConversion) Type() sql.Type {
	return sql.Timestamp
}

func (t *TimestampConversion) IsNullable() bool {
	return false
}

func (t *TimestampConversion) Eval(ctx *sql.Context, r sql.Row) (interface{}, error) {
	e, err := t.Date.Eval(ctx, r)
	if err != nil {
		return nil, err
	}
	return sql.Timestamp.Convert(e)
}

func (t *TimestampConversion) Children() []sql.Expression {
	if t.Date == nil {
		return nil
	}
	return []sql.Expression{t.Date}
}

func (t *TimestampConversion) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewTimestamp(children...)
}

func NewTimestamp(args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("TIMESTAMP", 1, len(args))
	}
	return &TimestampConversion{args[0]}, nil
}

// DatetimeConversion is a shorthand function for CONVERT(expr, DATETIME)
type DatetimeConversion struct {
	Date sql.Expression
}

var _ sql.FunctionExpression = (*DatetimeConversion)(nil)

// FunctionName implements sql.FunctionExpression
func (t *DatetimeConversion) FunctionName() string {
	return "datetime"
}

// Description implements sql.FunctionExpression
func (t *DatetimeConversion) Description() string {
	return "returns a DATETIME value for the expression given (e.g. the string '2020-01-02')."
}

func (t *DatetimeConversion) Resolved() bool {
	return t.Date == nil || t.Date.Resolved()
}

func (t *DatetimeConversion) String() string {
	return fmt.Sprintf("%s(%s)", t.FunctionName(), t.Date)
}

func (t *DatetimeConversion) Type() sql.Type {
	return sql.Datetime
}

func (t *DatetimeConversion) IsNullable() bool {
	return false
}

func (t *DatetimeConversion) Eval(ctx *sql.Context, r sql.Row) (interface{}, error) {
	e, err := t.Date.Eval(ctx, r)
	if err != nil {
		return nil, err
	}
	return sql.Datetime.Convert(e)
}

func (t *DatetimeConversion) Children() []sql.Expression {
	if t.Date == nil {
		return nil
	}
	return []sql.Expression{t.Date}
}

func (t *DatetimeConversion) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDatetime(children...)
}

// NewDatetime returns a DatetimeConversion instance to handle the sql function "datetime". This is
// not a standard mysql function, but provides a shorthand for datetime conversions.
func NewDatetime(args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("DATETIME", 1, len(args))
	}

	return &DatetimeConversion{args[0]}, nil
}

// UnixTimestamp converts the argument to the number of seconds since 1970-01-01 00:00:00 UTC.
// With no argument, returns number of seconds since unix epoch for the current time.
type UnixTimestamp struct {
	Date sql.Expression
}

var _ sql.FunctionExpression = (*UnixTimestamp)(nil)

func NewUnixTimestamp(args ...sql.Expression) (sql.Expression, error) {
	if len(args) > 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("UNIX_TIMESTAMP", 1, len(args))
	}
	if len(args) == 0 {
		return &UnixTimestamp{nil}, nil
	}
	return &UnixTimestamp{args[0]}, nil
}

// FunctionName implements sql.FunctionExpression
func (ut *UnixTimestamp) FunctionName() string {
	return "unix_timestamp"
}

// Description implements sql.FunctionExpression
func (ut *UnixTimestamp) Description() string {
	return "returns the datetime argument to the number of seconds since the Unix epoch. With no argument, returns the number of seconds since the Unix epoch for the current time."
}

func (ut *UnixTimestamp) Children() []sql.Expression {
	if ut.Date != nil {
		return []sql.Expression{ut.Date}
	}
	return nil
}

func (ut *UnixTimestamp) Resolved() bool {
	if ut.Date != nil {
		return ut.Date.Resolved()
	}
	return true
}

func (ut *UnixTimestamp) IsNullable() bool {
	return true
}

func (ut *UnixTimestamp) Type() sql.Type {
	return sql.Float64
}

func (ut *UnixTimestamp) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewUnixTimestamp(children...)
}

func (ut *UnixTimestamp) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if ut.Date == nil {
		return toUnixTimestamp(ctx.QueryTime())
	}

	date, err := ut.Date.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}

	date, err = sql.Datetime.Convert(date)
	if err != nil {
		// If we aren't able to convert the value to a date, return 0 and set
		// a warning to match MySQL's behavior
		ctx.Warn(1292, "Incorrect datetime value: %s", ut.Date.String())
		return 0, nil
	}

	return toUnixTimestamp(date.(time.Time))
}

func toUnixTimestamp(t time.Time) (interface{}, error) {
	return sql.Float64.Convert(float64(t.Unix()) + float64(t.Nanosecond())/float64(1000000000))
}

func (ut *UnixTimestamp) String() string {
	if ut.Date != nil {
		return fmt.Sprintf("%s(%s)", ut.FunctionName(), ut.Date)
	} else {
		return fmt.Sprintf("%s()", ut.FunctionName())
	}
}

// FromUnixtime converts the argument to a datetime.
type FromUnixtime struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*FromUnixtime)(nil)

func NewFromUnixtime(arg sql.Expression) sql.Expression {
	return &FromUnixtime{NewUnaryFunc(arg, "FROM_UNIXTIME", sql.Datetime)}
}

// Description implements sql.FunctionExpression
func (r *FromUnixtime) Description() string {
	return "formats Unix timestamp as a date."
}

func (r *FromUnixtime) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := r.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	n, err := sql.Int64.Convert(val)
	if err != nil {
		return nil, err
	}

	return time.Unix(n.(int64), 0), nil
}

func (r *FromUnixtime) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	return NewFromUnixtime(children[0]), nil
}

type CurrDate struct {
	NoArgFunc
}

func (c CurrDate) IsNonDeterministic() bool {
	return true
}

var _ sql.FunctionExpression = CurrDate{}

// Description implements sql.FunctionExpression
func (c CurrDate) Description() string {
	return "returns the current date."
}

func NewCurrDate() sql.Expression {
	return CurrDate{
		NoArgFunc: NoArgFunc{"curdate", sql.LongText},
	}
}

func NewCurrentDate() sql.Expression {
	return CurrDate{
		NoArgFunc: NoArgFunc{"current_date", sql.LongText},
	}
}

func currDateLogic(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	t := ctx.QueryTime()
	return fmt.Sprintf("%d-%02d-%02d", t.Year(), t.Month(), t.Day()), nil
}

// Eval implements sql.Expression
func (c CurrDate) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return currDateLogic(ctx, row)
}

// WithChildren implements sql.Expression
func (c CurrDate) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NoArgFuncWithChildren(c, children)
}

// Determines the return type of a DateAdd/DateSub expression
// Logic is based on https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-add
func dateOffsetType(input sql.Expression, interval *expression.Interval) sql.Type {
	if input == nil {
		return sql.Null
	}
	inputType := input.Type()

	// result is null if expression is null
	if inputType == sql.Null {
		return sql.Null
	}

	// set type flags
	isInputDate := inputType == sql.Date
	isInputTime := inputType == sql.Time
	isInputDatetime := inputType == sql.Datetime || inputType == sql.Timestamp

	// result is Datetime if expression is Datetime or Timestamp
	if isInputDatetime {
		return sql.Datetime
	}

	// determine what kind of interval we're dealing with
	isYmdInterval := strings.Contains(interval.Unit, "YEAR") ||
		strings.Contains(interval.Unit, "QUARTER") ||
		strings.Contains(interval.Unit, "MONTH") ||
		strings.Contains(interval.Unit, "WEEK") ||
		strings.Contains(interval.Unit, "DAY")

	isHmsInterval := strings.Contains(interval.Unit, "HOUR") ||
		strings.Contains(interval.Unit, "MINUTE") ||
		strings.Contains(interval.Unit, "SECOND")
	isMixedInterval := isYmdInterval && isHmsInterval

	// handle input of Date type
	if isInputDate {
		if isHmsInterval || isMixedInterval {
			// if interval contains time components, result is Datetime
			return sql.Datetime
		} else {
			// otherwise result is Date
			return sql.Date
		}
	}

	// handle input of Time type
	if isInputTime {
		if isYmdInterval || isMixedInterval {
			// if interval contains date components, result is Datetime
			return sql.Datetime
		} else {
			// otherwise result is Time
			return sql.Time
		}
	}

	// handle dynamic input type
	if sql.IsDeferredType(inputType) {
		if isYmdInterval && !isHmsInterval {
			// if interval contains only date components, result is Date
			return sql.Date
		} else {
			// otherwise result is Datetime
			return sql.Datetime
		}
	}

	// default type is VARCHAR
	return sql.Text
}
