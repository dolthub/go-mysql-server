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
func NewDateAdd(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
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

// WithChildren implements the Expression interface.
func (d *DateAdd) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewDateAdd(ctx, children...)
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

	date, err = sql.Datetime.Convert(date)
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

var _ sql.FunctionExpression = (*DateSub)(nil)

// NewDateSub creates a new date add function.
func NewDateSub(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
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

// WithChildren implements the Expression interface.
func (d *DateSub) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewDateSub(ctx, children...)
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

// TimestampConversion is a shorthand function for CONVERT(expr, TIMESTAMP)
type TimestampConversion struct {
	Date sql.Expression
}

var _ sql.FunctionExpression = (*TimestampConversion)(nil)

// FunctionName implements sql.FunctionExpression
func (t *TimestampConversion) FunctionName() string {
	return "timestamp"
}

func (t *TimestampConversion) Resolved() bool {
	return t.Date == nil || t.Date.Resolved()
}

func (t *TimestampConversion) String() string {
	return fmt.Sprintf("TIMESTAMP(%s)", t.Date)
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

func (t *TimestampConversion) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewTimestamp(ctx, children...)
}

func NewTimestamp(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
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

func (t *DatetimeConversion) Resolved() bool {
	return t.Date == nil || t.Date.Resolved()
}

func (t *DatetimeConversion) String() string {
	return fmt.Sprintf("DATETIME(%s)", t.Date)
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

func (t *DatetimeConversion) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewDatetime(ctx, children...)
}

// NewDatetime returns a DatetimeConversion instance to handle the sql function "datetime". This is
// not a standard mysql function, but provides a shorthand for datetime conversions.
func NewDatetime(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
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

func NewUnixTimestamp(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
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

func (ut *UnixTimestamp) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewUnixTimestamp(ctx, children...)
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
		return nil, err
	}

	return toUnixTimestamp(date.(time.Time))
}

func toUnixTimestamp(t time.Time) (interface{}, error) {
	return sql.Float64.Convert(float64(t.Unix()) + float64(t.Nanosecond())/float64(1000000000))
}

func (ut *UnixTimestamp) String() string {
	if ut.Date != nil {
		return fmt.Sprintf("UNIX_TIMESTAMP(%s)", ut.Date)
	} else {
		return "UNIX_TIMESTAMP()"
	}
}

// FromUnixtime converts the argument to a datetime.
type FromUnixtime struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*FromUnixtime)(nil)

func NewFromUnixtime(ctx *sql.Context, arg sql.Expression) sql.Expression {
	return &FromUnixtime{NewUnaryFunc(arg, "FROM_UNIXTIME", sql.Datetime)}
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

func (r *FromUnixtime) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	return NewFromUnixtime(ctx, children[0]), nil
}

type CurrDate struct {
	NoArgFunc
}

var _ sql.FunctionExpression = CurrDate{}

func NewCurrDate(ctx *sql.Context) sql.Expression {
	return CurrDate{
		NoArgFunc: NoArgFunc{"curdate", sql.LongText},
	}
}

func NewCurrentDate(ctx *sql.Context) sql.Expression {
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
func (c CurrDate) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NoArgFuncWithChildren(ctx, c, children)
}
