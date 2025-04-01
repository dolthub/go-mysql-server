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

	"github.com/shopspring/decimal"

	gmstime "github.com/dolthub/go-mysql-server/internal/time"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// NewAddDate returns a new function expression, or an error if one couldn't be created. The ADDDATE
// function is a synonym for DATE_ADD, with the one exception that if the second argument is NOT an
// explicitly declared interval, then the value is used and the interval period is assumed to be DAY.
// In either case, this function will actually return a *DateAdd struct.
func NewAddDate(args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("ADDDATE", 2, len(args))
	}

	// If the interval is explicitly specified, then we simply pass it all to DateSub
	i, ok := args[1].(*expression.Interval)
	if ok {
		return &DateAdd{args[0], i}, nil
	}

	// Otherwise, the interval period is assumed to be DAY
	i = expression.NewInterval(args[1], "DAY")
	return &DateAdd{args[0], i}, nil
}

// DateAdd adds an interval to a date.
type DateAdd struct {
	Date     sql.Expression
	Interval *expression.Interval
}

var _ sql.FunctionExpression = (*DateAdd)(nil)
var _ sql.CollationCoercible = (*DateAdd)(nil)

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

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*DateAdd) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return ctx.GetCollation(), 4
}

// WithChildren implements the Expression interface.
func (d *DateAdd) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDateAdd(children...)
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

	delta, err := d.Interval.EvalDelta(ctx, row)
	if err != nil {
		return nil, err
	}

	if delta == nil {
		return nil, nil
	}

	var dateVal interface{}
	dateVal, _, err = types.DatetimeMaxPrecision.Convert(ctx, date)
	if err != nil {
		ctx.Warn(1292, err.Error())
		return nil, nil
	}

	// return appropriate type
	res := types.ValidateTime(delta.Add(dateVal.(time.Time)))
	if res == nil {
		return nil, nil
	}

	resType := d.Type()
	if types.IsText(resType) {
		// If the input is a properly formatted date/datetime string, the output should also be a string
		if dateStr, isStr := date.(string); isStr {
			if res.(time.Time).Nanosecond() > 0 {
				return res.(time.Time).Format(sql.DatetimeLayoutNoTrim), nil
			}
			if isHmsInterval(d.Interval) {
				return res.(time.Time).Format(sql.TimestampDatetimeLayout), nil
			}
			for _, layout := range types.DateOnlyLayouts {
				if _, pErr := time.Parse(layout, dateStr); pErr != nil {
					continue
				}
				return res.(time.Time).Format(sql.DateLayout), nil
			}
		}
	}

	ret, _, err := resType.Convert(ctx, res)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (d *DateAdd) String() string {
	return fmt.Sprintf("%s(%s,%s)", d.FunctionName(), d.Date, d.Interval)
}

// NewSubDate returns a new function expression, or an error if one couldn't be created. The SUBDATE
// function is a synonym for DATE_SUB, with the one exception that if the second argument is NOT an
// explicitly declared interval, then the value is used and the interval period is assumed to be DAY.
// In either case, this function will actually return a *DateSub struct.
func NewSubDate(args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("SUBDATE", 2, len(args))
	}

	// If the interval is explicitly specified, then we simply pass it all to DateSub
	i, ok := args[1].(*expression.Interval)
	if ok {
		return &DateSub{args[0], i}, nil
	}

	// Otherwise, the interval period is assumed to be DAY
	i = expression.NewInterval(args[1], "DAY")
	return &DateSub{args[0], i}, nil
}

// DateSub subtracts an interval from a date.
type DateSub struct {
	Date     sql.Expression
	Interval *expression.Interval
}

var _ sql.FunctionExpression = (*DateSub)(nil)
var _ sql.CollationCoercible = (*DateSub)(nil)

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

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*DateSub) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return ctx.GetCollation(), 4
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

	delta, err := d.Interval.EvalDelta(ctx, row)
	if err != nil {
		return nil, err
	}

	if delta == nil {
		return nil, nil
	}

	var dateVal interface{}
	dateVal, _, err = types.DatetimeMaxPrecision.Convert(ctx, date)
	if err != nil {
		ctx.Warn(1292, err.Error())
		return nil, nil
	}

	// return appropriate type
	res := types.ValidateTime(delta.Sub(dateVal.(time.Time)))
	if res == nil {
		return nil, nil
	}

	resType := d.Type()
	if types.IsText(resType) {
		// If the input is a properly formatted date/datetime string, the output should also be a string
		if dateStr, isStr := date.(string); isStr {
			if res.(time.Time).Nanosecond() > 0 {
				return res.(time.Time).Format(sql.DatetimeLayoutNoTrim), nil
			}
			if isHmsInterval(d.Interval) {
				return res.(time.Time).Format(sql.TimestampDatetimeLayout), nil
			}
			for _, layout := range types.DateOnlyLayouts {
				if _, pErr := time.Parse(layout, dateStr); pErr != nil {
					continue
				}
				return res.(time.Time).Format(sql.DateLayout), nil
			}
		}
	}

	ret, _, err := resType.Convert(ctx, res)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (d *DateSub) String() string {
	return fmt.Sprintf("%s(%s,%s)", d.FunctionName(), d.Date, d.Interval)
}

// DatetimeConversion is a shorthand function for CONVERT(expr, DATETIME)
type DatetimeConversion struct {
	Date sql.Expression
}

var _ sql.FunctionExpression = (*DatetimeConversion)(nil)
var _ sql.CollationCoercible = (*DatetimeConversion)(nil)

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
	return t.Date.Type()
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*DatetimeConversion) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (t *DatetimeConversion) IsNullable() bool {
	return false
}

func (t *DatetimeConversion) Eval(ctx *sql.Context, r sql.Row) (interface{}, error) {
	e, err := t.Date.Eval(ctx, r)
	if err != nil {
		return nil, err
	}
	ret, _, err := types.DatetimeMaxPrecision.Convert(ctx, e)
	return ret, err
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

// NewDatetime returns a DatetimeConversion instance to handle the sql function "datetime". The standard
// MySQL function associated with this function is "timestamp", which actually returns a datetime type
// instead of a timestamp type.
// https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_timestamp
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
	typ  sql.Type
}

var _ sql.FunctionExpression = (*UnixTimestamp)(nil)
var _ sql.CollationCoercible = (*UnixTimestamp)(nil)

const MaxUnixTimeMicroSecs = 32536771199999999

// canEval returns if the expression contains an expression that cannot be evaluated without sql.Context or sql.Row.
func canEval(expr sql.Expression) bool {
	evaluable := true
	transform.InspectExpr(expr, func(e sql.Expression) bool {
		switch e.(type) {
		case *expression.GetField, *ConvertTz:
			evaluable = false
			return true
		}
		return false
	})
	return evaluable
}

func getNowExpr(expr sql.Expression) *Now {
	var now *Now
	transform.InspectExpr(expr, func(e sql.Expression) bool {
		if n, ok := e.(*Now); ok {
			now = n
			return true
		}
		return false
	})
	return now
}

func evalNowType(now *Now) sql.Type {
	if now.prec == nil {
		return types.Int64
	}
	if !canEval(now.prec) {
		return types.MustCreateDecimalType(19, 6)
	}
	prec, pErr := now.prec.Eval(nil, nil)
	if pErr != nil {
		return nil
	}
	scale, ok := types.CoalesceInt(prec)
	if !ok {
		return nil
	}
	typ, tErr := types.CreateDecimalType(19, uint8(scale))
	if tErr != nil {
		return nil
	}
	return typ
}

func NewUnixTimestamp(args ...sql.Expression) (sql.Expression, error) {
	if len(args) > 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("UNIX_TIMESTAMP", 1, len(args))
	}
	if len(args) == 0 {
		return &UnixTimestamp{}, nil
	}

	arg := args[0]
	if dtType, isDtType := arg.Type().(sql.DatetimeType); isDtType {
		return &UnixTimestamp{Date: arg, typ: types.MustCreateDecimalType(19, uint8(dtType.Precision()))}, nil
	}
	if !canEval(arg) {
		return &UnixTimestamp{Date: arg, typ: types.MustCreateDecimalType(19, 6)}, nil
	}
	if now := getNowExpr(arg); now != nil {
		return &UnixTimestamp{Date: arg, typ: evalNowType(now)}, nil
	}

	// evaluate arg to determine return type
	// no need to consider timezone conversions, because they have no impact on precision
	date, err := arg.Eval(nil, nil)
	if err != nil || date == nil {
		return &UnixTimestamp{Date: arg}, nil
	}
	// special case: text types with fractional seconds preserve scale
	// e.g. '2000-01-02 12:34:56.000' -> scale 3
	if types.IsText(arg.Type()) {
		dateStr := date.(string)
		idx := strings.Index(dateStr, ".")
		if idx != -1 {
			dateStr = strings.TrimSpace(dateStr[idx:])
			scale := uint8(len(dateStr) - 1)
			if scale > 0 {
				if scale > 6 {
					scale = 6
				}
				typ, tErr := types.CreateDecimalType(19, scale)
				if tErr != nil {
					return nil, tErr
				}
				return &UnixTimestamp{Date: arg, typ: typ}, nil
			}
		}
	}
	date, _, err = types.DatetimeMaxPrecision.Convert(ctx, date)
	if err != nil {
		return &UnixTimestamp{Date: arg}, nil
	}
	unixMicro := date.(time.Time).UnixMicro()
	if unixMicro%1e6 > 0 {
		scale := uint8(6)
		for ; unixMicro%10 == 0; unixMicro /= 10 {
			scale--
		}
		typ, tErr := types.CreateDecimalType(19, scale)
		if tErr != nil {
			return nil, tErr
		}
		return &UnixTimestamp{Date: arg, typ: typ}, nil
	}

	return &UnixTimestamp{Date: arg, typ: types.Int64}, nil
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
	if ut.typ == nil {
		return types.Int64
	}
	return ut.typ
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*UnixTimestamp) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (ut *UnixTimestamp) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewUnixTimestamp(children...)
}

func (ut *UnixTimestamp) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if ut.Date == nil {
		return toUnixTimestamp(ctx.QueryTime(), ut.Type()), nil
	}

	date, err := ut.Date.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}

	date, _, err = types.DatetimeMaxPrecision.Convert(ctx, date)
	if err != nil {
		// If we aren't able to convert the value to a date, return 0 and set
		// a warning to match MySQL's behavior
		ctx.Warn(1292, "Incorrect datetime value: %s", ut.Date.String())
		return int64(0), nil
	}

	// https://dev.mysql.com/doc/refman/8.4/en/date-and-time-functions.html#function_unix-timestamp
	// When the date argument is a TIMESTAMP column,
	// UNIX_TIMESTAMP() returns the internal timestamp value directly,
	// with no implicit “string-to-Unix-timestamp” conversion.
	if ut.Date.Type().Equals(types.Timestamp) {
		return toUnixTimestamp(date.(time.Time), ut.Type()), nil
	}
	// The function above returns the time value in UTC time zone.
	// Instead, it should use the current session time zone.

	// For example, if the current session TZ is set to +07:00 and given value is '2023-09-25 07:02:57',
	// then the correct time value is '2023-09-25 07:02:57 +07:00'.
	// Currently, we get '2023-09-25 07:02:57 +00:00' from the above function.
	// ConvertTimeZone function is used to get the value in +07:00 TZ
	// It will return the correct value of '2023-09-25 00:02:57 +00:00',
	// which is equivalent of '2023-09-25 07:02:57 +07:00'.
	stz, err := SessionTimeZone(ctx)
	if err != nil {
		return nil, err
	}

	ctz, ok := gmstime.ConvertTimeZone(date.(time.Time), stz, "UTC")
	if ok {
		date = ctz
	}

	return toUnixTimestamp(date.(time.Time), ut.Type()), nil
}

func toUnixTimestamp(t time.Time, resType sql.Type) interface{} {
	unixMicro := t.UnixMicro()
	if unixMicro > MaxUnixTimeMicroSecs {
		return int64(0)
	}
	if unixMicro < 1e6 {
		return resType.Zero()
	}
	if types.IsDecimal(resType) {
		// scale decimal
		scale := int32(resType.(types.DecimalType_).Scale())
		for i := 6 - scale; i > 0; i-- {
			unixMicro /= 10
		}
		res := decimal.New(unixMicro, -scale)
		str := res.String()
		if str == "" {
		}
		return res
	}
	return unixMicro / 1e6
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
var _ sql.CollationCoercible = (*FromUnixtime)(nil)

func NewFromUnixtime(arg sql.Expression) sql.Expression {
	return &FromUnixtime{NewUnaryFunc(arg, "FROM_UNIXTIME", types.DatetimeMaxPrecision)}
}

// Description implements sql.FunctionExpression
func (r *FromUnixtime) Description() string {
	return "formats Unix timestamp as a date."
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*FromUnixtime) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (r *FromUnixtime) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := r.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	n, _, err := types.Int64.Convert(ctx, val)
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
var _ sql.CollationCoercible = CurrDate{}

// Description implements sql.FunctionExpression
func (c CurrDate) Description() string {
	return "returns the current date."
}

func NewCurrDate() sql.Expression {
	return CurrDate{
		NoArgFunc: NoArgFunc{"curdate", types.LongText},
	}
}

func NewCurrentDate() sql.Expression {
	return CurrDate{
		NoArgFunc: NoArgFunc{"current_date", types.LongText},
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

// CollationCoercibility implements the interface sql.CollationCoercible.
func (CurrDate) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// WithChildren implements sql.Expression
func (c CurrDate) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NoArgFuncWithChildren(c, children)
}

func isYmdInterval(interval *expression.Interval) bool {
	return strings.Contains(interval.Unit, "YEAR") ||
		strings.Contains(interval.Unit, "QUARTER") ||
		strings.Contains(interval.Unit, "MONTH") ||
		strings.Contains(interval.Unit, "WEEK") ||
		strings.Contains(interval.Unit, "DAY")
}

func isHmsInterval(interval *expression.Interval) bool {
	return strings.Contains(interval.Unit, "HOUR") ||
		strings.Contains(interval.Unit, "MINUTE") ||
		strings.Contains(interval.Unit, "SECOND")
}

// Determines the return type of a DateAdd/DateSub expression
// Logic is based on https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-add
func dateOffsetType(input sql.Expression, interval *expression.Interval) sql.Type {
	if input == nil {
		return types.Null
	}
	inputType := input.Type()

	// result is null if expression is null
	if inputType == types.Null {
		return types.Null
	}

	if types.IsDatetimeType(inputType) || types.IsTimestampType(inputType) {
		return types.DatetimeMaxPrecision
	}

	// set type flags
	isInputDate := inputType == types.Date
	isInputTime := inputType == types.Time

	// determine what kind of interval we're dealing with
	isYmd := isYmdInterval(interval)
	isHms := isHmsInterval(interval)
	isMixed := isYmd && isHms

	// handle input of Date type
	if isInputDate {
		if isHms || isMixed {
			// if interval contains time components, result is Datetime
			return types.DatetimeMaxPrecision
		} else {
			// otherwise result is Date
			return types.Date
		}
	}

	// handle input of Time type
	if isInputTime {
		if isYmd || isMixed {
			// if interval contains date components, result is Datetime
			return types.DatetimeMaxPrecision
		} else {
			// otherwise result is Time
			return types.Time
		}
	}

	// handle dynamic input type
	if types.IsDeferredType(inputType) {
		if isYmd && !isHms {
			// if interval contains only date components, result is Date
			return types.Date
		} else {
			// otherwise result is Datetime
			return types.DatetimeMaxPrecision
		}
	}

	// default type is VARCHAR
	return types.Text
}
