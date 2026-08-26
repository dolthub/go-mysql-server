// Copyright 2022 Dolthub, Inc.
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

package types

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/values"
)

const ZeroDateStr = "0000-00-00"

var ZeroTimestampDatetimeStrs = [][]byte{
	[]byte("0000-00-00 00:00:00"),
	[]byte("0000-00-00 00:00:00.0"),
	[]byte("0000-00-00 00:00:00.00"),
	[]byte("0000-00-00 00:00:00.000"),
	[]byte("0000-00-00 00:00:00.0000"),
	[]byte("0000-00-00 00:00:00.00000"),
	[]byte("0000-00-00 00:00:00.000000"),
}

// A Zero timestamp or datetime begins with three delimited groups of zeros,
// and then optionally either a space or a dot, followed by a zero time.
var zeroTimestampRegex = regexp.MustCompile(`^0+-0+-0+(.*)$`)

// IsZeroTimestampStr checks if a string is a valid zero string for a datetime type.
func IsZeroTimestampStr(timestamp string) bool {
	match := zeroTimestampRegex.FindStringSubmatchIndex(timestamp)

	if match == nil {
		return false
	}
	remainder := timestamp[match[2]:]
	if len(remainder) == 0 {
		return true
	}
	if remainder[0] != '.' && remainder[0] != ' ' {
		return false
	}
	return IsZeroTimeStr(remainder[1:])
}

func IsZeroTimeStr(time string) bool {
	return strings.HasPrefix("00:00:00.000000", time)
}

const MinDatetimeStringLength = 8 // length of "2000-1-1"

const MaxDatetimePrecision = 6

var (
	// ErrConvertingToTime is thrown when a value cannot be converted to a Time
	ErrConvertingToTime = errors.NewKind("Incorrect datetime value: '%v'")

	ErrConvertingToTimeOutOfRange = errors.NewKind("value %q is outside of %v range")

	// datetimeTypeMaxDatetime is the maximum representable Datetime/Date value. MYSQL: 9999-12-31 23:59:59.499999 (microseconds)
	datetimeTypeMaxDatetime = time.Date(9999, 12, 31, 23, 59, 59, 499999000, time.UTC)

	// datetimeTypeMinDatetime is the minimum representable Datetime/Date value. MYSQL: 1000-01-01 00:00:00.000000 (microseconds)
	datetimeTypeMinDatetime = time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)

	// datetimeTypeMaxTimestamp is the maximum representable Timestamp value, MYSQL: 2038-01-19 03:14:07.999999 (microseconds)
	datetimeTypeMaxTimestamp = time.Unix(math.MaxInt32, 999999000).UTC()

	// datetimeTypeMinTimestamp is the minimum representable Timestamp value, MYSQL: 1970-01-01 00:00:01.000000 (microseconds)
	datetimeTypeMinTimestamp = time.Unix(1, 0).UTC()

	datetimeTypeMaxDate = time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)

	// datetimeTypeMinDate is the minimum representable Date value, MYSQL: 1000-01-01 00:00:00.000000 (microseconds)
	datetimeTypeMinDate = time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)

	// The MAX and MIN are extrapolated from commit ff05628a530 in the MySQL source code from my_time.cc
	// datetimeMaxTime is the maximum representable time value, MYSQL: 9999-12-31 23:59:59.999999 (microseconds)
	datetimeMaxTime = time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)

	// datetimeMinTime is the minimum representable time value, MYSQL: 0000-00-00 00:00:00.000000 (microseconds)
	datetimeMinTime = ZeroTime

	DateOnlyLayouts = []string{
		"2006-01-02",
		"2006/01/02",
		"20060102",
		"2006-1-2",
	}

	TimezoneTimestampDatetimeLayout = "2006-01-02 15:04:05.999999999 -0700 MST" // represents standard Time.time.UTC()

	// TimestampDatetimeLayouts hold extra timestamps allowed for parsing. It does
	// not have all the layouts supported by mysql. Missing are two digit year
	// versions of common cases and dates that use non common separators.
	//
	// https://github.com/MariaDB/server/blob/mysql-5.5.36/sql-common/my_time.c#L124
	TimestampDatetimeLayouts = append([]string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999",
		"2006-1-2 15:4:5.999999999",
		"2006-1-2:15:4:5.999999999",
		time.RFC3339,
		"2006-01-02 15:04:05.",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:.",
		"2006-01-02 15:04:",
		"2006-01-02 15:04",
		"2006-01-02 15:4",
		"20060102150405",
	}, DateOnlyLayouts...)

	// ZeroTime is -0001-11-30 00:00:00 UTC which is the closest Go can get to 0000-00-00 00:00:00 without conflicting
	// with a valid timestamp in MySQL
	ZeroTime = time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)

	// Date is a date with day, month and year.
	Date = MustCreateDatetimeType(sqltypes.Date, 0)
	// Datetime is a date and a time with default precision (no fractional seconds).
	Datetime = MustCreateDatetimeType(sqltypes.Datetime, 0)
	// Datetime3 is a date and time with a precision of 3 (fractional seconds to 3 decimal places)
	Datetime3 = MustCreateDatetimeType(sqltypes.Datetime, 3)
	// DatetimeMaxPrecision is a date and a time with maximum precision
	DatetimeMaxPrecision = MustCreateDatetimeType(sqltypes.Datetime, MaxDatetimePrecision)
	// Timestamp is a UNIX timestamp with default precision (no fractional seconds).
	Timestamp = MustCreateDatetimeType(sqltypes.Timestamp, 0)
	// TimestampMaxPrecision is a UNIX timestamp with maximum precision
	TimestampMaxPrecision = MustCreateDatetimeType(sqltypes.Timestamp, MaxDatetimePrecision)
	// DatetimeMaxRange is a date and a time with maximum precision and maximum range.
	DatetimeMaxRange = MustCreateDatetimeType(sqltypes.Datetime, MaxDatetimePrecision)

	datetimeValueType = reflect.TypeOf(time.Time{})
)

type datetimeType struct {
	baseType  query.Type
	precision int
}

var _ sql.DatetimeType = datetimeType{}
var _ sql.CollationCoercible = datetimeType{}

// CreateDatetimeType creates a Type dealing with all temporal types that are not TIME nor YEAR.
func CreateDatetimeType(baseType query.Type, precision int) (sql.DatetimeType, error) {
	switch baseType {
	case sqltypes.Date, sqltypes.Datetime, sqltypes.Timestamp:
		if precision < 0 || precision > MaxDatetimePrecision {
			return nil, fmt.Errorf("precision must be between 0 and 6, got %d", precision)
		}
		return datetimeType{
			baseType:  baseType,
			precision: precision,
		}, nil
	}
	return nil, sql.ErrInvalidBaseType.New(baseType.String(), "datetime")
}

// MustCreateDatetimeType is the same as CreateDatetimeType except it panics on errors.
func MustCreateDatetimeType(baseType query.Type, precision int) sql.DatetimeType {
	dt, err := CreateDatetimeType(baseType, precision)
	if err != nil {
		panic(err)
	}
	return dt
}

func (t datetimeType) Precision() int {
	return t.precision
}

// Compare implements Type interface.
func (t datetimeType) Compare(ctx context.Context, a interface{}, b interface{}) (int, error) {
	if hasNulls, res := CompareNulls(a, b); hasNulls {
		return res, nil
	}

	var at time.Time
	var bt time.Time
	var ok bool
	var err error
	if at, ok = a.(time.Time); !ok {
		at, err = ConvertToTime(ctx, a, t)
		if err != nil {
			return 0, err
		}
	} else if t.baseType == sqltypes.Date {
		at = at.Truncate(24 * time.Hour)
	}
	if bt, ok = b.(time.Time); !ok {
		bt, err = ConvertToTime(ctx, b, t)
		if err != nil {
			return 0, err
		}

	} else if t.baseType == sqltypes.Date {
		bt = bt.Truncate(24 * time.Hour)
	}

	if at.Before(bt) {
		return -1, nil
	} else if at.After(bt) {
		return 1, nil
	}
	return 0, nil
}

// CompareValue implements the ValueType interface
func (t datetimeType) CompareValue(ctx *sql.Context, a, b sql.Value) (int, error) {
	panic("TODO: implement CompareValue for DatetimeType")
}

// Convert implements Type interface.
func (t datetimeType) Convert(ctx context.Context, v interface{}) (interface{}, sql.ConvertInRange, error) {
	if v == nil {
		return nil, sql.InRange, nil
	}
	res, err := ConvertToTime(ctx, v, t)
	if err != nil && !sql.ErrTruncatedIncorrect.Is(err) {
		return nil, sql.InRange, err
	}
	return res, sql.InRange, err
}

// precisionConversion is a conversion ratio to divide time.Second by to truncate the appropriate amount for the
// precision of a type with time info
var precisionConversion = [7]int{
	1, 10, 100, 1_000, 10_000, 100_000, 1_000_000,
}

func ConvertToTime(ctx context.Context, v interface{}, t datetimeType) (time.Time, error) {
	if v == nil {
		return time.Time{}, nil
	}

	res, err := t.ConvertWithoutRangeCheck(ctx, v)
	if err != nil && !sql.ErrTruncatedIncorrect.Is(err) {
		return time.Time{}, err
	}
	// TODO: is this correct?
	if res == nil {
		return time.Time{}, nil
	}

	dt := res.(time.Time)
	if dt.Equal(ZeroTime) {
		return ZeroTime, nil
	}

	// Round the date to the precision of this type
	if t.precision < MaxDatetimePrecision {
		truncationDuration := time.Second / time.Duration(precisionConversion[t.precision])
		res = dt.Round(truncationDuration)
	} else {
		res = dt.Round(time.Microsecond)
	}

	if t == DatetimeMaxRange {
		validated := ValidateTime(dt)
		if validated == nil {
			return time.Time{}, ErrConvertingToTimeOutOfRange.New(v, t)
		}
		return validated.(time.Time), err
	}

	switch t.baseType {
	case sqltypes.Date:
		if dt.Year() < 0 || dt.Year() > 9999 {
			return time.Time{}, ErrConvertingToTimeOutOfRange.New(dt.Format(sql.DateLayout), t.String())
		}
	case sqltypes.Datetime:
		if dt.Year() < 0 || dt.Year() > 9999 {
			return time.Time{}, ErrConvertingToTimeOutOfRange.New(dt.Format(sql.TimestampDatetimeLayout), t.String())
		}
	case sqltypes.Timestamp:
		if ValidateTimestamp(dt) == nil {
			return time.Time{}, ErrConvertingToTimeOutOfRange.New(dt.Format(sql.TimestampDatetimeLayout), t.String())
		}
	}

	return dt, err
}

// ConvertWithoutRangeCheck converts the parameter to time.Time without checking the range.
func (t datetimeType) ConvertWithoutRangeCheck(ctx context.Context, v any) (any, error) {
	if v == nil {
		return nil, nil
	}

	var err error
	v, err = sql.UnwrapAny(ctx, v)
	if err != nil {
		return time.Time{}, err
	}

	var res time.Time
	switch value := v.(type) {
	case []byte:
		return t.ConvertWithoutRangeCheck(ctx, string(value))
	case string:
		if IsZeroTimestampStr(value) {
			return ZeroTime, nil
		}
		var val any
		var ok bool
		val, err = t.parseDatetime(value)
		res, ok = val.(time.Time)
		if !ok {
			return nil, err
		}
	case time.Time:
		res = value.UTC()
	// For most integer values, we just return an error (but MySQL is more lenient for some of these). A special case
	// is zero values, which are important when converting from postgres defaults.
	case int:
		if value == 0 {
			return ZeroTime, nil
		}
		return ZeroTime, ErrConvertingToTime.New(v)
	case int8:
		if value == 0 {
			return ZeroTime, nil
		}
		return ZeroTime, ErrConvertingToTime.New(v)
	case int16:
		if value == 0 {
			return ZeroTime, nil
		}
		return ZeroTime, ErrConvertingToTime.New(v)
	case int32:
		if value == 0 {
			return ZeroTime, nil
		}
		return ZeroTime, ErrConvertingToTime.New(v)
	case int64:
		if value == 0 {
			return ZeroTime, nil
		}
		return ZeroTime, ErrConvertingToTime.New(v)
	case uint:
		if value == 0 {
			return ZeroTime, nil
		}
		return ZeroTime, ErrConvertingToTime.New(v)
	case uint8:
		if value == 0 {
			return ZeroTime, nil
		}
		return ZeroTime, ErrConvertingToTime.New(v)
	case uint16:
		if value == 0 {
			return ZeroTime, nil
		}
		return ZeroTime, ErrConvertingToTime.New(v)
	case uint32:
		if value == 0 {
			return ZeroTime, nil
		}
		return ZeroTime, ErrConvertingToTime.New(v)
	case uint64:
		if value == 0 {
			return ZeroTime, nil
		}
		return ZeroTime, ErrConvertingToTime.New(v)
	case float32:
		if value == 0 {
			return ZeroTime, nil
		}
		return ZeroTime, ErrConvertingToTime.New(v)
	case float64:
		if value == 0 {
			return ZeroTime, nil
		}
		return ZeroTime, ErrConvertingToTime.New(v)
	case *apd.Decimal:
		if value.IsZero() {
			return ZeroTime, nil
		}
		return ZeroTime, ErrConvertingToTime.New(v)
	case Timespan:
		// when receiving TIME, MySQL fills in date with today
		nowTimeStr := sql.Now().Format("2006-01-02")
		nowTime, err := time.Parse("2006-01-02", nowTimeStr)
		if err != nil {
			return ZeroTime, ErrConvertingToTime.New(v)
		}
		return nowTime.Add(value.AsTimeDuration()), nil
	case bool:
		if !value {
			return ZeroTime, nil
		}
		return ZeroTime, ErrConvertingToTime.New(v)
	default:
		return ZeroTime, sql.ErrConvertToSQL.New(value, t)
	}

	switch t.baseType {
	case sqltypes.Date:
		res = res.Truncate(24 * time.Hour)
	default:
		roundDuration := time.Second / time.Duration(precisionConversion[t.precision])
		res = res.Round(roundDuration)
	}

	return res, err
}

// IsLeapYear returns if |year| is a leap year
func IsLeapYear(year int64) bool {
	return year != 0 && ((year%4 == 0 && year%100 != 0) || year%400 == 0)
}

var DaysPerMonth = [12]int64{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

// GetLastDay returns the last day of the month for the given year and month
func GetLastDay(year, month int) (res int, ok bool) {
	if month < 1 || month > 12 {
		return 31, false // defaults to 31 when the month is invalid
	}
	if month == 2 && IsLeapYear(int64(year)) {
		return 29, true
	}
	return int(DaysPerMonth[month-1]), true
}

// DateTimeRegex will match MySQL's DateTime format.
// The date portion (YYYY-MM-DD) is required all parts of the time portion (HH:MM:SS.MICROS) is optional.
// The standard datetime format is YYYY-MM-DD HH:MM:SS.MICROS, but MySQL supports a "relaxed" format where
// any punctuation (of various lengths) can be used between the date and time parts.
// Some exceptions:
//   - Whitespace characters are allowed in delimiter between Day and Hour
//   - The only valid delimiter between Seconds and Microseconds is a single decimal point (.)
//
// MySQL Reference: https://dev.mysql.com/doc/refman/8.4/en/datetime.html
//
//		Match 1: The entire datetime string
//		Group 1: Year
//		Group 2: Month
//		Group 3: Day
//		Group 4: Hour (optional)
//		Group 5: Minutes (optional)
//		Group 6: Seconds (optional)
//		Group 7: Microseconds (optional)
//	 Group 8: any trailing characters to be Truncated
var DateTimeRegex = regexp.MustCompile(`^(\d+)\p{P}+(\d+)\p{P}+(\d+)[\s\p{P}]*(\d*)?\p{P}*(\d*)?\p{P}*(\d*)?\p{P}*(\d*)?(.*)$`)

// parseDatetime parses a DateTime according to MySQL rules.
// TODO: return []error for accumulated warnings
func (t datetimeType) parseDatetime(value string) (any, error) {
	if t, err := time.Parse(TimezoneTimestampDatetimeLayout, value); err == nil {
		return t.UTC(), nil
	}

	// TODO: no delimiters

	value = strings.Trim(value, NumericCutSet) // TODO: leading and trailing whitespace(s) should throw warning
	matchIdxs := DateTimeRegex.FindStringSubmatchIndex(value)
	if len(matchIdxs) == 0 {
		return nil, sql.ErrTruncatedIncorrect.New(value)
	}

	// TODO: Handle delimiter warnings. These do not stop parsing unlike ErrTruncatedIncorrect warnings.

	// Date portion required for valid DateTime parsing, so no need to check for -1 indexes
	yearStr := value[matchIdxs[2]:matchIdxs[3]]
	monthStr := value[matchIdxs[4]:matchIdxs[5]]
	dayStr := value[matchIdxs[6]:matchIdxs[7]]

	// TODO: Invalid and Zero Dates are affected by sql_modes:
	// 	STRICT_TRANS_TABLES, NO_ZERO_DATE, and NO_ZERO_IN_DATE
	//  We currently only have STRICT_TRANS_TABLES enabled by default (which is a no-op alone), but it appears we can't
	//  properly support 0 Month and 0 Day.
	//  We should copy MySQL's default and have all these sql_modes enabled, and disallow disabling them (or at least
	//  throw a warning).
	//  MySQL References:
	//	 https://dev.mysql.com/doc/refman/9.7/en/sql-mode.html#sqlmode_no_zero_date
	//   https://dev.mysql.com/doc/refman/9.7/en/sql-mode.html#sqlmode_no_zero_in_date
	// TODO: make constants for MIN/MAX values
	// Negative numbers should be impossible, so we don't check for them
	year, err := strconv.Atoi(yearStr)
	if err != nil {
		return nil, sql.ErrTruncatedIncorrect.New(value)
	}
	if year > 9999 {
		return nil, sql.ErrTruncatedIncorrect.New(value)
	}
	// MySQL special case for abbreviated ('00) date formats
	if len(yearStr) == 2 {
		year += 2000
	}
	month, err := strconv.Atoi(monthStr)
	if err != nil {
		return nil, sql.ErrTruncatedIncorrect.New(value)
	}
	if month > 12 {
		return nil, sql.ErrTruncatedIncorrect.New(value)
	}
	day, err := strconv.Atoi(dayStr)
	if err != nil {
		return nil, sql.ErrTruncatedIncorrect.New(value)
	}
	// GetLastDay already handles invalid months
	if lastDay, _ := GetLastDay(year, month); day > lastDay {
		return nil, sql.ErrTruncatedIncorrect.New(value)
	}

	// The remaining match index pairs are optional
	// Case 1: matchIdx[i] = -1 and matchIdx[i+1] = -1 => empty string
	// Case 2: matchIdx[i] == matchIdx[i+1] => empty string
	// Case 3: matchIdx[i] = x and matchIdx[i+1] = y where y > x => [x:y]
	var hour, mins, sec, usec int
	if matchIdxs[8] != matchIdxs[9] {
		hourStr := value[matchIdxs[8]:matchIdxs[9]]
		hour, err = strconv.Atoi(hourStr)
		if err != nil {
			return nil, sql.ErrTruncatedIncorrect.New(value)
		}
		if hour > 23 {
			return nil, sql.ErrTruncatedIncorrect.New(value)
		}
	}
	if matchIdxs[10] != matchIdxs[11] {
		minStr := value[matchIdxs[10]:matchIdxs[11]]
		mins, err = strconv.Atoi(minStr)
		if err != nil {
			return nil, sql.ErrTruncatedIncorrect.New(value)
		}
		if mins > 59 {
			return nil, sql.ErrTruncatedIncorrect.New(value)
		}
	}
	if matchIdxs[12] != matchIdxs[13] {
		secStr := value[matchIdxs[12]:matchIdxs[13]]
		sec, err = strconv.Atoi(secStr)
		if err != nil {
			return nil, sql.ErrTruncatedIncorrect.New(value)
		}
		if sec > 59 {
			return nil, sql.ErrTruncatedIncorrect.New(value)
		}
	}
	if matchIdxs[14] != matchIdxs[15] {
		// microseconds can only be delimited by '.'; everything else causes this to be ignored
		// this check should be safe because of the outer if statement
		if matchIdxs[14]-matchIdxs[13] == 1 && value[matchIdxs[13]] == '.' {
			// Extract usec part
			// [0] = '.'
			// [1-6] = microseconds
			// [7] = additional digit for rounding
			usecStr := value[matchIdxs[13]:matchIdxs[15]]
			if len(usecStr) >= 8 {
				usecStr = usecStr[:8]
			}
			var usecf64 float64
			usecf64, err = strconv.ParseFloat(usecStr, 64)
			if err != nil {
				return nil, sql.ErrTruncatedIncorrect.New(value)
			}
			usec = int(math.Round(usecf64 * 1_000_000))
		}
	}
	// Trailing invalid characters
	if matchIdxs[17] != matchIdxs[16] {
		err = sql.ErrTruncatedIncorrect.New(value)
	}

	resTime := time.Date(year, time.Month(month), day, hour, mins, sec, usec*1000, time.UTC)
	resTime = resTime.Round(time.Microsecond)
	return resTime, err
}

// Equals implements the Type interface.
func (t datetimeType) Equals(otherType sql.Type) bool {
	if dtType, isDtType := otherType.(sql.DatetimeType); isDtType {
		return t.baseType == dtType.Type() && t.precision == dtType.Precision()
	}
	return false
}

// MaxTextResponseByteLength implements the Type interface
func (t datetimeType) MaxTextResponseByteLength(*sql.Context) uint32 {
	switch t.baseType {
	case sqltypes.Date:
		return uint32(len(sql.DateLayout))
	case sqltypes.Datetime, sqltypes.Timestamp:
		return uint32(len(sql.TimestampDatetimeLayout))
	default:
		panic(sql.ErrInvalidBaseType.New(t.baseType.String(), "datetime"))
	}
}

// Promote implements the Type interface.
func (t datetimeType) Promote() sql.Type {
	return DatetimeMaxPrecision
}

// SQL implements Type interface.
func (t datetimeType) SQL(ctx *sql.Context, dest []byte, v any) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	var err error
	dest, err = t.Serialize(ctx, dest, v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.MakeTrusted(t.baseType, dest), nil
}

func (t datetimeType) Serialize(ctx *sql.Context, dest []byte, v any) ([]byte, error) {
	vt, err := ConvertToTime(ctx, v, t)
	if err != nil {
		return dest, err
	}

	switch t.baseType {
	case sqltypes.Date:
		dest = appendDateFormat(dest, vt)
	case sqltypes.Datetime, sqltypes.Timestamp:
		dest = appendDatetimeFormat(dest, vt, t.precision)
	default:
		return dest, sql.ErrInvalidBaseType.New(t.baseType.String(), "datetime")
	}
	return dest, nil
}

// SQLValue implements the ValueType interface.
func (t datetimeType) SQLValue(ctx *sql.Context, v sql.Value, dest []byte) (sqltypes.Value, error) {
	if v.IsNull() {
		return sqltypes.NULL, nil
	}

	switch t.baseType {
	case sqltypes.Date:
		vt := values.ReadDate(v.Val)
		if vt.Equal(ZeroTime) {
			dest = append(dest, ZeroDateStr...)
		} else {
			dest = appendDateFormat(dest, vt)
		}
	case sqltypes.Datetime, sqltypes.Timestamp:
		x := values.ReadInt64(v.Val)
		vt := time.UnixMicro(x).UTC()
		dest = appendDatetimeFormat(dest, vt, t.precision)
	default:
		return sqltypes.Value{}, sql.ErrInvalidBaseType.New(t.baseType.String(), "datetime")
	}
	return sqltypes.MakeTrusted(t.baseType, dest), nil
}

func appendDateFormat(dest []byte, t time.Time) []byte {
	if t.Equal(ZeroTime) {
		dest = append(dest, ZeroDateStr...)
		return dest
	}
	year, m, d := t.Date()
	if year == 0 {
		dest = append(dest, '0', '0', '0', '0')
	} else {
		dest = strconv.AppendInt(dest, int64(year), 10)
	}
	month := int64(m)
	day := int64(d)
	dest = append(dest,
		'-',
		'0'+byte(month/10),
		'0'+byte(month%10),
		'-',
		'0'+byte(day/10),
		'0'+byte(day%10),
	)
	return dest
}

func appendDatetimeFormat(dest []byte, t time.Time, precision int) []byte {
	if t.Equal(ZeroTime) {
		dest = append(dest, ZeroTimestampDatetimeStrs[precision]...)
		return dest
	}
	dest = appendDateFormat(dest, t)
	dest = append(dest, ' ')
	h, m, s := t.Clock()
	dest = appendTimeFormat(dest, int64(h), int64(m), int64(s), int64(t.Nanosecond()/1000), precision)
	return dest
}

func (t datetimeType) String() string {
	switch t.baseType {
	case sqltypes.Date:
		return "date"
	case sqltypes.Datetime:
		if t.precision > 0 {
			return fmt.Sprintf("datetime(%d)", t.precision)
		}
		return "datetime"
	case sqltypes.Timestamp:
		if t.precision > 0 {
			return fmt.Sprintf("timestamp(%d)", t.precision)
		}
		return "timestamp"
	default:
		panic(sql.ErrInvalidBaseType.New(t.baseType.String(), "datetime"))
	}
}

// Type implements Type interface.
func (t datetimeType) Type() query.Type {
	return t.baseType
}

// ValueType implements Type interface.
func (t datetimeType) ValueType() reflect.Type {
	return datetimeValueType
}

func (t datetimeType) Zero() interface{} {
	return ZeroTime
}

// CollationCoercibility implements sql.CollationCoercible interface.
func (datetimeType) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// MaximumTime is the latest accepted time for this type.
func (t datetimeType) MaximumTime() time.Time {
	if t.baseType == sqltypes.Timestamp {
		return datetimeTypeMaxTimestamp
	}
	return datetimeTypeMaxDatetime
}

// MinimumTime is the earliest accepted time for this type.
func (t datetimeType) MinimumTime() time.Time {
	if t.baseType == sqltypes.Timestamp {
		return datetimeTypeMinTimestamp
	}
	return datetimeTypeMinDatetime
}

// ValidateTime receives a time and returns either that time or nil if it's
// not a valid time.
func ValidateTime(t time.Time) interface{} {
	if t.Before(datetimeMinTime) || t.After(datetimeMaxTime) {
		return nil
	}
	return t
}

// ValidateTimestamp receives a time and returns either that time or nil if it's
// not a valid timestamp.
func ValidateTimestamp(t time.Time) interface{} {
	if t.Before(datetimeTypeMinTimestamp) || t.After(datetimeTypeMaxTimestamp) {
		return nil
	}
	return t
}
