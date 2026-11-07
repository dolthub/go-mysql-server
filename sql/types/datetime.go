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
	"strconv"
	"time"
	"unicode"

	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/encodings"
	"github.com/dolthub/go-mysql-server/sql/values"
)

const (
	MaxYear   = 9999
	MaxMonth  = 12
	MaxDay    = 31
	MaxHour   = 23
	MaxMinute = 59
	MaxSecond = 59
	MaxMicros = 999999

	// MaxDateWholeScale is the maximum number of digits needed to represent the whole portion of a date
	MaxDateWholeScale = 8
	// MaxDatetimeWholeScale is the maximum number of digits needed to represent the whole portion of a datetime
	MaxDatetimeWholeScale = 14
	// MaxDatetimePrecision is the maximum number of digits needed to represent the fractional portion of a datetime
	MaxDatetimePrecision = 6
	// MaxDatetimeStringLength is the longest the string representation of a datetime should ever be
	MaxDatetimeStringLength = 25
)

// TODO: remove these?
var (
	ZeroDateStr               = "0000-00-00"
	ZeroTimestampDatetimeStrs = [][]byte{
		[]byte("0000-00-00 00:00:00"),
		[]byte("0000-00-00 00:00:00.0"),
		[]byte("0000-00-00 00:00:00.00"),
		[]byte("0000-00-00 00:00:00.000"),
		[]byte("0000-00-00 00:00:00.0000"),
		[]byte("0000-00-00 00:00:00.00000"),
		[]byte("0000-00-00 00:00:00.000000"),
	}
)

var (
	// datetimeTypeMaxDatetime is the maximum representable Datetime/Date value. MYSQL: 9999-12-31 23:59:59.499999 (microseconds)
	datetimeTypeMaxDatetime = time.Date(9999, 12, 31, 23, 59, 59, 499999000, time.UTC)

	// datetimeTypeMinDatetime is the minimum representable Datetime/Date value. MYSQL: 1000-01-01 00:00:00.000000 (microseconds)
	datetimeTypeMinDatetime = time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)

	// datetimeTypeMaxTimestamp is the maximum representable Timestamp value, MYSQL: 2038-01-19 03:14:07.999999 (microseconds)
	datetimeTypeMaxTimestamp = time.Unix(math.MaxInt32, 999999000).UTC()

	// datetimeTypeMinTimestamp is the minimum representable Timestamp value, MYSQL: 1970-01-01 00:00:01.000000 (microseconds)
	datetimeTypeMinTimestamp = time.Unix(1, 0).UTC()

	// The MAX and MIN are extrapolated from commit ff05628a530 in the MySQL source code from my_time.cc
	// datetimeMaxTime is the maximum representable time value, MYSQL: 9999-12-31 23:59:59.999999 (microseconds)
	datetimeMaxTime = time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)

	// datetimeMinTime is the minimum representable time value, MYSQL: 0000-00-00 00:00:00.000000 (microseconds)
	datetimeMinTime = ZeroTime

	NoDelimiterDateLayout = "20060102"
	DateOnlyLayouts       = []string{
		"2006-01-02",
		"2006/01/02",
		NoDelimiterDateLayout,
		"2006-1-2",
	}

	// RFC3339NoTz represents the RFC3339 Datetime layout without a specified timezone
	RFC3339NoTz = "2006-01-02T15:04:05"

	// DatetimeTimezoneLayout represents standard Time.time.UTC()
	DatetimeTimezoneLayout = "2006-01-02 15:04:05.999999999 -0700 MST"

	// ExtraDatetimeLayouts hold additional Datetime layouts allowed for parsing.
	// This is to maintain existing compatibility for drivers that use time.Time directly.
	ExtraDatetimeLayouts = []string{
		time.RFC3339Nano,
		time.RFC3339,
		RFC3339NoTz,
		DatetimeTimezoneLayout,
	}

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
			return nil, sql.ErrTooBigPrecision.New(precision, MaxDatetimePrecision)
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
func (t datetimeType) Compare(ctx context.Context, a, b any) (int, error) {
	if hasNulls, res := CompareNulls(a, b); hasNulls {
		return res, nil
	}

	var ok bool
	var at, bt time.Time
	if av, _, err := t.Convert(ctx, a); err != nil {
		return 0, err
	} else if at, ok = av.(time.Time); !ok {
		return 1, nil
	}
	if bv, _, err := t.Convert(ctx, b); err != nil {
		return 0, err
	} else if bt, ok = bv.(time.Time); !ok {
		return 1, nil
	}

	if at.Before(bt) {
		return -1, nil
	}
	if at.After(bt) {
		return 1, nil
	}
	return 0, nil
}

// CompareValue implements the ValueType interface
func (t datetimeType) CompareValue(ctx *sql.Context, a, b sql.Value) (int, error) {
	panic("TODO: implement CompareValue for DatetimeType")
}

// splitFloat splits the float f into its whole and fractional parts
// invalid values will return false
func splitFloat(f float64) (int64, int64, bool) {
	if math.IsNaN(f) || f > math.MaxInt64 || f < math.MinInt64 {
		return 0, 0, false
	}
	whole := int64(f)
	return whole, int64((f - float64(whole)) * 1e9), true
}

// splitDecimal splits the decimal d into its whole and fractional parts
// invalid values will return false
func splitDecimal(d *apd.Decimal) (int64, int64, bool) {
	if d.Form != apd.Finite {
		return 0, 0, false
	}
	var wholeDec, fracDec apd.Decimal
	d.Modf(&wholeDec, &fracDec)
	whole, err := wholeDec.Int64()
	if err != nil {
		return 0, 0, false
	}
	fracDec.Exponent += 9
	fracDec.Modf(&fracDec, nil)
	frac, err := fracDec.Int64()
	if err != nil {
		return 0, 0, false
	}
	return whole, frac, true
}

// Convert implements Type interface.
func (t datetimeType) Convert(ctx context.Context, v any) (any, sql.ConvertInRange, error) {
	v, err := sql.UnwrapAny(ctx, v)
	if err != nil {
		return nil, sql.InRange, err
	}

	var res any
	var numericConvertible bool = true
	switch value := v.(type) {
	case nil:
		return nil, sql.InRange, nil
	case []byte:
		return t.Convert(ctx, string(value))
	case string:
		res, err = t.parseDatetime(value)
	case Timespan:
		// when receiving TIME, MySQL fills in date with today
		nowTimeStr := sql.Now().Format("2006-01-02")
		nowTime, err := time.Parse("2006-01-02", nowTimeStr)
		if err != nil {
			return ZeroTime, sql.InRange, sql.ErrTruncatedIncorrect.New(t.String(), v)
		}
		return nowTime.Add(value.AsTimeDuration()), sql.InRange, nil
	case time.Time:
		res = value.UTC()
	// For most integer values, we just return an error (but MySQL is more lenient for some of these). A special case
	// is zero values, which are important when converting from postgres defaults.
	case bool:
		if value {
			err = sql.ErrIncorrectValue.New(t.String(), v)
		}
		return ZeroTime, sql.InRange, err
	case int:
		res, numericConvertible = t.convertNumber(int64(value), 0)
	case int8:
		res, numericConvertible = t.convertNumber(int64(value), 0)
	case int16:
		res, numericConvertible = t.convertNumber(int64(value), 0)
	case int32:
		res, numericConvertible = t.convertNumber(int64(value), 0)
	case int64:
		res, numericConvertible = t.convertNumber(value, 0)
	// unsigned integers that overflow an int64 will be negative and fail a check in convertNumber
	case uint:
		res, numericConvertible = t.convertNumber(int64(value), 0)
	case uint8:
		res, numericConvertible = t.convertNumber(int64(value), 0)
	case uint16:
		res, numericConvertible = t.convertNumber(int64(value), 0)
	case uint32:
		res, numericConvertible = t.convertNumber(int64(value), 0)
	case uint64:
		res, numericConvertible = t.convertNumber(int64(value), 0)
	case float32:
		if datetime, nsec, ok := splitFloat(float64(value)); ok {
			res, numericConvertible = t.convertNumber(datetime, nsec)
		}
	case float64:
		if datetime, nsec, ok := splitFloat(value); ok {
			res, numericConvertible = t.convertNumber(datetime, nsec)
		}
	case *apd.Decimal:
		if datetime, nsec, ok := splitDecimal(value); ok {
			res, numericConvertible = t.convertNumber(datetime, nsec)
		}
	default:
		return ZeroTime, sql.InRange, sql.ErrConvertToSQL.New(value, t)
	}
	if !numericConvertible {
		return nil, sql.InRange, sql.ErrTruncatedIncorrect.New(t.String(), v)
	}
	if err != nil && !sql.IsConvertWarning(err) {
		return nil, sql.InRange, err
	}
	resTime, ok := res.(time.Time)
	if !ok {
		return nil, sql.InRange, sql.ErrTruncatedIncorrect.New(t.String(), v)
	}

	switch t.baseType {
	case sqltypes.Date:
		resTime = resTime.Truncate(24 * time.Hour)
		res = ValidateTime(resTime)
		if res == nil {
			return nil, sql.InRange, sql.ErrTruncatedIncorrect.New(t.String(), v)
		}
		resTime = res.(time.Time)
	case sqltypes.Timestamp:
		roundDuration := time.Second / time.Duration(precisionConversion[t.precision])
		resTime = resTime.Round(roundDuration)
		res = ValidateTimestamp(resTime)
		if res == nil {
			return nil, sql.InRange, sql.ErrTruncatedIncorrect.New(t.String(), v)
		}
		resTime = res.(time.Time)
	default:
		roundDuration := time.Second / time.Duration(precisionConversion[t.precision])
		resTime = resTime.Round(roundDuration)
		res = ValidateTime(resTime)
		if res == nil {
			return nil, sql.InRange, sql.ErrTruncatedIncorrect.New(t.String(), v)
		}
		resTime = res.(time.Time)
	}

	return resTime, sql.InRange, err // Keep any errors as potential warnings later
}

// getNumericParts is a helper function for converting Date and Time types to numeric types.
// Depending on t.baseType, this will return:
// - |whole| an int64 representing the date (and time) portion
// - |nsec| an int64 representing the nanoseconds
func (t datetimeType) getNumericParts(val time.Time) (int64, int64, error) {
	if val.Equal(ZeroTime) {
		return 0, 0, nil
	}
	var nsec int64
	year, month, day := val.Date()
	hour, mins, sec := val.Clock()
	whole := int64(year*100_00 + int(month)*100 + day)
	switch t.baseType {
	case sqltypes.Date:
	case sqltypes.Datetime, sqltypes.Timestamp:
		whole *= ClockScalar
		whole += int64(hour*100_00 + mins*100 + sec)
		nsec = int64(val.Nanosecond())
	default:
		return 0, 0, sql.ErrInvalidBaseType.New(t.baseType.String(), "datetime")
	}
	return whole, nsec, nil
}

// ToFloat64 implements the sql.DatetimeType interface.
// It converts time.Time{} into float64 according to MySQL's behavior.
func (t datetimeType) ToFloat64(val time.Time) (float64, error) {
	whole, nsec, err := t.getNumericParts(val)
	if err != nil {
		return 0, err
	}
	return float64(whole) + float64(nsec)/1e9, nil
}

// ToDecimal implements the sql.DatetimeType interface.
// It converts time.Time{} into *apd.Decimal according to MySQL's behavior.
func (t datetimeType) ToDecimal(val time.Time) (*apd.Decimal, error) {
	whole, nsec, err := t.getNumericParts(val)
	if err != nil {
		return nil, err
	}
	res := &apd.Decimal{}
	_, err = apd.BaseContext.Add(res, apd.New(whole, 0), apd.New(nsec, -9))
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ToString implements the sql.DatetimeType interface.
// It converts time.Time{} into string truncating the values according to the baseType and precision.
func (t datetimeType) ToString(val time.Time) (string, error) {
	buf := make([]byte, 0, MaxDatetimeStringLength)
	switch t.baseType {
	case sqltypes.Date:
		buf = appendDateFormat(buf, val)
	case sqltypes.Datetime, sqltypes.Timestamp:
		buf = appendDatetimeFormat(buf, val, t.precision)
	default:
		return "", sql.ErrInvalidBaseType.New(t.baseType.String(), "datetime")
	}
	return encodings.BytesToString(buf), nil
}

// precisionConversion is a conversion ratio to divide time.Second by to truncate the appropriate amount for the
// precision of a type with time info
var precisionConversion = [7]int64{
	1, 10, 100, 1_000, 10_000, 100_000, 1_000_000,
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

const (
	// MinDatetimeStringLength represents the length of the shortest possible datetime string
	// Example: 'Y-M-D' or 'YYMMD'
	MinDatetimeStringLength = 5
	// MaxNoDelimDatetimeLength represents the maximum length for a datetime without superfluous delimiters
	// Example: YYYYMMDDHHMMSS
	MaxNoDelimDatetimeLength = 14
	// FourDigitNoDelimDatetimeLength represents the length of a datetime without delimiters
	// Example: YYMMDDHHMMSS
	FourDigitNoDelimDatetimeLength = 8
)

// makeDatetime validates the date/time parameters and returns a time.Time object.
func makeDatetime(year, month, day, hour, min, sec, nsec int) (time.Time, bool) {
	if year > MaxYear || month > MaxMonth || day > MaxDay || hour > MaxHour || min > MaxMinute || sec > MaxSecond {
		return ZeroTime, false
	}
	// We do support ZERO_DATE, so zero for year, month, and day is allowed.
	if year == 0 && month == 0 && day == 0 {
		return time.Date(0, 0, 0, hour, min, sec, nsec, time.UTC), true
	}
	// We do NOT support ZERO_IN_DATE, so zero for month and day are not allowed.
	if month == 0 || day == 0 {
		return ZeroTime, false
	}
	if lastDay, _ := GetLastDay(year, month); day > lastDay {
		return ZeroTime, false
	}
	res := time.Date(year, time.Month(month), day, hour, min, sec, nsec, time.UTC)
	return res, true
}

// parseDatetimeExtraLayouts parses a string Datetime according to formats not directly supported by MySQL
func (t datetimeType) parseDatetimeExtraLayouts(str string) (any, error) {
	var res time.Time
	var err error
	for _, layout := range ExtraDatetimeLayouts {
		res, err = time.Parse(layout, str)
		if err == nil {
			return res, nil
		}
	}
	return res, err
}

// isMySQLPunct checks if |char| is considered a punctuation character under MySQL's rules.
// This set of characters is equivalent to the standard ASCII punctuation set minus the SPACE character (0x20).
func isMySQLPunct(char rune) bool {
	return (char >= 0x21 && char <= 0x2F) ||
		(char >= 0x3A && char <= 0x40) ||
		(char >= 0x5B && char <= 0x60) ||
		(char >= 0x7B && char <= 0x7E)
}

// readFuncN will return a slice of the string |s| where all runes satisfy |f|.
// The count |n| indicates the maximum number of runes to match, where n < 0 indicates any number of matches.
func readFuncN(str string, n int, f func(rune) bool) (string, string) {
	for idx, char := range str {
		if !f(char) || n == 0 {
			return str[:idx], str[idx:]
		}
		n -= 1
	}
	return str, ""
}

// parseTimeParts converts the strings |yearStr|, |monthStr|, and |dayStr| into the integer values.
// Additionally, two-digit years are expanded to the correct four-digit year.
// Empty string for any argument is invalid and will set |ok| = false.
func parseDateParts(yearStr, monthStr, dayStr string) (year, month, day int, ok bool) {
	var err error
	year, err = strconv.Atoi(yearStr)
	if err != nil {
		return 0, 0, 0, false
	}
	month, err = strconv.Atoi(monthStr)
	if err != nil {
		return 0, 0, 0, false
	}
	day, err = strconv.Atoi(dayStr)
	if err != nil {
		return 0, 0, 0, false
	}
	// MySQL special case for abbreviated ('00) date formats
	if (year != 0 || month != 0 || day != 0) && len(yearStr) == 2 {
		year = TwoDigitYear(year)
	}
	return year, month, day, true
}

// parseTimeParts converts the strings |hourStr|, |minStr|, and |secStr| into the integer values.
// Empty string for any argument is valid and is converted to 0.
func parseTimeParts(hourStr, minStr, secStr string) (hour, min, sec int, ok bool) {
	var err error
	if len(hourStr) > 0 {
		hour, err = strconv.Atoi(hourStr)
		if err != nil {
			return 0, 0, 0, false
		}
	}
	if len(minStr) > 0 {
		min, err = strconv.Atoi(minStr)
		if err != nil {
			return 0, 0, 0, false
		}
	}
	if len(secStr) > 0 {
		sec, err = strconv.Atoi(secStr)
		if err != nil {
			return 0, 0, 0, false
		}
	}
	return hour, min, sec, true
}

// parseMicros interprets |microStr| as a formatted microsecond string.
func parseMicros(microStr string) (micros int, ok bool) {
	if len(microStr) <= 1 {
		return 0, true
	}
	end := min(len(microStr), MaxDatetimePrecision+2)
	microsf64, err := strconv.ParseFloat(microStr[:end], 64)
	if err != nil {
		return 0, false
	}
	micros = int(math.Round(microsf64 * float64(microsPerSec)))
	return micros, true
}

// readMicros takes in a string and splits it into a valid microsecond and the remaining string according to
// MySQL's rules.
func readMicros(str string) (string, string) {
	if len(str) == 0 || str[0] != '.' {
		return "", str
	}
	var idx int
	for idx = 1; idx < len(str); idx++ {
		if !unicode.IsDigit(rune(str[idx])) {
			break
		}
	}
	return str[:idx], str[idx:]
}

// parseDatetime parses a Datetime according to MySQL rules.
// The date portion (YYYY-MM-DD) is required while all parts of the time portion (HH:MM:SS.MICROS) is optional.
// The standard datetime format is YYYY-MM-DD HH:MM:SS.MICROS, but MySQL supports a "relaxed" format where
// any punctuation (of various lengths) can be used between the date and time parts.
// Some exceptions:
//   - Whitespace characters are allowed in delimiter between Day and Hour
//   - The only valid delimiter between Seconds and Microseconds is a single decimal point (.)
//
// MySQL Reference: https://dev.mysql.com/doc/refman/8.4/en/datetime.html
func (t datetimeType) parseDatetime(origStr string) (any, error) {
	if len(origStr) == 0 {
		return nil, sql.ErrIncorrectValue.New(t.String(), origStr)
	}

	// Trim whitespaces
	var err error
	trimStr, str := readFuncN(origStr, -1, isMySQLDatetimeWhitespace)
	if len(str) == 0 {
		return nil, sql.ErrIncorrectValue.New(t.String(), origStr)
	}
	if len(trimStr) > 0 {
		err = sql.ErrDeprecatedDelimiter.New(origStr[0], t.String(), origStr)
	}

	if res, pErr := t.parseDatetimeExtraLayouts(str); pErr == nil {
		return res, err
	}

	var yearStr, monthStr, dayStr, hourStr, minStr, secStr, microStr string
	dtStr := str

	// If |yearStr| exceeds MinDatetimeStringLength AND is the end of the string OR is delimited by '.', then
	// |yearStr| is treated as a non-delimited date string with the remainder of the string
	yearStr, str = readFuncN(str, -1, unicode.IsDigit)
	if len(yearStr) >= MinDatetimeStringLength && (len(str) == 0 || str[0] == '.') {
		// Depending on the length of yearStr, the date is interpreted as having a two-digit year or four-digit year
		yearLen := 2
		if len(yearStr) == FourDigitNoDelimDatetimeLength || len(yearStr) >= MaxNoDelimDatetimeLength {
			yearLen = 4
		}
		// Restart parsing from beginning of |yearStr|
		yearStr, str = readFuncN(dtStr, yearLen, unicode.IsDigit)
	}

	if len(str) == 0 {
		return nil, sql.ErrIncorrectValue.New(t.String(), origStr)
	}

	delim := rune(str[0])
	trimStr, str = readFuncN(str, -1, isMySQLPunct)
	if len(trimStr) > 1 || (isMySQLPunct(delim) && delim != '-') {
		err = sql.ErrDeprecatedDelimiter.New(delim, t.String(), origStr)
	}

	// If this particular delimiter is MySQL punctuation but not '.', we use a "strict" datetime parsing format.
	// Otherwise, we use a "relaxed" datetime parsing format. Invalid characters are ignored and dealt with later.
	// In the "strict" format, date/time parts consist of any number of digits that must be delimited.
	// In the "relaxed" format, date/time parts consist of up to 2 digits with optional delimiters.
	numDigits := 2
	if isMySQLPunct(delim) && delim != '.' {
		numDigits = -1
	}

	monthStr, str = readFuncN(str, numDigits, unicode.IsDigit)
	trimStr, str = readFuncN(str, -1, isMySQLPunct)
	if len(trimStr) > 1 {
		err = sql.ErrDeprecatedDelimiter.New(delim, t.String(), origStr)
	}

	dayStr, str = readFuncN(str, numDigits, unicode.IsDigit)
	// A single capital 'T' is also allowed here
	if len(str) > 0 && str[0] == 'T' {
		str = str[1:]
	} else {
		trimStr, str = readFuncN(str, -1, func(r rune) bool {
			return isMySQLPunct(r) || isMySQLDatetimeWhitespace(r)
		})
		// Multiple valid punctuations/whitespaces or trailing punctuation/whitespace(s) after day results in a
		// delimiter error
		if len(trimStr) > 1 || len(str) == 0 && len(trimStr) > 0 {
			err = sql.ErrDeprecatedDelimiter.New(delim, t.String(), origStr)
		}
	}

	hourStr, str = readFuncN(str, numDigits, unicode.IsDigit)
	trimStr, str = readFuncN(str, -1, isMySQLPunct)
	if len(trimStr) > 1 {
		err = sql.ErrDeprecatedDelimiter.New(delim, t.String(), origStr)
	}

	// Trailing whitespace(s) after hour results in NULL
	trimStr, str = readFuncN(str, -1, isMySQLDatetimeWhitespace)
	if len(hourStr) > 0 && len(trimStr) > 0 {
		return nil, sql.ErrTruncatedIncorrect.New(t.String(), origStr)
	}

	minStr, str = readFuncN(str, numDigits, unicode.IsDigit)
	trimStr, str = readFuncN(str, -1, isMySQLPunct)
	if len(trimStr) > 1 {
		err = sql.ErrDeprecatedDelimiter.New(delim, t.String(), origStr)
	}

	// Trailing whitespace(s) after minute results in NULL
	trimStr, str = readFuncN(str, -1, isMySQLDatetimeWhitespace)
	if len(minStr) > 0 && len(trimStr) > 0 {
		return nil, sql.ErrTruncatedIncorrect.New(t.String(), origStr)
	}

	secStr, str = readFuncN(str, numDigits, unicode.IsDigit)
	microStr, str = readMicros(str)

	if len(str) > 0 {
		// Trailing '-' after everything results in NULL
		if str[0] == '-' {
			return nil, sql.ErrTruncatedIncorrect.New(t.String(), origStr)
		}

		// Trailing whitespace are considered a delimiter error, everything else is a truncation error
		trimStr, str = readFuncN(str, -1, isMySQLDatetimeWhitespace)
		if len(trimStr) > 0 {
			err = sql.ErrDeprecatedDelimiter.New(delim, t.String(), origStr)
		} else {
			err = sql.ErrTruncatedIncorrect.New(t.String(), origStr)
		}
	}

	year, month, day, ok := parseDateParts(yearStr, monthStr, dayStr)
	if !ok {
		return nil, sql.ErrIncorrectValue.New(t.String(), origStr)
	}
	hours, mins, secs, ok := parseTimeParts(hourStr, minStr, secStr)
	if !ok {
		return nil, sql.ErrTruncatedIncorrect.New(t.String(), origStr)
	}
	micros, ok := parseMicros(microStr)
	if !ok {
		return nil, sql.ErrTruncatedIncorrect.New(t.String(), origStr)
	}

	res, ok := makeDatetime(year, month, day, hours, mins, secs, micros*int(nanosPerMicro))
	if !ok {
		return nil, sql.ErrIncorrectValue.New(t.String(), origStr)
	}

	return res, err
}

const (
	// ClockScalar is used to shift datetime numbers into the appropriate powers during number to datetime conversions
	ClockScalar = 100_00_00 // HH_MM_SS

	// MinNumericDate is the smallest valid Date number
	MinNumericDate = 0000_01_01 // YYYY_MM_DD

	// MaxNumericDate is the largest valid Date number
	MaxNumericDate = 9999_12_31 // YYYY_MM_DD

	// MinNumeric1900AbbrevDate is the smallest valid Date that abbreviates the years 1970-1999
	MinNumeric1900AbbrevDate = 70_01_01 // YY_MM_DD

	// MaxNumeric1900AbbrevDate is the largest valid Date that abbreviates the years 1970-1999
	MaxNumeric1900AbbrevDate = 99_12_31 // YY_MM_DD

	// NumericDate1900Offset is used to convert numeric abbreviated dates to their unabbreviated form for 1900-1970
	NumericDate1900Offset = 1900_00_00 // YYYY_MM_DD

	// MaxNumeric2000AbbrevDate is the largest valid Date that abbreviates the years 2000-2069
	MaxNumeric2000AbbrevDate = 69_12_31 // YY_MM_DD

	// NumericDate2000Offset is used to convert numeric abbreviated dates to their unabbreviated form for 2000-2069
	NumericDate2000Offset = 2000_00_00 // YYYY_MM_DD

	// MaxNumericDatetime is the largest valid Datetime number
	MaxNumericDatetime = 9999_12_31_23_59_59 // YYYY_MM_DD_HH_MM_SS

	// MinNumeric1900AbbrevDatetime is the smallest valid Datetime that abbreviates the years 1970-1999
	MinNumeric1900AbbrevDatetime = 70_01_01_00_00_00 // YY_MM_DD_HH_MM_SS

	// MaxNumeric1900AbbrevDatetime is the largest valid Datetime that abbreviates the years 1970-1999
	MaxNumeric1900AbbrevDatetime = 99_12_31_23_59_59 // YY_MM_DD_HH_MM_SS

	// MaxNumeric2000AbbrevDatetime is the largest valid Datetime that abbreviates the years 2000-2069
	MaxNumeric2000AbbrevDatetime = 69_12_31_23_59_59 // YY_MM_DD_HH_MM_SS
)

// convertNumber converts datetime and nanos into the appropriate time.Time object according to the MySQL spec.
// Reference: https://github.com/google/mysql/blob/master/sql-common/my_time.c#L1209
func (t datetimeType) convertNumber(datetime int64, nsec int64) (time.Time, bool) {
	if datetime < 0 || nsec < 0 {
		return time.Time{}, false
	}
	if datetime == 0 && nsec == 0 {
		return ZeroTime, true
	}
	// convert each case to YYYY_MM_DD_HH_MM_SS format
	switch {
	case datetime < MinNumericDate:
		return time.Time{}, false
	case datetime <= MaxNumeric2000AbbrevDate:
		datetime = (datetime + NumericDate2000Offset) * ClockScalar
	case datetime < MinNumeric1900AbbrevDate:
		return time.Time{}, false
	case datetime <= MaxNumeric1900AbbrevDate:
		datetime = (datetime + NumericDate1900Offset) * ClockScalar
	case datetime <= MaxNumericDate:
		datetime *= ClockScalar
	case datetime <= MaxNumeric2000AbbrevDatetime:
		datetime += NumericDate2000Offset * ClockScalar
	case datetime < MinNumeric1900AbbrevDatetime:
		return time.Time{}, false
	case datetime <= MaxNumeric1900AbbrevDatetime:
		datetime += NumericDate1900Offset * ClockScalar
	case datetime > MaxNumericDatetime:
		return time.Time{}, false
	}

	date, clock := int(datetime/ClockScalar), int(datetime%ClockScalar)
	year, month, day := date/100_00, (date/100)%100, date%100
	hour, mins, sec := clock/100_00, (clock/100)%100, clock%100

	return makeDatetime(year, month, day, hour, mins, sec, int(nsec))
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

	val, _, err := t.Convert(ctx, v)
	if err != nil && !sql.ErrTruncatedIncorrect.Is(err) {
		return sqltypes.Value{}, err
	}
	if val == nil {
		val = ZeroTime
	}
	vt, ok := val.(time.Time)
	if !ok {
		return sqltypes.Value{}, sql.ErrConvertToSQL.New(v, t)
	}

	switch t.baseType {
	case sqltypes.Date:
		dest = appendDateFormat(dest, vt)
	case sqltypes.Datetime, sqltypes.Timestamp:
		dest = appendDatetimeFormat(dest, vt, t.precision)
	default:
		return sqltypes.Value{}, sql.ErrInvalidBaseType.New(t.baseType.String(), "datetime")
	}

	return sqltypes.MakeTrusted(t.baseType, dest), nil
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
	if t.Truncate(24 * time.Hour).Equal(ZeroTime) {
		dest = append(dest, ZeroDateStr...)
		return dest
	}
	year, m, d := t.Date()
	switch {
	case year < 10:
		dest = append(dest, '0', '0', '0')
	case year < 100:
		dest = append(dest, '0', '0')
	case year < 1000:
		dest = append(dest, '0')
	}
	dest = strconv.AppendInt(dest, int64(year), 10)
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
func ValidateTime(t time.Time) any {
	if t.Before(datetimeMinTime) || t.After(datetimeMaxTime) {
		return nil
	}
	return t
}

// ValidateTimestamp receives a time and returns either that time or nil if it's
// not a valid timestamp.
func ValidateTimestamp(t time.Time) any {
	if ZeroTime.Equal(t) {
		return t
	}
	if t.Before(datetimeTypeMinTimestamp) || t.After(datetimeTypeMaxTimestamp) {
		return nil
	}
	return t
}
