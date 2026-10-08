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
	"strings"
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
	timespanMinimum int64 = -3020399000000
	timespanMaximum int64 = 3020399000000
	microsPerSec    int64 = 1_000_000
	microsPerMin    int64 = 60 * microsPerSec
	microsPerHour   int64 = 60 * microsPerMin
	nanosPerMicro   int64 = 1000

	// MaxTimespan represents the largest valid TIME value -838:59:59 during string conversion
	MaxTimespan = Timespan(3020399_000000)
	// MinTimespan represents the smallest valid TIME value -838:59:59 during string conversion
	MinTimespan = -MaxTimespan
	// MaxNumericTimespan represents the smallest valid TIME value -838:59:59.999999 during number conversion
	MaxNumericTimespan = MaxTimespan + MaxMicros
	// MinNumericTimespan represents the smallest valid TIME value -838:59:59.999999 during number conversion
	MinNumericTimespan = -MaxNumericTimespan

	// MaxTimespanStringLength is the longest string representation of a valid TIME value (len(+111:22:33.123456))
	MaxTimespanStringLength = 17
)

var (
	timeValueType = reflect.TypeOf(Timespan(0))

	Time             = MustCreateTimespanType(0)
	TimeMaxPrecision = MustCreateTimespanType(6)
)

// TimeType represents the TIME type.
// https://dev.mysql.com/doc/refman/8.0/en/time.html
// TIME is implemented as TIME(6).
// The type of the returned value is Timespan.
type TimeType interface {
	sql.Type
	// ConvertToTimespan returns a Timespan from the given interface. Follows the same conversion rules as
	// Convert(), in that this will process the value based on its base-10 visual representation (for example, Convert()
	// will interpret the value `1234` as 12 minutes and 34 seconds). Returns an error for nil values.
	ConvertToTimespan(v interface{}) (Timespan, error)
	// ConvertToTimeDuration returns a time.Duration from the given interface. Follows the same conversion rules as
	// Convert(), in that this will process the value based on its base-10 visual representation (for example, Convert()
	// will interpret the value `1234` as 12 minutes and 34 seconds). Returns an error for nil values.
	ConvertToTimeDuration(v interface{}) (time.Duration, error)
	// MicrosecondsToTimespan returns a Timespan from the given number of microseconds. This differs from Convert(), as
	// that will process the value based on its base-10 visual representation (for example, Convert() will interpret
	// the value `1234` as 12 minutes and 34 seconds). This clamps the given microseconds to the allowed range.
	MicrosecondsToTimespan(v int64) Timespan

	// Precision returns the specified precision for this TimeType instance
	Precision() int
	// ToString converts the given valid Timespan to its string representation
	ToString(Timespan) (string, error)
}

type TimespanType_ struct {
	precision int
}

var _ TimeType = TimespanType_{}
var _ sql.CollationCoercible = TimespanType_{}

// CreateTimespanType creates a new TimeType that handles TIME values with the specified precision.
func CreateTimespanType(precision int) (TimeType, error) {
	if precision < 0 || precision > MaxDatetimePrecision {
		return nil, sql.ErrTooBigPrecision.New(precision, MaxDatetimePrecision)
	}
	return TimespanType_{
		precision: precision,
	}, nil
}

// MustCreateTimespanType is the same as CreateTimespanType except it panics on errors.
func MustCreateTimespanType(precision int) TimeType {
	typ, err := CreateTimespanType(precision)
	if err != nil {
		panic(err)
	}
	return typ
}

// MaxTextResponseByteLength implements the Type interface
func (t TimespanType_) MaxTextResponseByteLength(*sql.Context) uint32 {
	return MaxTimespanStringLength
}

// Compare implements Type interface.
func (t TimespanType_) Compare(ctx context.Context, a interface{}, b interface{}) (int, error) {
	if hasNulls, res := CompareNulls(a, b); hasNulls {
		return res, nil
	}

	var ok bool
	var at, bt Timespan
	if av, _, err := t.Convert(ctx, a); err != nil {
		return 0, err
	} else if at, ok = av.(Timespan); !ok {
		return 1, nil
	}
	if bv, _, err := t.Convert(ctx, b); err != nil {
		return 0, err
	} else if bt, ok = bv.(Timespan); !ok {
		return 1, nil
	}

	if at < bt {
		return -1, nil
	}
	if at > bt {
		return 1, nil
	}
	return 0, nil
}

// CompareValue implements the ValueType interface
func (t TimespanType_) CompareValue(ctx *sql.Context, a, b sql.Value) (int, error) {
	panic("TODO: implement CompareValue for TimespanType")
}

func (t TimespanType_) Convert(ctx context.Context, v any) (any, sql.ConvertInRange, error) {
	if v == nil {
		return nil, sql.InRange, nil
	}
	v, err := sql.UnwrapAny(ctx, v)
	if err != nil {
		return nil, sql.InRange, err
	}
	var res any
	var ok bool = true
	switch value := v.(type) {
	case Timespan:
		// We only create a Timespan if it's valid, so we can skip this check if we receive a Timespan.
		// Timespan values are not intended to be modified by an integrator, therefore it is on the integrator if they
		// corrupt a Timespan.
		return value, sql.InRange, nil
	case bool:
		if !value {
			return Timespan(0), sql.InRange, nil
		}
		return Timespan(microsPerSec), sql.InRange, nil
	case int:
		res, ok = t.convertNumber(int64(value), 0)
	case int8:
		res, ok = t.convertNumber(int64(value), 0)
	case int16:
		res, ok = t.convertNumber(int64(value), 0)
	case int32:
		res, ok = t.convertNumber(int64(value), 0)
	case int64:
		res, ok = t.convertNumber(value, 0)
	case uint:
		if value > math.MaxInt64 {
			return Timespan(0), sql.Overflow, sql.ErrTruncatedIncorrect.New(t.String(), value)
		}
		res, ok = t.convertNumber(int64(value), 0)
	case uint8:
		res, ok = t.convertNumber(int64(value), 0)
	case uint16:
		res, ok = t.convertNumber(int64(value), 0)
	case uint32:
		res, ok = t.convertNumber(int64(value), 0)
	case uint64:
		if value > math.MaxInt64 {
			return Timespan(0), sql.Overflow, sql.ErrTruncatedIncorrect.New(t.String(), value)
		}
		res, ok = t.convertNumber(int64(value), 0)
	case float32:
		var clock, nanos int64
		if clock, nanos, ok = splitFloat(float64(value)); ok {
			res, ok = t.convertNumber(clock, nanos)
		}
	case float64:
		var clock, nanos int64
		if clock, nanos, ok = splitFloat(value); ok {
			res, ok = t.convertNumber(clock, nanos)
		}
	case *apd.Decimal:
		var clock, nanos int64
		if clock, nanos, ok = splitDecimal(value); ok {
			res, ok = t.convertNumber(clock, nanos)
		}
	case time.Duration:
		micros := value.Nanoseconds() / nanosPerMicro
		return t.MicrosecondsToTimespan(micros), sql.InRange, nil
	case time.Time:
		hours, mins, secs := value.Clock()
		micros := int64(value.Nanosecond()) / nanosPerMicro
		res = t.makeTime(false, int64(hours), int64(mins), int64(secs), micros)
	case []byte:
		return t.Convert(ctx, string(value))
	case string:
		res, err = t.parseTime(value)
		if err != nil {
			return res, sql.InRange, err
		}
	default:
		return nil, sql.InRange, sql.ErrConvertToSQL.New(value, t)
	}
	if !ok {
		err = sql.ErrTruncatedIncorrect.New(t.String(), v)
	}

	return res, sql.InRange, err
}

// ConvertToTimespan converts the given interface value to a Timespan. This follows the conversion rules of MySQL, which
// are based on the base-10 visual representation of numbers (for example, Time.Convert() will interpret the value
// `1234` as 12 minutes and 34 seconds). Returns an error on a nil value.
func (t TimespanType_) ConvertToTimespan(v any) (Timespan, error) {
	var res any
	var ok bool = true
	switch value := v.(type) {
	case Timespan:
		// We only create a Timespan if it's valid, so we can skip this check if we receive a Timespan.
		// Timespan values are not intended to be modified by an integrator, therefore it is on the integrator if they
		// corrupt a Timespan.
		return value, nil
	case bool:
		if !value {
			return Timespan(0), nil
		}
		return Timespan(microsPerSec), nil
	case int:
		res, ok = t.convertNumber(int64(value), 0)
	case int8:
		res, ok = t.convertNumber(int64(value), 0)
	case int16:
		res, ok = t.convertNumber(int64(value), 0)
	case int32:
		res, ok = t.convertNumber(int64(value), 0)
	case int64:
		res, ok = t.convertNumber(value, 0)
	case uint:
		if value > math.MaxInt64 {
			return Timespan(0), sql.ErrTruncatedIncorrect.New(t.String(), value)
		}
		res, ok = t.convertNumber(int64(value), 0)
	case uint8:
		res, ok = t.convertNumber(int64(value), 0)
	case uint16:
		res, ok = t.convertNumber(int64(value), 0)
	case uint32:
		res, ok = t.convertNumber(int64(value), 0)
	case uint64:
		if value > math.MaxInt64 {
			return Timespan(0), sql.ErrTruncatedIncorrect.New(t.String(), value)
		}
		res, ok = t.convertNumber(int64(value), 0)
	case float32:
		var clock, nanos int64
		if clock, nanos, ok = splitFloat(float64(value)); ok {
			res, ok = t.convertNumber(clock, nanos)
		}
	case float64:
		var clock, nanos int64
		if clock, nanos, ok = splitFloat(value); ok {
			res, ok = t.convertNumber(clock, nanos)
		}
	case *apd.Decimal:
		var clock, nanos int64
		if clock, nanos, ok = splitDecimal(value); ok {
			res, ok = t.convertNumber(clock, nanos)
		}
	case time.Duration:
		micros := value.Nanoseconds() / nanosPerMicro
		return t.MicrosecondsToTimespan(micros), nil
	case time.Time:
		hours, mins, secs := value.Clock()
		micros := int64(value.Nanosecond()) / nanosPerMicro
		res = t.makeTime(false, int64(hours), int64(mins), int64(secs), micros)
	case []byte:
		return t.ConvertToTimespan(string(value))
	case string:
		impl, err := t.stringToTimespan(value)
		if err == nil {
			return impl, nil
		}
		if strings.Contains(value, ".") {
			strAsDouble, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return Timespan(0), sql.ErrTruncatedIncorrect.New(t.String(), value)
			}
			return t.ConvertToTimespan(strAsDouble)
		} else {
			strAsInt, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return Timespan(0), sql.ErrTruncatedIncorrect.New(t.String(), value)
			}
			return t.ConvertToTimespan(strAsInt)
		}
	default:
		return Timespan(0), sql.ErrConvertToSQL.New(value, t)
	}
	if !ok {
		return Timespan(0), sql.ErrTruncatedIncorrect.New(t.String(), v)
	}
	if v, ok := res.(Timespan); ok {
		return v, nil
	}
	return Timespan(0), nil
}

const (
	MaxTimeHour              int64 = 838
	MinNumericTime           int64 = -838_59_59
	MaxNumericTime           int64 = 838_59_59
	MinNumericDatetimeCutoff int64 = 01_01_01_00_00_00 // 2001-01-01 00:00:00.000000
)

func (t TimespanType_) convertNumber(clock int64, nanos int64) (any, bool) {
	// Some values are treated as Datetime types, and the time portion is extracted.
	// This only applies in the positive direction.
	if clock >= MinNumericDatetimeCutoff {
		timeVal, ok := datetimeType{}.convertNumber(clock, 0)
		if !ok {
			return nil, false
		}
		hours, mins, secs := timeVal.Clock()
		res := t.makeTime(false, int64(hours), int64(mins), int64(secs), nanos)
		return res, true
	}
	if clock < MinNumericTime || clock > MaxNumericTime {
		return nil, false
	}

	var isNeg bool
	if clock < 0 {
		isNeg = true
		clock = -clock
		nanos = -nanos
	}

	hours, mins, secs := clock/100_00, (clock/100)%100, clock%100
	if hours > MaxTimeHour {
		return nil, false
	}
	if mins > MaxMinute {
		return nil, false
	}
	if secs > MaxSecond {
		return nil, false
	}
	res := t.makeTime(isNeg, hours, mins, secs, nanos)
	if res > MaxTimespan+MaxMicros {
		return MaxTimespan, true
	}
	if res < MinNumericTimespan {
		return MinTimespan, true
	}
	return res, true
}

// makeTime creates a Timespan with the given parameters.
// nanos will be rounded according to TimespanType precision.
func (t TimespanType_) makeTime(isNeg bool, hours, mins, secs, nanos int64) Timespan {
	var neg int64 = 1
	if isNeg {
		neg = -1
	}

	precConv := precisionConversion[MaxDatetimePrecision-t.precision]
	microsFrac := float64(nanos) / float64(precConv*nanosPerMicro)
	microsFrac = math.Round(microsFrac)
	micros := int64(microsFrac * float64(precConv))
	res := Timespan(neg * (microsPerSec*secs +
		microsPerMin*mins +
		microsPerHour*hours +
		micros))

	return res
}

// ConvertToTimeDuration implements the TimeType interface.
func (t TimespanType_) ConvertToTimeDuration(v any) (time.Duration, error) {
	val, err := t.ConvertToTimespan(v)
	if err != nil {
		return time.Duration(0), err
	}
	return val.AsTimeDuration(), nil
}

// Equals implements the Type interface.
func (t TimespanType_) Equals(otherType sql.Type) bool {
	_, ok := otherType.(TimespanType_)
	return ok
}

// Promote implements the Type interface.
func (t TimespanType_) Promote() sql.Type {
	return t
}

// SQL implements Type interface.
func (t TimespanType_) SQL(_ *sql.Context, dest []byte, v any) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	ti, err := t.ConvertToTimespan(v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	isNeg, hours, mins, secs, micros := ti.timespanToUnits()
	if isNeg {
		dest = append(dest, '-')
	}
	dest = appendTimeFormat(dest, int64(hours), int64(mins), int64(secs), int64(micros), t.precision)
	return sqltypes.MakeTrusted(sqltypes.Time, dest), nil
}

// SQLValue implements ValueType interface.
func (t TimespanType_) SQLValue(ctx *sql.Context, v sql.Value, dest []byte) (sqltypes.Value, error) {
	if v.IsNull() {
		return sqltypes.NULL, nil
	}

	x := values.ReadInt64(v.Val)
	isNeg, hours, mins, secs, micros := Timespan(x).timespanToUnits()
	if isNeg {
		dest = append(dest, '-')
	}
	dest = appendTimeFormat(dest, int64(hours), int64(mins), int64(secs), int64(micros), t.precision)
	return sqltypes.MakeTrusted(sqltypes.Time, dest), nil
}

// String implements Type interface.
func (t TimespanType_) String() string {
	if t.precision == 0 {
		return "time"
	}
	return fmt.Sprintf("time(%d)", t.precision)
}

// Type implements Type interface.
func (t TimespanType_) Type() query.Type {
	return sqltypes.Time
}

// ValueType implements Type interface.
func (t TimespanType_) ValueType() reflect.Type {
	return timeValueType
}

// Zero implements Type interface.
func (t TimespanType_) Zero() interface{} {
	return Timespan(0)
}

// CollationCoercibility implements sql.CollationCoercible interface.
func (TimespanType_) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// No built in for absolute values on int64
func int64Abs(v int64) int64 {
	shift := v >> 63
	return (v ^ shift) - shift
}

// isMySQLPunct checks if the character is a valid punctuation character according to MySQL standards.
// This exists because MySQL's rules differ from unicode.IsPunct and regex punctuation
func isMySQLPunct(char rune) bool {
	// TODO: write a unit test for this
	return unicode.IsPunct(char) || char == '-' || char == ':' || char == '.'
}

var mysqlWhitespaces = [4]rune{' ', '\n', '\t', '\r'}

func isMySQLWhitespace(char rune) bool {
	for _, ws := range mysqlWhitespaces {
		if ws == char {
			return true
		}
	}
	return false
}

// parseTimeNoDelim converts the string, but with no delimiters
func (t TimespanType_) parseTimeNoDelim(isNeg bool, str string) (any, bool) {
	var clockStr string
	idx := strings.IndexFunc(str, func(r rune) bool {
		return !unicode.IsDigit(r)
	})
	if idx == -1 {
		clockStr = str
		str = ""
	} else {
		clockStr = str[:idx]
		str = str[idx:]
	}
	if len(clockStr) == 0 {
		return nil, false
	}
	if len(clockStr) >= 12 {
		res, ok, err := datetimeType{}.parseDatetime(str)
		return res, ok && err == nil
	}
	// format is HHHHHHHMMSS.MICROS
	cLen := len(clockStr)
	hourStr := safeSubstr(clockStr, 0, cLen-4)
	minStr := safeSubstr(clockStr, cLen-4, cLen-2)
	secStr := safeSubstr(clockStr, cLen-2, cLen)

	microStr, str := parseMicros(str)
	hours, mins, secs, micros, ok := t.parseTimeParts(hourStr, minStr, secStr, microStr)
	if !ok || mins > MaxMinute || secs > MaxSecond {
		return nil, false
	}
	if hours > MaxTimeHour {
		if isNeg {
			return MinTimespan, false
		} else {
			return MaxTimespan, false
		}
	}
	res := t.makeTime(isNeg, hours, mins, secs, micros*nanosPerMicro)
	if res > MaxTimespan {
		return MaxTimespan, false
	}
	if res < MinTimespan {
		return MinTimespan, false
	}
	return res, len(str) == 0
}

// parseTimePart will split |str| into the valid time part and the remaining string.
// A valid time portion is a ':' followed by at least one digit.
func parseTimePart(str string) (string, string) {
	if len(str) <= 1 || str[0] != ':' {
		return "", str
	}
	var idx int
	for idx = 1; idx < len(str); idx++ {
		if !unicode.IsDigit(rune(str[idx])) {
			if idx == 1 {
				return "", str
			}
			break
		}
	}
	return str[1:idx], str[idx:]
}

func trimWhitespaces(str string) (string, bool) {
	for idx, char := range str {
		if !isMySQLWhitespace(char) {
			return str[idx:], idx > 0
		}
	}
	return "", true
}

func (t TimespanType_) parseTimeDatetime(str string) (any, error) {
	dtType := datetimeType{
		baseType:  query.Type_DATETIME,
		precision: t.precision,
	}
	val, _, err := dtType.parseDatetime(str)
	dt, isDt := val.(time.Time)
	if !isDt {
		return nil, err
	}
	hours, mins, secs := dt.Clock()
	nanos := int64(dt.Nanosecond())
	return t.makeTime(false, int64(hours), int64(mins), int64(secs), nanos), err
}

func (t TimespanType_) parseTime(origStr string) (any, error) {
	if len(origStr) == 0 {
		return nil, sql.ErrTruncatedIncorrect.New(t.String(), origStr)
	}

	var err error
	var str = origStr
	str, didTrim := trimWhitespaces(str)
	if didTrim {
		err = sql.ErrTruncatedIncorrect.New(t.String(), origStr)
	}

	var isNeg bool
	if len(str) > 0 && str[0] == '-' {
		isNeg = true
		str = str[1:]
		if len(str) == 0 {
			return nil, sql.ErrTruncatedIncorrect.New(t.String(), origStr)
		}
	}

	var dtStr = str
	str, didTrim = trimWhitespaces(str)
	if didTrim {
		err = sql.ErrTruncatedIncorrect.New(t.String(), origStr)
		if len(str) == 0 {
			if isNeg {
				return Timespan(0), err
			}
		}
	}

	// read the hours part
	var trimStr = str
	var hourStr, minStr, secStr, microStr string
	idx := strings.IndexFunc(str, func(r rune) bool {
		return !unicode.IsDigit(r)
	})
	if idx == -1 {
		idx = len(str)
	}
	hourStr = str[:idx]
	str = str[idx:]
	minStr, str = parseTimePart(str)
	secStr, str = parseTimePart(str)
	if len(minStr) == 0 && len(secStr) == 0 {
		if len(dtStr) >= 12 {
			var res any
			res, err = t.parseTimeDatetime(dtStr)
			if err == nil {
				if didTrim {
					err = sql.ErrTruncatedIncorrect.New(t.String(), origStr)
				}
				return res, err
			}
			err = nil
		}

		res, ok := t.parseTimeNoDelim(isNeg, trimStr)
		if !ok {
			err = sql.ErrTruncatedIncorrect.New(t.String(), origStr)
		}
		// mysql special case
		if res == nil && didTrim {
			res = Timespan(0)
		}
		return res, err
	}

	microStr, str = parseMicros(str)

	// MySQL Special Case
	// If everything so far is a valid delimited TIME without microseconds followed by a MySQL Whitespace AND the
	// trimmed string is greater than or equal to 12 in length, parse as a datetime string.
	if len(str) > 0 && isMySQLWhitespace(rune(str[0])) &&
		len(hourStr) > 0 &&
		len(minStr) > 0 &&
		len(secStr) > 0 &&
		len(microStr) <= 1 &&
		len(dtStr) >= 12 {
		var res any
		res, err = t.parseTimeDatetime(dtStr)
		if err != nil {
			return res, err
		}
		if didTrim {
			err = sql.ErrTruncatedIncorrect.New(t.String(), origStr)
		}
		return res, err
	}

	hours, mins, secs, micros, ok := t.parseTimeParts(hourStr, minStr, secStr, microStr)
	if !ok || mins > MaxMinute || secs > MaxSecond {
		return nil, sql.ErrTruncatedIncorrect.New(t.String(), origStr)
	}
	if hours > MaxTimeHour {
		if isNeg {
			return MinTimespan, sql.ErrTruncatedIncorrect.New(t.String(), origStr)
		} else {
			return MaxTimespan, sql.ErrTruncatedIncorrect.New(t.String(), origStr)
		}
	}
	res := t.makeTime(isNeg, hours, mins, secs, micros*nanosPerMicro)
	if res > MaxTimespan {
		return MaxTimespan, sql.ErrTruncatedIncorrect.New(t.String(), origStr)
	}
	if res < MinTimespan {
		return MinTimespan, sql.ErrTruncatedIncorrect.New(t.String(), origStr)
	}
	if len(str) > 0 {
		err = sql.ErrTruncatedIncorrect.New(t.String(), origStr)
	}
	return res, err
}

func (t TimespanType_) parseTimeParts(hourStr, minStr, secStr, microStr string) (hours, mins, secs, micros int64, ok bool) {
	var err error
	if len(hourStr) > 0 {
		hours, err = strconv.ParseInt(hourStr, 10, 64)
		if err != nil {
			return 0, 0, 0, 0, false
		}
	}
	if len(minStr) > 0 {
		mins, err = strconv.ParseInt(minStr, 10, 64)
		if err != nil {
			return 0, 0, 0, 0, false
		}
	}
	if len(secStr) > 0 {
		secs, err = strconv.ParseInt(secStr, 10, 64)
		if err != nil {
			return 0, 0, 0, 0, false
		}
	}
	if len(microStr) > 1 { // the first character is expected to be '.'
		// MySQL's weird special case for strings with microseconds
		if t.precision < MaxDatetimePrecision ||
			t.precision == MaxDatetimePrecision &&
				len(microStr) > MaxDatetimePrecision+1 &&
				microStr[len(microStr)-1] < '5' {
			microStr = microStr[:min(len(microStr), MaxDatetimePrecision+1)]
		}

		var microsf64 float64
		microsf64, err = strconv.ParseFloat(microStr, 64)
		if err != nil {
			return 0, 0, 0, 0, false
		}
		micros = int64(math.Round(microsf64 * float64(microsPerSec)))
	}
	return hours, mins, secs, micros, true
}

func (t TimespanType_) stringToTimespan(s string) (Timespan, error) {
	var isNeg bool
	var hours, mins, secs, nanos int64

	if len(s) > 0 && s[0] == '-' {
		isNeg = true
		s = s[1:]
	}

	comps := strings.SplitN(s, ".", 2)

	// Parse microseconds
	if len(comps) == 2 {
		microStr := comps[1]
		if len(microStr) < t.precision {
			microStr += strings.Repeat("0", t.precision-len(microStr))
		}
		microStr, remainStr := microStr[0:t.precision], microStr[t.precision:]
		var convertedMicroseconds int
		if len(microStr) > 0 {
			var err error
			convertedMicroseconds, err = strconv.Atoi(microStr)
			if err != nil {
				return Timespan(0), sql.ErrTruncatedIncorrect.New(t.String(), s)
			}
		}

		if len(remainStr) > 0 {
			var roundChar byte
			// MySQL has a weird special case where MaxPrecision causes it to use the last digit to round.
			if t.precision == MaxDatetimePrecision {
				roundChar = remainStr[len(remainStr)-1]
			} else {
				roundChar = remainStr[0]
			}
			if roundChar >= '5' {
				convertedMicroseconds++
			}
		}
		nanos = int64(convertedMicroseconds)
		for i := 0; i < MaxDatetimePrecision-t.precision; i++ {
			nanos *= 10
		}
		nanos *= nanosPerMicro
	}

	// Parse H-M-S time
	hmsComps := strings.SplitN(comps[0], ":", 3)
	hms := make([]string, 3)
	if len(hmsComps) >= 2 {
		if len(hmsComps[0]) > 3 {
			return Timespan(0), sql.ErrTruncatedIncorrect.New(t.String(), s)
		}
		hms[0] = hmsComps[0]
		if len(hmsComps[1]) > 2 {
			return Timespan(0), sql.ErrTruncatedIncorrect.New(t.String(), s)
		}
		hms[1] = hmsComps[1]
		if len(hmsComps) == 3 {
			if len(hmsComps[2]) > 2 {
				return Timespan(0), sql.ErrTruncatedIncorrect.New(t.String(), s)
			}
			hms[2] = hmsComps[2]
		}
	} else {
		l := len(hmsComps[0])
		hms[2] = safeSubstr(hmsComps[0], l-2, l)
		hms[1] = safeSubstr(hmsComps[0], l-4, l-2)
		hms[0] = safeSubstr(hmsComps[0], l-7, l-4)
	}

	hmsHours, err := strconv.Atoi(hms[0])
	if len(hms[0]) > 0 && err != nil {
		return Timespan(0), sql.ErrTruncatedIncorrect.New(t.String(), s)
	}
	hours = int64(hmsHours)

	hmsMinutes, err := strconv.Atoi(hms[1])
	if len(hms[1]) > 0 && err != nil {
		return Timespan(0), sql.ErrTruncatedIncorrect.New(t.String(), s)
	} else if hmsMinutes >= 60 {
		return Timespan(0), sql.ErrTruncatedIncorrect.New(t.String(), s)
	}
	mins = int64(hmsMinutes)

	hmsSeconds, err := strconv.Atoi(hms[2])
	if len(hms[2]) > 0 && err != nil {
		return Timespan(0), sql.ErrTruncatedIncorrect.New(t.String(), s)
	} else if hmsSeconds >= 60 {
		return Timespan(0), sql.ErrTruncatedIncorrect.New(t.String(), s)
	}
	secs = int64(hmsSeconds)

	// special case for time strings overflowing hours results in max time
	if hours > MaxTimeHour {
		if isNeg {
			return MinTimespan, nil
		}
		return MaxTimespan, nil
	}
	// another special case for strings that are max time
	if hours == MaxTimeHour && mins == MaxMinute && secs == MaxSecond {
		if isNeg {
			return MinTimespan, nil
		}
		return MaxTimespan, nil
	}

	res := t.makeTime(isNeg, hours, mins, secs, nanos)
	return res, nil
}

func safeSubstr(s string, start int, end int) string {
	start = max(start, 0)
	start = min(start, len(s))
	end = max(max(end, 0), start)
	end = min(end, len(s))
	return s[start:end]
}

// MicrosecondsToTimespan implements the TimeType interface.
func (_ TimespanType_) MicrosecondsToTimespan(v int64) Timespan {
	if v < timespanMinimum {
		v = timespanMinimum
	} else if v > timespanMaximum {
		v = timespanMaximum
	}
	return Timespan(v)
}

// Precision implements the TimeType interface.
func (t TimespanType_) Precision() int {
	return t.precision
}

// ToString implements the sql.TimeType interface.
// It converts a value Timespan into a string rounding the microseconds accord to the Type's precision.
func (t TimespanType_) ToString(val Timespan) (string, error) {
	dest := make([]byte, 0, MaxTimespanStringLength)
	isNeg, hours, mins, secs, micros := val.timespanToUnits()
	if isNeg {
		dest = append(dest, '-')
	}
	dest = appendTimeFormat(dest, int64(hours), int64(mins), int64(secs), int64(micros), t.precision)
	return encodings.BytesToString(dest), nil
}

func unitsToTimespan(isNegative bool, hours int16, minutes int8, seconds int8, microseconds int32) Timespan {
	negative := int64(1)
	if isNegative {
		negative = -1
	}
	return Timespan(negative *
		(int64(microseconds) +
			(int64(seconds) * microsPerSec) +
			(int64(minutes) * microsPerMin) +
			(int64(hours) * microsPerHour)))
}

// Timespan is the value type returned by TimeType.Convert().
type Timespan int64

func (t Timespan) timespanToUnits() (isNegative bool, hours int16, minutes int8, seconds int8, microseconds int32) {
	isNegative = t < 0
	absV := int64Abs(int64(t))
	hours = int16(absV / microsPerHour)
	minutes = int8((absV / microsPerMin) % 60)
	seconds = int8((absV / microsPerSec) % 60)
	microseconds = int32(absV % microsPerSec)
	return
}

// String returns the Timespan formatted as a string (such as for display purposes).
func (t Timespan) String() string {
	return string(t.Bytes())
}

func (t Timespan) Bytes() []byte {
	isNegative, hours, minutes, seconds, microseconds := t.timespanToUnits()
	sz := 10
	if microseconds > 0 {
		sz += 7
	}
	ret := make([]byte, sz)
	i := 0
	if isNegative {
		ret[0] = '-'
		i++
	}

	i = appendDigit(int64(hours), 2, ret, i)
	ret[i] = ':'
	i++
	i = appendDigit(int64(minutes), 2, ret, i)
	ret[i] = ':'
	i++
	i = appendDigit(int64(seconds), 2, ret, i)
	if microseconds > 0 {
		ret[i] = '.'
		i++
		i = appendDigit(int64(microseconds), t.precision(), ret, i)
	}

	return ret[:i]
}

// precision is the number of digits of sub-second precision.
// For the timespan type, this is currently always 6 (microsecond precision)
// See https://github.com/dolthub/dolt/issues/10661
func (t Timespan) precision() int {
	return 6
}

func (t Timespan) AppendBytes(dest []byte) []byte {
	isNeg, h, m, s, ms := t.timespanToUnits()
	if isNeg {
		dest = append(dest, '-')
	}
	dest = appendTimeFormat(dest, int64(h), int64(m), int64(s), int64(ms), t.precision())
	return dest
}

func appendTimeFormat(dest []byte, h, m, s, ms int64, msPrecision int) []byte {
	if h < 10 {
		dest = append(dest, '0')
	}
	dest = strconv.AppendInt(dest, h, 10)
	dest = append(dest,
		':',
		'0'+byte(m/10), '0'+byte(m%10), ':',
		'0'+byte(s/10), '0'+byte(s%10))

	if msPrecision > 0 {
		dest = appendMicroseconds(dest, ms, msPrecision)
	}

	return dest
}

// appendDigit format prints 0-extended integer into buffer
func appendDigit(v int64, extend int, buf []byte, i int) int {
	cmp := int64(1)
	for _ = range extend - 1 {
		cmp *= 10
	}
	for cmp > 0 && v < cmp {
		buf[i] = '0'
		i++
		cmp /= 10
	}
	if v == 0 {
		return i
	}
	tmpBuf := strconv.AppendInt(buf[i:i], v, 10)
	return i + len(tmpBuf)
}

func appendMicroseconds(dest []byte, micros int64, precision int) []byte {
	if precision <= 0 {
		return dest
	}
	subSecondSize := precisionConversion[MaxDatetimePrecision-precision]
	subSeconds := micros / subSecondSize
	dest = append(dest, '.')
	cmp := precisionConversion[precision-1]
	for cmp > 1 && subSeconds < cmp {
		dest = append(dest, '0')
		cmp /= 10
	}
	dest = strconv.AppendInt(dest, subSeconds, 10)
	return dest
}

// AsMicroseconds returns the Timespan in microseconds.
func (t Timespan) AsMicroseconds() int64 {
	// Timespan already being implemented in microseconds is an implementation detail that integrators do not need to
	// know about. This is also the reason for the comparison functions.
	return int64(t)
}

// AsTimeDuration returns the Timespan as a time.Duration.
func (t Timespan) AsTimeDuration() time.Duration {
	return time.Duration(t.AsMicroseconds() * nanosPerMicro)
}

// Equals returns whether the calling Timespan and given Timespan are equivalent.
func (t Timespan) Equals(other Timespan) bool {
	return t == other
}

// Compare returns an integer comparing two values. The result will be 0 if t==other, -1 if t < other, and +1 if t > other.
func (t Timespan) Compare(other Timespan) int {
	if t < other {
		return -1
	} else if t > other {
		return 1
	}
	return 0
}

// Negate returns a new Timespan that has been negated.
func (t Timespan) Negate() Timespan {
	return -1 * t
}

// Add returns a new Timespan that is the sum of the calling Timespan and given Timespan. The resulting Timespan is
// clamped to the allowed range.
func (t Timespan) Add(other Timespan) Timespan {
	v := int64(t + other)
	if v < timespanMinimum {
		v = timespanMinimum
	} else if v > timespanMaximum {
		v = timespanMaximum
	}
	return Timespan(v)
}

// Subtract returns a new Timespan that is the difference of the calling Timespan and given Timespan. The resulting
// Timespan is clamped to the allowed range.
func (t Timespan) Subtract(other Timespan) Timespan {
	v := int64(t - other)
	if v < timespanMinimum {
		v = timespanMinimum
	} else if v > timespanMaximum {
		v = timespanMaximum
	}
	return Timespan(v)
}
