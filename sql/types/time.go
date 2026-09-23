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
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/values"
)

const (
	MaxTimePrecision = 6

	MinTime int64 = -3020399000000 // -838:59:59
	MaxTime int64 = 3020399000000  // 838:59:59

	MicrosPerSec  int64 = 1_000_000
	MicrosPerMin  int64 = 60 * MicrosPerSec
	MicrosPerHour int64 = 60 * MicrosPerMin
	MicrosPerDay  int64 = 24 * MicrosPerHour
)

var (
	Time             = MustCreateTimeType(0)
	TimeMaxPrecision = MustCreateTimeType(MaxTimePrecision)
	timeValueType    = reflect.TypeOf(int64(0))
)

type timeType struct {
	precision int
}

var _ sql.TimeType = timeType{}
var _ sql.CollationCoercible = timeType{}

// CreateTimeType creates a Type dealing with TIME.
func CreateTimeType(precision int) (sql.TimeType, error) {
	if precision < 0 || precision > MaxTimePrecision {
		return nil, sql.ErrTooBigPrecision.New(precision, MaxTimePrecision)
	}
	return timeType{
		precision: precision,
	}, nil
}

// MustCreateTimeType is the same as CreateTimeType except it panics on errors.
func MustCreateTimeType(precision int) sql.TimeType {
	typ, err := CreateTimeType(precision)
	if err != nil {
		panic(err)
	}
	return typ
}

// Convert implements the sql.Type interface.
func (t timeType) Convert(ctx context.Context, v any) (any, sql.ConvertInRange, error) {
	v, err := sql.UnwrapAny(ctx, v)
	if err != nil {
		return nil, sql.InRange, err
	}
	if v == nil {
		return nil, sql.InRange, nil
	}

	var res any
	var canConvNum bool = true
	switch value := v.(type) {
	case nil:
		return nil, sql.InRange, nil
	case []byte:
		return t.Convert(ctx, string(value))
	case string:
		// TODO: res, _, err = t.parseTime(value)
		impl, err := stringToTimespan(value)
		if err == nil {
			return impl, sql.InRange, nil
		}
		if strings.Contains(value, ".") {
			var val float64
			val, err = strconv.ParseFloat(value, 64)
			if err != nil {
				return nil, sql.InRange, err
			}
			return t.ConvertToTimespan(strAsDouble)
		} else {
			strAsInt, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return Timespan(0), ErrConvertingToTimeType.New(v)
			}
			return t.ConvertToTimespan(strAsInt)
		}
	case bool:
		if !value {
			return sql.Time(0), sql.InRange, nil
		}
		return sql.Time(MicrosPerSec), sql.InRange, nil
	case int:
		res, canConvNum = t.convertNumber(int64(value), 0)
	case int8:
		res, canConvNum = t.convertNumber(int64(value), 0)
	case int16:
		res, canConvNum = t.convertNumber(int64(value), 0)
	case int32:
		res, canConvNum = t.convertNumber(int64(value), 0)
	case int64:
		res, canConvNum = t.convertNumber(value, 0)
	case uint:
		res, canConvNum = t.convertNumber(int64(value), 0)
	case uint8:
		res, canConvNum = t.convertNumber(int64(value), 0)
	case uint16:
		res, canConvNum = t.convertNumber(int64(value), 0)
	case uint32:
		res, canConvNum = t.convertNumber(int64(value), 0)
	case uint64:
		res, canConvNum = t.convertNumber(int64(value), 0)
	case float32:
		if datetime, nsec, ok := splitFloat(float64(value)); ok {
			res, canConvNum = t.convertNumber(datetime, nsec)
		}
	case float64:
		if datetime, nsec, ok := splitFloat(value); ok {
			res, canConvNum = t.convertNumber(datetime, nsec)
		}
	case *apd.Decimal:
		if datetime, nsec, ok := splitDecimal(value); ok {
			res, canConvNum = t.convertNumber(datetime, nsec)
		}
	case time.Duration:
		microseconds := value.Nanoseconds() / nanosecondsPerMicrosecond
		return t.MicrosecondsToTimespan(microseconds), nil
	case time.Time:
		h, m, s := value.Clock()
		us := int64(value.Nanosecond())/nanosecondsPerMicrosecond +
			microsecondsPerSecond*int64(s) +
			microsecondsPerMinute*int64(m) +
			microsecondsPerHour*int64(h)
		return Timespan(us), nil
	}
	return ret, sql.InRange, err
}

func (t timeType) convertNumber(clock int64, micros int64) (sql.Time, bool) {
	absValue := int64Abs(value)
	if absValue >= -59 && absValue <= 59 {
		return t.MicrosecondsToTimespan(value * microsecondsPerSecond), nil
	} else if absValue >= 100 && absValue <= 9999 {
		minutes := absValue / 100
		seconds := absValue % 100
		if minutes <= 59 && seconds <= 59 {
			microseconds := (seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute)
			if value < 0 {
				return t.MicrosecondsToTimespan(-1 * microseconds), nil
			}
			return t.MicrosecondsToTimespan(microseconds), nil
		}
	} else if absValue >= 10000 && absValue <= 9999999 {
		hours := absValue / 10000
		minutes := (absValue / 100) % 100
		seconds := absValue % 100
		if minutes <= 59 && seconds <= 59 {
			microseconds := (seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute) + (hours * microsecondsPerHour)
			if value < 0 {
				return t.MicrosecondsToTimespan(-1 * microseconds), nil
			}
			return t.MicrosecondsToTimespan(microseconds), nil
		}
	}
}

// Compare implements the sql.Type interface.
func (t timeType) Compare(s context.Context, a, b any) (int, error) {
	if hasNulls, res := CompareNulls(a, b); hasNulls {
		return res, nil
	}

	as, err := t.ConvertToTimespan(a)
	if err != nil {
		return 0, err
	}
	bs, err := t.ConvertToTimespan(b)
	if err != nil {
		return 0, err
	}

	return as.Compare(bs), nil
}

// CompareValue implements the ValueType interface
func (t timeType) CompareValue(ctx *sql.Context, a, b sql.Value) (int, error) {
	panic("TODO: implement CompareValue for TimespanType")
}

// ConvertToTimespan converts the given interface value to a Timespan. This follows the conversion rules of MySQL, which
// are based on the base-10 visual representation of numbers (for example, Time.Convert() will interpret the value
// `1234` as 12 minutes and 34 seconds). Returns an error on a nil value.
func (t timeType) ConvertToTimespan(v interface{}) (Timespan, error) {
	switch value := v.(type) {
	case Timespan:
		// We only create a Timespan if it's valid, so we can skip this check if we receive a Timespan.
		// Timespan values are not intended to be modified by an integrator, therefore it is on the integrator if they corrupt a Timespan.
		return value, nil
	case int:
		return t.ConvertToTimespan(int64(value))
	case uint:
		return t.ConvertToTimespan(int64(value))
	case int8:
		return t.ConvertToTimespan(int64(value))
	case uint8:
		return t.ConvertToTimespan(int64(value))
	case int16:
		return t.ConvertToTimespan(int64(value))
	case uint16:
		return t.ConvertToTimespan(int64(value))
	case int32:
		return t.ConvertToTimespan(int64(value))
	case uint32:
		return t.ConvertToTimespan(int64(value))
	case int64:
		absValue := int64Abs(value)
		if absValue >= -59 && absValue <= 59 {
			return t.MicrosecondsToTimespan(value * microsecondsPerSecond), nil
		} else if absValue >= 100 && absValue <= 9999 {
			minutes := absValue / 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				microseconds := (seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute)
				if value < 0 {
					return t.MicrosecondsToTimespan(-1 * microseconds), nil
				}
				return t.MicrosecondsToTimespan(microseconds), nil
			}
		} else if absValue >= 10000 && absValue <= 9999999 {
			hours := absValue / 10000
			minutes := (absValue / 100) % 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				microseconds := (seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute) + (hours * microsecondsPerHour)
				if value < 0 {
					return t.MicrosecondsToTimespan(-1 * microseconds), nil
				}
				return t.MicrosecondsToTimespan(microseconds), nil
			}
		}
	case uint64:
		return t.ConvertToTimespan(int64(value))
	case float32:
		return t.ConvertToTimespan(float64(value))
	case float64:
		intValue := int64(value)
		microseconds := int64Abs(int64(math.Round((value - float64(intValue)) * float64(microsecondsPerSecond))))
		absValue := int64Abs(intValue)
		if absValue >= -59 && absValue <= 59 {
			totalMicroseconds := (absValue * microsecondsPerSecond) + microseconds
			if value < 0 {
				return t.MicrosecondsToTimespan(-1 * totalMicroseconds), nil
			}
			return t.MicrosecondsToTimespan(totalMicroseconds), nil
		} else if absValue >= 100 && absValue <= 9999 {
			minutes := absValue / 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				totalMicroseconds := (seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute) + microseconds
				if value < 0 {
					return t.MicrosecondsToTimespan(-1 * totalMicroseconds), nil
				}
				return t.MicrosecondsToTimespan(totalMicroseconds), nil
			}
		} else if absValue >= 10000 && absValue <= 9999999 {
			hours := absValue / 10000
			minutes := (absValue / 100) % 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				totalMicroseconds := (seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute) + (hours * microsecondsPerHour) + microseconds
				if value < 0 {
					return t.MicrosecondsToTimespan(-1 * totalMicroseconds), nil
				}
				return t.MicrosecondsToTimespan(totalMicroseconds), nil
			}
		}
	case *apd.Decimal:
		return t.ConvertToTimespan(DecimalRoundedIntPart(value))
	case string:
		impl, err := stringToTimespan(value)
		if err == nil {
			return impl, nil
		}
		if strings.Contains(value, ".") {
			strAsDouble, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return Timespan(0), ErrConvertingToTimeType.New(v)
			}
			return t.ConvertToTimespan(strAsDouble)
		} else {
			strAsInt, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return Timespan(0), ErrConvertingToTimeType.New(v)
			}
			return t.ConvertToTimespan(strAsInt)
		}
	case time.Duration:
		microseconds := value.Nanoseconds() / nanosecondsPerMicrosecond
		return t.MicrosecondsToTimespan(microseconds), nil
	case time.Time:
		h, m, s := value.Clock()
		us := int64(value.Nanosecond())/nanosecondsPerMicrosecond +
			microsecondsPerSecond*int64(s) +
			microsecondsPerMinute*int64(m) +
			microsecondsPerHour*int64(h)
		return Timespan(us), nil
	}

	return Timespan(0), ErrConvertingToTimeType.New(v)
}

// ConvertToTimeDuration implements the TimeType interface.
func (t timeType) ConvertToTimeDuration(v interface{}) (time.Duration, error) {
	val, err := t.ConvertToTimespan(v)
	if err != nil {
		return time.Duration(0), err
	}
	return val.AsTimeDuration(), nil
}

// Equals implements the Type interface.
func (t timeType) Equals(otherType sql.Type) bool {
	_, ok := otherType.(TimespanType_)
	return ok
}

// Promote implements the Type interface.
func (t timeType) Promote() sql.Type {
	return t
}

// SQL implements Type interface.
func (t timeType) SQL(_ *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	ti, err := t.ConvertToTimespan(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	dest = ti.AppendBytes(dest)
	return sqltypes.MakeTrusted(sqltypes.Time, dest), nil
}

// SQLValue implements ValueType interface.
func (t timeType) SQLValue(ctx *sql.Context, v sql.Value, dest []byte) (sqltypes.Value, error) {
	if v.IsNull() {
		return sqltypes.NULL, nil
	}

	x := values.ReadInt64(v.Val)
	Timespan(x).timespanToUnits()

	appendTimeFormat()
	dest = Timespan(x).AppendBytes(dest)
	return sqltypes.MakeTrusted(sqltypes.Time, dest), nil
}

// String implements Type interface.
func (t timeType) String() string {
	return "time(6)"
}

// Type implements Type interface.
func (t timeType) Type() query.Type {
	return sqltypes.Time
}

// ValueType implements Type interface.
func (t timeType) ValueType() reflect.Type {
	return timeValueType
}

// Zero implements Type interface.
func (t timeType) Zero() interface{} {
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

func stringToTimespan(s string) (Timespan, error) {
	var negative bool
	var hours int16
	var minutes int8
	var seconds int8
	var microseconds int32

	if len(s) > 0 && s[0] == '-' {
		negative = true
		s = s[1:]
	}

	comps := strings.SplitN(s, ".", 2)

	// Parse microseconds
	if len(comps) == 2 {
		microStr := comps[1]
		if len(microStr) < 6 {
			microStr += strings.Repeat("0", 6-len(comps[1]))
		}
		microStr, remainStr := microStr[0:6], microStr[6:]
		convertedMicroseconds, err := strconv.Atoi(microStr)
		if err != nil {
			return Timespan(0), ErrConvertingToTimeType.New(s)
		}
		// MySQL just uses the last digit to round up. This is weird, but matches their implementation.
		if len(remainStr) > 0 && remainStr[len(remainStr)-1:] >= "5" {
			convertedMicroseconds++
		}
		microseconds = int32(convertedMicroseconds)
	}

	// Parse H-M-S time
	hmsComps := strings.SplitN(comps[0], ":", 3)
	hms := make([]string, 3)
	if len(hmsComps) >= 2 {
		if len(hmsComps[0]) > 3 {
			return Timespan(0), ErrConvertingToTimeType.New(s)
		}
		hms[0] = hmsComps[0]
		if len(hmsComps[1]) > 2 {
			return Timespan(0), ErrConvertingToTimeType.New(s)
		}
		hms[1] = hmsComps[1]
		if len(hmsComps) == 3 {
			if len(hmsComps[2]) > 2 {
				return Timespan(0), ErrConvertingToTimeType.New(s)
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
		return Timespan(0), ErrConvertingToTimeType.New(s)
	}
	hours = int16(hmsHours)

	hmsMinutes, err := strconv.Atoi(hms[1])
	if len(hms[1]) > 0 && err != nil {
		return Timespan(0), ErrConvertingToTimeType.New(s)
	} else if hmsMinutes >= 60 {
		return Timespan(0), ErrConvertingToTimeType.New(s)
	}
	minutes = int8(hmsMinutes)

	hmsSeconds, err := strconv.Atoi(hms[2])
	if len(hms[2]) > 0 && err != nil {
		return Timespan(0), ErrConvertingToTimeType.New(s)
	} else if hmsSeconds >= 60 {
		return Timespan(0), ErrConvertingToTimeType.New(s)
	}
	seconds = int8(hmsSeconds)

	if microseconds == int32(microsecondsPerSecond) {
		microseconds = 0
		seconds++
	}
	if seconds == 60 {
		seconds = 0
		minutes++
	}
	if minutes == 60 {
		minutes = 0
		hours++
	}

	if hours > 838 {
		hours = 838
		minutes = 59
		seconds = 59
	}

	if hours == 838 && minutes == 59 && seconds == 59 {
		microseconds = 0
	}

	return unitsToTimespan(negative, hours, minutes, seconds, microseconds), nil
}

func safeSubstr(s string, start int, end int) string {
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
	}
	if start > len(s) {
		start = len(s)
		end = len(s)
	} else if end > len(s) {
		end = len(s)
	}
	return s[start:end]
}

// MicrosecondsToTimespan implements the TimeType interface.
func (_ TimespanType_) MicrosecondsToTimespan(v int64) Timespan {
	if v < MinTime {
		v = MinTime
	} else if v > MaxTime {
		v = MaxTime
	}
	return Timespan(v)
}

func unitsToTimespan(isNegative bool, hours int16, minutes int8, seconds int8, microseconds int32) Timespan {
	negative := int64(1)
	if isNegative {
		negative = -1
	}
	return Timespan(negative *
		(int64(microseconds) +
			(int64(seconds) * microsecondsPerSecond) +
			(int64(minutes) * microsecondsPerMinute) +
			(int64(hours) * microsecondsPerHour)))
}

func (t Timespan) timespanToUnits() (isNegative bool, hours int16, minutes int8, seconds int8, microseconds int32) {
	isNegative = t < 0
	absV := int64Abs(int64(t))
	hours = int16(absV / microsecondsPerHour)
	minutes = int8((absV / microsecondsPerMinute) % 60)
	seconds = int8((absV / microsecondsPerSecond) % 60)
	microseconds = int32(absV % microsecondsPerSecond)
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
		i = appendDigit(int64(microseconds), 6, ret, i)
	}

	return ret[:i]
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
	return time.Duration(t.AsMicroseconds() * nanosecondsPerMicrosecond)
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
	if v < MinTime {
		v = MinTime
	} else if v > MaxTime {
		v = MaxTime
	}
	return Timespan(v)
}

// Subtract returns a new Timespan that is the difference of the calling Timespan and given Timespan. The resulting
// Timespan is clamped to the allowed range.
func (t Timespan) Subtract(other Timespan) Timespan {
	v := int64(t - other)
	if v < MinTime {
		v = MinTime
	} else if v > MaxTime {
		v = MaxTime
	}
	return Timespan(v)
}
