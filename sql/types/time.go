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

	SecsPerMin  int64 = 60
	MinsPerHour int64 = 60
	HoursPerDay int64 = 24

	MicrosPerSec  int64 = 1_000_000
	MicrosPerMin  int64 = MicrosPerSec * SecsPerMin
	MicrosPerHour int64 = MicrosPerMin * MinsPerHour
	MicrosPerDay  int64 = MicrosPerHour * HoursPerDay
	NanosPerMicro int64 = 1_000
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
// This follows the conversion rules of MySQL, which are based on the base-10 visual representation of numbers.
// For example, Time.Convert() will interpret the value `1234` as 12 minutes and 34 seconds).
// Returns an error on a nil value.
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
		// TODO: sometimes these are parsed as datetimes
		res, err = t.parseTime(value)
		if err == nil {
			return res, sql.InRange, nil
		}
		// Treat string as either a float64 or int64
		if strings.Contains(value, ".") {
			v, err = strconv.ParseFloat(value, 64)
		} else {
			v, err = strconv.ParseInt(value, 10, 64)
		}
		if err != nil {
			return nil, sql.InRange, err
		}
		return t.Convert(ctx, v)
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
			res, canConvNum = t.convertNumber(datetime, nsec/NanosPerMicro)
		}
	case float64:
		if datetime, nsec, ok := splitFloat(value); ok {
			res, canConvNum = t.convertNumber(datetime, nsec/NanosPerMicro)
		}
	case *apd.Decimal:
		if datetime, nsec, ok := splitDecimal(value); ok {
			res, canConvNum = t.convertNumber(datetime, nsec/NanosPerMicro)
		}
	case time.Duration:
		// TODO: check bounds
		micros := value.Nanoseconds() / NanosPerMicro
		return sql.Time(micros), sql.InRange, nil
	case time.Time:
		hours, mins, secs := value.Clock()
		res, _ = makeTime(false, int64(hours), int64(mins), int64(secs), int64(value.Nanosecond())/NanosPerMicro)
	default:
		return sql.Time(0), sql.InRange, sql.ErrConvertToSQL.New(value, t.String())
	}

	if !canConvNum {
		return nil, sql.InRange, sql.ErrTruncatedIncorrect.New(t.String(), v)
	}
	if err != nil && !sql.ErrTruncatedIncorrect.Is(err) {
		return nil, sql.InRange, err
	}
	return res, sql.InRange, err
}

func (t timeType) convertNumber(clock int64, micros int64) (sql.Time, bool) {
	// TODO: handle datetimes here too
	// TODO: constants
	if clock > 838_59_59 {
		return sql.Time(0), false
	}
	if clock < -838_59_59 {
		return sql.Time(0), false
	}
	var isNeg bool
	absClock := clock
	if clock < 0 {
		isNeg = true
		absClock = -clock
	}
	hours := absClock / ClockScalar
	mins := (absClock / 100) % 100
	secs := absClock % 100
	ts, ok := makeTime(isNeg, hours, mins, secs, micros)
	if !ok {
		return sql.Time(0), false
	}
	return ts, true
}

// Compare implements the sql.Type interface.
func (t timeType) Compare(ctx context.Context, a, b any) (int, error) {
	if hasNulls, res := CompareNulls(a, b); hasNulls {
		return res, nil
	}

	var ok bool
	var at, bt sql.Time
	if av, _, err := t.Convert(ctx, a); err != nil {
		return 0, err
	} else if at, ok = av.(sql.Time); !ok {
		return 1, nil
	}
	if bv, _, err := t.Convert(ctx, b); err != nil {
		return 0, err
	} else if bt, ok = bv.(sql.Time); !ok {
		return 1, nil
	}

	if at > bt {
		return 1, nil
	}
	if at < bt {
		return -1, nil
	}
	return 0, nil
}

// CompareValue implements the ValueType interface
func (t timeType) CompareValue(ctx *sql.Context, a, b sql.Value) (int, error) {
	panic("TODO: implement CompareValue for TimespanType")
}

// ConvertToTimeDuration implements the TimeType interface.
func (t timeType) ConvertToTimeDuration(ctx context.Context, v any) (time.Duration, error) {
	val, _, err := t.Convert(ctx, v)
	if err != nil {
		return time.Duration(0), err
	}
	return time.Duration(int64(val.(sql.Time)) * NanosPerMicro), nil
}

// Equals implements the Type interface.
func (t timeType) Equals(otherType sql.Type) bool {
	otherTimeType, ok := otherType.(sql.TimeType)
	if !ok {
		return false
	}
	return t.precision == otherTimeType.Precision()
}

// MaxTextResponseByteLength implements the Type interface
func (t timeType) MaxTextResponseByteLength(_ *sql.Context) uint32 {
	// 10 digits are required for a text representation without microseconds, but with microseconds
	// requires 17, so return 17 as an upper limit (i.e. len(+123:00:00.999999"))
	return 17
}

// Promote implements the Type interface.
func (t timeType) Promote() sql.Type {
	return t
}

// SQL implements Type interface.
func (t timeType) SQL(ctx *sql.Context, dest []byte, v any) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}
	val, _, err := t.Convert(ctx, v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	vt, ok := val.(sql.Time)
	if !ok {
		return sqltypes.Value{}, sql.ErrConvertToSQL.New(v, t)
	}
	isNeg, hours, mins, secs, micros := t.timeToUnits(vt)
	dest = appendTimeFormat(dest, isNeg, hours, mins, secs, micros, t.precision)
	return sqltypes.MakeTrusted(sqltypes.Time, dest), nil
}

// SQLValue implements ValueType interface.
func (t timeType) SQLValue(ctx *sql.Context, v sql.Value, dest []byte) (sqltypes.Value, error) {
	if v.IsNull() {
		return sqltypes.NULL, nil
	}

	x := values.ReadInt64(v.Val)
	isNeg, hours, mins, secs, micros := t.timeToUnits(sql.Time(x))
	dest = appendTimeFormat(dest, isNeg, hours, mins, secs, micros, t.precision)
	return sqltypes.MakeTrusted(sqltypes.Time, dest), nil
}

// String implements Type interface.
func (t timeType) String() string {
	if t.precision == 0 {
		return "TIME"
	}
	return fmt.Sprintf("TIME(%d)", t.precision)
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
	return sql.Time(0)
}

// No built in for absolute values on int64
func int64Abs(v int64) int64 {
	shift := v >> 63
	return (v ^ shift) - shift
}

func (t timeType) parseTime(str string) (sql.Time, error) {
	value := strings.Trim(str, NumericCutSet)
	if len(value) == 0 {
		return sql.Time(0), sql.ErrIncorrectValue.New(t.String(), str)
	}

	var isNeg bool
	var hours, mins, secs, micros int64
	if value[0] == '-' {
		isNeg = true
		value = value[1:]
	}

	comps := strings.SplitN(value, ".", 2)

	// Parse microseconds
	if len(comps) == 2 {
		microStr := comps[1]
		if len(microStr) < MaxTimePrecision {
			microStr += strings.Repeat("0", MaxTimePrecision-len(comps[1]))
		}
		microStr, remainStr := microStr[0:6], microStr[6:]
		convertedMicroseconds, err := strconv.Atoi(microStr)
		if err != nil {
			return sql.Time(0), sql.ErrIncorrectValue.New(t.String(), str)
		}
		// MySQL just uses the last digit to round up.
		if len(remainStr) > 0 && remainStr[len(remainStr)-1:] >= "5" {
			convertedMicroseconds++
		}
		micros = int64(convertedMicroseconds)
	}

	// Parse H-M-S time
	hmsComps := strings.SplitN(comps[0], ":", 3)
	hms := make([]string, 3)
	if len(hmsComps) >= 2 {
		if len(hmsComps[0]) > 3 {
			return sql.Time(0), sql.ErrIncorrectValue.New(t.String(), str)
		}
		hms[0] = hmsComps[0]
		if len(hmsComps[1]) > 2 {
			return sql.Time(0), sql.ErrIncorrectValue.New(t.String(), str)
		}
		hms[1] = hmsComps[1]
		if len(hmsComps) == 3 {
			if len(hmsComps[2]) > 2 {
				return sql.Time(0), sql.ErrIncorrectValue.New(t.String(), str)
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
	if err != nil {
		return sql.Time(0), sql.ErrIncorrectValue.New(t.String(), str)
	}
	hours = int64(hmsHours)

	hmsMins, err := strconv.Atoi(hms[1])
	if len(hms[1]) > 0 && err != nil {
		return sql.Time(0), sql.ErrIncorrectValue.New(t.String(), str)
	} else if hmsMins > MaxMinute {
		return sql.Time(0), sql.ErrIncorrectValue.New(t.String(), str)
	}

	hmsSecs, err := strconv.Atoi(hms[2])
	if len(hms[2]) > 0 && err != nil {
		return sql.Time(0), sql.ErrIncorrectValue.New(t.String(), str)
	} else if hmsSecs > MaxSecond {
		return sql.Time(0), sql.ErrIncorrectValue.New(t.String(), str)
	}
	secs = int64(hmsSecs)
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

func makeTime(isNeg bool, hours, mins, secs, micros int64) (sql.Time, bool) {
	neg := int64(1)
	if isNeg {
		neg = -1
	}
	if hours > 838 {
		return sql.Time(0), false
	}
	if mins > MaxMinute {
		return sql.Time(0), false
	}
	if secs > MaxSecond {
		return sql.Time(0), false
	}
	return sql.Time(neg * (micros +
		(secs * MicrosPerSec) +
		(mins * MicrosPerMin) +
		(hours * MicrosPerHour)),
	), true
}

func (t timeType) timeToUnits(timeVal sql.Time) (isNeg bool, hours, mins, secs, micros int64) {
	isNeg = timeVal < 0
	absTimeVal := int64Abs(int64(timeVal))
	hours = absTimeVal / MicrosPerHour
	mins = (absTimeVal / MicrosPerMin) % MinsPerHour
	secs = (absTimeVal / MicrosPerSec) % SecsPerMin
	micros = absTimeVal % MicrosPerSec // TODO: handle precision here?
	return
}

// Precision implements the sql.TimeType interface.
func (t timeType) Precision() int {
	return t.precision
}

// CollationCoercibility implements sql.CollationCoercible interface.
func (t timeType) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}
