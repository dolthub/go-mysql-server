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

package sql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"
)

var (
	Time TimeType = timespanType{}

	ErrConvertingToTimeType = errors.NewKind("value %v is not a valid Time")

	timespanMinimum           int64 = -3020399000000
	timespanMaximum           int64 = 3020399000000
	microsecondsPerSecond     int64 = 1000000
	microsecondsPerMinute     int64 = 60000000
	microsecondsPerHour       int64 = 3600000000
	nanosecondsPerMicrosecond int64 = 1000
)

// Represents the TIME type.
// https://dev.mysql.com/doc/refman/8.0/en/time.html
// TIME is implemented as TIME(6)
// TODO: implement parameters on the TIME type
type TimeType interface {
	Type
	ConvertToTimeDuration(v interface{}) (time.Duration, error)
	//TODO: move this out of go-mysql-server and into the Dolt layer
	Marshal(v interface{}) (int64, error)
	Unmarshal(v int64) string
}

type timespanType struct{}
type timespanImpl struct {
	negative     bool
	hours        int16
	minutes      int8
	seconds      int8
	microseconds int32
}

// Compare implements Type interface.
func (t timespanType) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	as, err := t.ConvertToTimespanImpl(a)
	if err != nil {
		return 0, err
	}
	bs, err := t.ConvertToTimespanImpl(b)
	if err != nil {
		return 0, err
	}

	ai := as.AsMicroseconds()
	bi := bs.AsMicroseconds()

	if ai < bi {
		return -1, nil
	} else if ai > bi {
		return 1, nil
	}
	return 0, nil
}

func (t timespanType) Convert(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	if ti, err := t.ConvertToTimespanImpl(v); err != nil {
		return nil, err
	} else {
		return ti.String(), nil
	}
}

// MustConvert implements the Type interface.
func (t timespanType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Convert implements Type interface.
func (t timespanType) ConvertToTimespanImpl(v interface{}) (timespanImpl, error) {
	switch value := v.(type) {
	case int:
		return t.ConvertToTimespanImpl(int64(value))
	case uint:
		return t.ConvertToTimespanImpl(int64(value))
	case int8:
		return t.ConvertToTimespanImpl(int64(value))
	case uint8:
		return t.ConvertToTimespanImpl(int64(value))
	case int16:
		return t.ConvertToTimespanImpl(int64(value))
	case uint16:
		return t.ConvertToTimespanImpl(int64(value))
	case int32:
		return t.ConvertToTimespanImpl(int64(value))
	case uint32:
		return t.ConvertToTimespanImpl(int64(value))
	case int64:
		absValue := int64Abs(value)
		if absValue >= -59 && absValue <= 59 {
			return microsecondsToTimespan(value * microsecondsPerSecond), nil
		} else if absValue >= 100 && absValue <= 9999 {
			minutes := absValue / 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				microseconds := (seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute)
				if value < 0 {
					return microsecondsToTimespan(-1 * microseconds), nil
				}
				return microsecondsToTimespan(microseconds), nil
			}
		} else if absValue >= 10000 && absValue <= 9999999 {
			hours := absValue / 10000
			minutes := (absValue / 100) % 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				microseconds := (seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute) + (hours * microsecondsPerHour)
				if value < 0 {
					return microsecondsToTimespan(-1 * microseconds), nil
				}
				return microsecondsToTimespan(microseconds), nil
			}
		}
	case uint64:
		return t.ConvertToTimespanImpl(int64(value))
	case float32:
		return t.ConvertToTimespanImpl(float64(value))
	case float64:
		intValue := int64(value)
		microseconds := int64Abs(int64(math.Round((value - float64(intValue)) * float64(microsecondsPerSecond))))
		absValue := int64Abs(intValue)
		if absValue >= -59 && absValue <= 59 {
			totalMicroseconds := (absValue * microsecondsPerSecond) + microseconds
			if value < 0 {
				return microsecondsToTimespan(-1 * totalMicroseconds), nil
			}
			return microsecondsToTimespan(totalMicroseconds), nil
		} else if absValue >= 100 && absValue <= 9999 {
			minutes := absValue / 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				totalMicroseconds := (seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute) + microseconds
				if value < 0 {
					return microsecondsToTimespan(-1 * totalMicroseconds), nil
				}
				return microsecondsToTimespan(totalMicroseconds), nil
			}
		} else if absValue >= 10000 && absValue <= 9999999 {
			hours := absValue / 10000
			minutes := (absValue / 100) % 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				totalMicroseconds := (seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute) + (hours * microsecondsPerHour) + microseconds
				if value < 0 {
					return microsecondsToTimespan(-1 * totalMicroseconds), nil
				}
				return microsecondsToTimespan(totalMicroseconds), nil
			}
		}
	case string:
		impl, err := stringToTimespan(value)
		if err == nil {
			return impl, nil
		}
		if strings.Contains(value, ".") {
			strAsDouble, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return timespanImpl{}, ErrConvertingToTimeType.New(v)
			}
			return t.ConvertToTimespanImpl(strAsDouble)
		} else {
			strAsInt, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return timespanImpl{}, ErrConvertingToTimeType.New(v)
			}
			return t.ConvertToTimespanImpl(strAsInt)
		}
	case time.Duration:
		microseconds := value.Nanoseconds() / 1000
		return microsecondsToTimespan(microseconds), nil
	}

	return timespanImpl{}, ErrConvertingToTimeType.New(v)
}

func (t timespanType) ConvertToTimeDuration(v interface{}) (time.Duration, error) {
	val, err := t.ConvertToTimespanImpl(v)
	if err != nil {
		return time.Duration(0), err
	}
	return val.AsTimeDuration(), nil
}

// Equals implements the Type interface.
func (t timespanType) Equals(otherType Type) bool {
	_, ok := otherType.(timespanType)
	return ok
}

// Promote implements the Type interface.
func (t timespanType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t timespanType) SQL(dest []byte, v interface{}) (sqltypes.Value, error) {
	ti, err := t.ConvertToTimespanImpl(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	val := appendAndSlice(dest, []byte(ti.String()))

	return sqltypes.MakeTrusted(sqltypes.Time, val), nil
}

// String implements Type interface.
func (t timespanType) String() string {
	return "TIME(6)"
}

// Type implements Type interface.
func (t timespanType) Type() query.Type {
	return sqltypes.Time
}

// Zero implements Type interface.
func (t timespanType) Zero() interface{} {
	return "00:00:00"
}

// Marshal takes a valid Time value and returns it as an int64.
func (t timespanType) Marshal(v interface{}) (int64, error) {
	if ti, err := t.ConvertToTimespanImpl(v); err != nil {
		return 0, err
	} else {
		return ti.AsMicroseconds(), nil
	}
}

// Unmarshal takes a previously-marshalled value and returns it as a string.
func (t timespanType) Unmarshal(v int64) string {
	return microsecondsToTimespan(v).String()
}

// No built in for absolute values on int64
func int64Abs(v int64) int64 {
	shift := v >> 63
	return (v ^ shift) - shift
}

func stringToTimespan(s string) (timespanImpl, error) {
	impl := timespanImpl{}
	if len(s) > 0 && s[0] == '-' {
		impl.negative = true
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
		microseconds, err := strconv.Atoi(microStr)
		if err != nil {
			return timespanImpl{}, ErrConvertingToTimeType.New(s)
		}
		// MySQL just uses the last digit to round up. This is weird, but matches their implementation.
		if len(remainStr) > 0 && remainStr[len(remainStr)-1:] >= "5" {
			microseconds++
		}
		impl.microseconds = int32(microseconds)
	}

	// Parse H-M-S time
	hmsComps := strings.SplitN(comps[0], ":", 3)
	hms := make([]string, 3)
	if len(hmsComps) >= 2 {
		if len(hmsComps[0]) > 3 {
			return timespanImpl{}, ErrConvertingToTimeType.New(s)
		}
		hms[0] = hmsComps[0]
		if len(hmsComps[1]) > 2 {
			return timespanImpl{}, ErrConvertingToTimeType.New(s)
		}
		hms[1] = hmsComps[1]
		if len(hmsComps) == 3 {
			if len(hmsComps[2]) > 2 {
				return timespanImpl{}, ErrConvertingToTimeType.New(s)
			}
			hms[2] = hmsComps[2]
		}
	} else {
		l := len(hmsComps[0])
		hms[2] = safeSubstr(hmsComps[0], l-2, l)
		hms[1] = safeSubstr(hmsComps[0], l-4, l-2)
		hms[0] = safeSubstr(hmsComps[0], l-7, l-4)
	}

	hours, err := strconv.Atoi(hms[0])
	if len(hms[0]) > 0 && err != nil {
		return timespanImpl{}, ErrConvertingToTimeType.New(s)
	}
	impl.hours = int16(hours)

	minutes, err := strconv.Atoi(hms[1])
	if len(hms[1]) > 0 && err != nil {
		return timespanImpl{}, ErrConvertingToTimeType.New(s)
	} else if minutes >= 60 {
		return timespanImpl{}, ErrConvertingToTimeType.New(s)
	}
	impl.minutes = int8(minutes)

	seconds, err := strconv.Atoi(hms[2])
	if len(hms[2]) > 0 && err != nil {
		return timespanImpl{}, ErrConvertingToTimeType.New(s)
	} else if seconds >= 60 {
		return timespanImpl{}, ErrConvertingToTimeType.New(s)
	}
	impl.seconds = int8(seconds)

	if impl.microseconds == int32(microsecondsPerSecond) {
		impl.microseconds = 0
		impl.seconds++
	}
	if impl.seconds == 60 {
		impl.seconds = 0
		impl.minutes++
	}
	if impl.minutes == 60 {
		impl.minutes = 0
		impl.hours++
	}

	if impl.hours > 838 {
		impl.hours = 838
		impl.minutes = 59
		impl.seconds = 59
	}

	if impl.hours == 838 && impl.minutes == 59 && impl.seconds == 59 {
		impl.microseconds = 0
	}

	return impl, nil
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

func microsecondsToTimespan(v int64) timespanImpl {
	if v < timespanMinimum {
		v = timespanMinimum
	} else if v > timespanMaximum {
		v = timespanMaximum
	}

	absV := int64Abs(v)

	return timespanImpl{
		negative:     v < 0,
		hours:        int16(absV / microsecondsPerHour),
		minutes:      int8((absV / microsecondsPerMinute) % 60),
		seconds:      int8((absV / microsecondsPerSecond) % 60),
		microseconds: int32(absV % microsecondsPerSecond),
	}
}

func (t timespanImpl) String() string {
	sign := ""
	if t.negative {
		sign = "-"
	}
	if t.microseconds == 0 {
		return fmt.Sprintf("%v%02d:%02d:%02d", sign, t.hours, t.minutes, t.seconds)
	}
	return fmt.Sprintf("%v%02d:%02d:%02d.%06d", sign, t.hours, t.minutes, t.seconds, t.microseconds)
}

func (t timespanImpl) AsMicroseconds() int64 {
	negative := int64(1)
	if t.negative {
		negative = -1
	}
	return negative * (int64(t.microseconds) +
		(int64(t.seconds) * microsecondsPerSecond) +
		(int64(t.minutes) * microsecondsPerMinute) +
		(int64(t.hours) * microsecondsPerHour))
}

func (t timespanImpl) AsTimeDuration() time.Duration {
	return time.Duration(t.AsMicroseconds() * nanosecondsPerMicrosecond)
}
