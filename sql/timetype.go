package sql

import (
	"fmt"
	"gopkg.in/src-d/go-errors.v1"
	"regexp"
	"strconv"
	"strings"
	"time"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

var (
	Time timespanType

	ErrConvertingToTimeType = errors.NewKind("value %v is not a valid Time")

	timespanRegex = regexp.MustCompile(`^-?(\d{1,3}):(\d{1,2})(:(\d{1,2})(\.(\d{1,6}))?)?$`)
	timespanMinimum int64 = -3020399000000
	timespanMaximum int64 = 3020399000000
	microsecondsPerSecond int64 = 1000000
	microsecondsPerMinute int64 = 60000000
	microsecondsPerHour int64 = 3600000000
)

// Represents the TIME type.
// https://dev.mysql.com/doc/refman/8.0/en/time.html
type TimeType interface {
	Type
}

type timespanType struct{}
type timespanImpl struct{
	negative bool
	hours int16
	minutes int8
	seconds int8
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
		} else if absValue >= 1000 && absValue <= 9999 {
			minutes := absValue / 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				if value < 0 {
					return microsecondsToTimespan(-1 * (seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute)), nil
				}
				return microsecondsToTimespan((seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute)), nil
			}
		} else if absValue >= 10000 && absValue <= 9999999 {
			hours := value / 10000
			minutes := (absValue / 100) % 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				return microsecondsToTimespan((seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute) + (hours * microsecondsPerHour)), nil
			}
		}
	case uint64:
		return t.ConvertToTimespanImpl(int64(value))
	case float32:
		return t.ConvertToTimespanImpl(float64(value))
	case float64:
		intValue := int64(value)
		microseconds := int64((value - float64(intValue)) * float64(microsecondsPerSecond))
		absValue := int64Abs(intValue)
		if absValue >= -59 && absValue <= 59 {
			return microsecondsToTimespan((intValue * microsecondsPerSecond) + microseconds), nil
		} else if absValue >= 1000 && absValue <= 9999 {
			minutes := absValue / 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				if intValue < 0 {
					return microsecondsToTimespan(-1 * (seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute) + microseconds), nil
				}
				return microsecondsToTimespan((seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute) + microseconds), nil
			}
		} else if absValue >= 10000 && absValue <= 9999999 {
			hours := intValue / 10000
			minutes := (absValue / 100) % 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				return microsecondsToTimespan((seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute) + (hours * microsecondsPerHour) + microseconds), nil
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

// Promote implements the Type interface.
func (t timespanType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t timespanType) SQL(v interface{}) (sqltypes.Value, error) {
	ti, err := t.ConvertToTimespanImpl(v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return sqltypes.MakeTrusted(sqltypes.Time, []byte(ti.String())), nil
}

// String implements Type interface.
func (t timespanType) String() string {
	return "TIME"
}

// Type implements Type interface.
func (t timespanType) Type() query.Type {
	return sqltypes.Time
}

// Zero implements Type interface.
func (t timespanType) Zero() interface{} {
	return "00:00:00"
}

// No built in for absolute values on int64
func int64Abs(v int64) int64 {
	shift := v >> 63
	return (v ^ shift) - shift
}

func stringToTimespan(s string) (timespanImpl, error) {
	matches := timespanRegex.FindStringSubmatch(s)
	if len(matches) == 7 {
		hours, _ := strconv.Atoi(matches[1])
		minutes, _ := strconv.Atoi(matches[2])
		if minutes > 59 {
			return timespanImpl{}, ErrConvertingToTimeType.New(s)
		}
		seconds, _ := strconv.Atoi(matches[4])
		if seconds > 59 {
			return timespanImpl{}, ErrConvertingToTimeType.New(s)
		}
		microseconds, _ := strconv.Atoi(matches[6])
		for i := 10; i < int(microsecondsPerSecond); i *= 10 {
			if microseconds < i {
				microseconds *= 10
			}
		}
		if hours > 838 {
			hours = 838
			minutes = 59
			seconds = 59
		}
		if hours == 838 && minutes == 59 && seconds == 59 {
			microseconds = 0
		}
		impl := timespanImpl{
			hours: int16(hours),
			minutes: int8(minutes),
			seconds: int8(seconds),
			microseconds: int32(microseconds),
		}
		if s[0] == '-' {
			impl.negative = true
		}
		return impl, nil
	}
	return timespanImpl{}, ErrConvertingToTimeType.New(s)
}

func microsecondsToTimespan(v int64) timespanImpl {
	if v < timespanMinimum {
		v = timespanMinimum
	} else if v > timespanMaximum {
		v = timespanMaximum
	}

	absV := int64Abs(v)

	return timespanImpl{
		negative: v < 0,
		hours: int16(absV / microsecondsPerHour),
		minutes: int8((absV / microsecondsPerMinute) % 60),
		seconds: int8((absV / microsecondsPerSecond) % 60),
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
