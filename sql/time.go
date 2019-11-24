package sql

import (
	"fmt"
	"gopkg.in/src-d/go-errors.v1"
	"regexp"
	"strconv"
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

type timespanType struct{}

// Compare implements Type interface.
func (t timespanType) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	as, err := t.Convert(a)
	if err != nil {
		return 0, err
	}
	bs, err := t.Convert(b)
	if err != nil {
		return 0, err
	}

	ai, _ := timespanStrToMicroseconds(as.(string))
	bi, _ := timespanStrToMicroseconds(bs.(string))

	if ai < bi {
		return -1, nil
	} else if ai > bi {
		return 1, nil
	}
	return 0, nil
}

// Convert implements Type interface.
func (t timespanType) Convert(v interface{}) (interface{}, error) {
	switch value := v.(type) {
	case int:
		return t.Convert(int64(value))
	case uint:
		return t.Convert(int64(value))
	case int8:
		return t.Convert(int64(value))
	case uint8:
		return t.Convert(int64(value))
	case int16:
		return t.Convert(int64(value))
	case uint16:
		return t.Convert(int64(value))
	case int32:
		return t.Convert(int64(value))
	case uint32:
		return t.Convert(int64(value))
	case int64:
		absValue := int64Abs(value)
		if absValue >= -59 && absValue <= 59 {
			return microsecondsToTimespanStr(value * microsecondsPerSecond), nil
		} else if absValue >= 1000 && absValue <= 9999 {
			minutes := absValue / 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				if value < 0 {
					return microsecondsToTimespanStr(-1 * (seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute)), nil
				}
				return microsecondsToTimespanStr((seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute)), nil
			}
		} else if absValue >= 100000 && absValue <= 9999999 {
			hours := value / 1000
			minutes := (absValue / 100) % 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				return microsecondsToTimespanStr((seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute) + (hours * microsecondsPerHour)), nil
			}
		}
	case uint64:
		return t.Convert(int64(value))
	case float32:
		return t.Convert(float64(value))
	case float64:
		intValue := int64(value)
		microseconds := int64((value - float64(intValue)) * float64(microsecondsPerSecond))
		absValue := int64Abs(intValue)
		if absValue >= -59 && absValue <= 59 {
			return microsecondsToTimespanStr((intValue * microsecondsPerSecond) + microseconds), nil
		} else if absValue >= 1000 && absValue <= 9999 {
			minutes := absValue / 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				if intValue < 0 {
					return microsecondsToTimespanStr(-1 * (seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute) + microseconds), nil
				}
				return microsecondsToTimespanStr((seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute) + microseconds), nil
			}
		} else if absValue >= 100000 && absValue <= 9999999 {
			hours := intValue / 1000
			minutes := (absValue / 100) % 100
			seconds := absValue % 100
			if minutes <= 59 && seconds <= 59 {
				return microsecondsToTimespanStr((seconds * microsecondsPerSecond) + (minutes * microsecondsPerMinute) + (hours * microsecondsPerHour) + microseconds), nil
			}
		}
	case string:
		// We use this as both a check, and also to format the original string
		microseconds, err := timespanStrToMicroseconds(value)
		if err == nil {
			return microsecondsToTimespanStr(microseconds), nil
		}
		strAsInt, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, ErrConvertingToTimeType.New(v)
		}
		return t.Convert(strAsInt)
	case time.Duration:
		microseconds := value.Nanoseconds() / 1000
		return microsecondsToTimespanStr(microseconds), nil
	}

	return nil, ErrConvertingToTimeType.New(v)
}

// SQL implements Type interface.
func (t timespanType) SQL(v interface{}) (sqltypes.Value, error) {
	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return sqltypes.MakeTrusted(sqltypes.Time, []byte(v.(string))), nil
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

func timespanStrToMicroseconds(s string) (int64, error) {
	matches := timespanRegex.FindStringSubmatch(s)
	if len(matches) == 7 {
		hours, _ := strconv.Atoi(matches[1])
		minutes, _ := strconv.Atoi(matches[2])
		if minutes > 59 {
			return 0, ErrConvertingToTimeType.New(s)
		}
		seconds, _ := strconv.Atoi(matches[4])
		if seconds > 59 {
			return 0, ErrConvertingToTimeType.New(s)
		}
		microseconds, _ := strconv.Atoi(matches[6])
		for i := 10; i < int(microsecondsPerSecond); i *= 10 {
			if microseconds < i {
				microseconds *= 10
			}
		}
		value := int64(microseconds) + (int64(seconds) * microsecondsPerSecond) + (int64(minutes) * microsecondsPerMinute) + (int64(hours) * microsecondsPerHour)
		if s[0] == '-' {
			value = -value
		}

		if value < timespanMinimum {
			return timespanMinimum, nil
		} else if value > timespanMaximum {
			return timespanMaximum, nil
		}
		return value, nil
	}
	return 0, ErrConvertingToTimeType.New(s)
}

// No built in for absolute values on int64
func int64Abs(v int64) int64 {
	shift := v >> 63
	return (v ^ shift) - shift
}

func microsecondsToTimespanStr(v int64) string {
	if v < timespanMinimum {
		v = timespanMinimum
	} else if v > timespanMaximum {
		v = timespanMaximum
	}

	absV := int64Abs(v)

	microseconds := absV % microsecondsPerSecond
	seconds := (absV / microsecondsPerSecond) % 60
	minutes := (absV / microsecondsPerMinute) % 60
	hours := v / microsecondsPerHour

	if microseconds == 0 {
		return fmt.Sprintf("%d:%02d:%02d", hours, minutes, seconds)
	}
	return fmt.Sprintf("%d:%02d:%02d.%06d", hours, minutes, seconds, microseconds)
}
