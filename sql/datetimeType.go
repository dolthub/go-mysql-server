package sql

import (
	"math"
	"reflect"
	"time"
	"vitess.io/vitess/go/vt/proto/query"

	"gopkg.in/src-d/go-errors.v1"
	"vitess.io/vitess/go/sqltypes"
)

const (
	// DateLayout is the layout of the MySQL date format in the representation
	// Go understands.
	DateLayout = "2006-01-02"

	// TimestampDatetimeLayout is the formatting string with the layout of the timestamp
	// using the format of Go "time" package.
	TimestampDatetimeLayout = "2006-01-02 15:04:05.999999"

	zeroDateStr = "0000-00-00"
	zeroTimestampDatetimeStr = "0000-00-00 00:00:00"
)

var (
	// ErrConvertingToTime is thrown when a value cannot be converted to a Time
	ErrConvertingToTime = errors.NewKind("value %q can't be converted to time.Time")

	// maxTimestamp is the maximum representable Timestamp value, which is the maximum 32-bit integer as a Unix time.
	maxTimestamp = time.Unix(math.MaxInt32, 999999000)

	// minTimestamp is the minimum representable Timestamp value, which is one second past the epoch.
	minTimestamp = time.Unix(1, 0)

	// TimestampDatetimeLayouts hold extra timestamps allowed for parsing. It does
	// not have all the layouts supported by mysql. Missing are two digit year
	// versions of common cases and dates that use non common separators.
	//
	// https://github.com/MariaDB/server/blob/mysql-5.5.36/sql-common/my_time.c#L124
	TimestampDatetimeLayouts = []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02",
		time.RFC3339,
		"20060102150405",
		"20060102",
	}

	// zeroTime is 0000-01-01 00:00:00 UTC which is the closest Go can get to 0000-00-00 00:00:00
	zeroTime = time.Unix(-62167219200, 0)

	// Date is a date with day, month and year.
	Date = MustCreateDatetimeType(sqltypes.Date)
	// Datetime is a date and a time
	Datetime = MustCreateDatetimeType(sqltypes.Datetime)
	// Timestamp is an UNIX timestamp.
	Timestamp = MustCreateDatetimeType(sqltypes.Timestamp)
)

type datetimeType struct{
	baseType query.Type
}

// CreateDatetimeType creates a Type dealing with all temporal types that are not TIME nor YEAR.
func CreateDatetimeType(baseType query.Type) (Type, error) {
	switch baseType {
	case sqltypes.Date, sqltypes.Datetime, sqltypes.Timestamp:
		return datetimeType{
			baseType: baseType,
		}, nil
	}
	return nil, ErrInvalidBaseType.New(baseType.String(), "datetime")
}

// MustCreateDatetimeType is the same as CreateDatetimeType except it panics on errors.
func MustCreateDatetimeType(baseType query.Type) Type {
	dt, err := CreateDatetimeType(baseType)
	if err != nil {
		panic(err)
	}
	return dt
}

// Compare implements Type interface.
func (t datetimeType) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	var at time.Time
	var bt time.Time
	var ok bool
	if at, ok = a.(time.Time); !ok {
		ai, err := t.Convert(a)
		if err != nil {
			return 0, err
		}
		at = ai.(time.Time)
	}
	if bt, ok = b.(time.Time); !ok {
		bi, err := t.Convert(b)
		if err != nil {
			return 0, err
		}
		bt = bi.(time.Time)
	}

	if at.Before(bt) {
		return -1, nil
	} else if at.After(bt) {
		return 1, nil
	}
	return 0, nil
}

// Convert implements Type interface.
func (t datetimeType) Convert(v interface{}) (interface{}, error) {
	var res time.Time

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
		res = time.Unix(value, 0).UTC()
	case uint64:
		return t.Convert(int64(value))
	case float32:
		return t.Convert(float64(value))
	case float64:
		seconds := int64(value)
		nanoseconds := int64((value - float64(seconds)) * float64(time.Second/time.Nanosecond))
		res = time.Unix(seconds, nanoseconds).UTC()
	case string:
		if value == zeroDateStr || value == zeroTimestampDatetimeStr {
			return zeroTime, nil
		}
		parsed := false
		for _, fmt := range TimestampDatetimeLayouts {
			if t, err := time.Parse(fmt, value); err == nil {
				res = t.UTC()
				parsed = true
				break
			}
		}
		if !parsed {
			return nil, ErrConvertingToTime.New(v)
		}
	case time.Time:
		res = value.UTC()
	default:
		ts, err := Int64.Convert(v)
		if err != nil {
			return nil, ErrInvalidType.New(reflect.TypeOf(v))
		}

		res = time.Unix(ts.(int64), 0).UTC()
	}

	switch t.baseType {
	case sqltypes.Date:
		res = truncateDate(res)
		if res.Year() < 1000 || res.Year() > 9999 {
			return zeroTime, nil
		}
	case sqltypes.Datetime:
		if res.Year() < 1000 || res.Year() > 9999 {
			return zeroTime, nil
		}
	case sqltypes.Timestamp:
		if res.Before(minTimestamp) || res.After(maxTimestamp) {
			return zeroTime, nil
		}
	}
	return res, nil
}

// SQL implements Type interface.
func (t datetimeType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	vt := v.(time.Time)

	switch t.baseType {
	case sqltypes.Date:
		if vt.Equal(zeroTime) {
			return sqltypes.MakeTrusted(
				sqltypes.Date,
				[]byte(vt.Format(zeroDateStr)),
			), nil
		}
		return sqltypes.MakeTrusted(
			sqltypes.Date,
			[]byte(vt.Format(DateLayout)),
		), nil
	case sqltypes.Datetime:
		if vt.Equal(zeroTime) {
			return sqltypes.MakeTrusted(
				sqltypes.Datetime,
				[]byte(vt.Format(zeroTimestampDatetimeStr)),
			), nil
		}
		return sqltypes.MakeTrusted(
			sqltypes.Datetime,
			[]byte(vt.Format(TimestampDatetimeLayout)),
		), nil
	case sqltypes.Timestamp:
		if vt.Equal(zeroTime) {
			return sqltypes.MakeTrusted(
				sqltypes.Timestamp,
				[]byte(vt.Format(zeroTimestampDatetimeStr)),
			), nil
		}
		return sqltypes.MakeTrusted(
			sqltypes.Timestamp,
			[]byte(vt.Format(TimestampDatetimeLayout)),
		), nil
	}
	panic(ErrInvalidBaseType.New(t.baseType.String(), "datetime"))
}

func (t datetimeType) String() string {
	switch t.baseType {
	case sqltypes.Date:
		return "DATE"
	case sqltypes.Datetime:
		return "DATETIME"
	case sqltypes.Timestamp:
		return "TIMESTAMP"
	}
	panic(ErrInvalidBaseType.New(t.baseType.String(), "datetime"))
}

// Type implements Type interface.
func (t datetimeType) Type() query.Type {
	return t.baseType
}

func (t datetimeType) Zero() interface{} {
	return zeroTime
}

func truncateDate(t time.Time) time.Time {
	return t.Truncate(24 * time.Hour)
}

// ValidateTime receives a time and returns either that time or nil if it's
// not a valid time.
func ValidateTime(t time.Time) interface{} {
	if t.After(time.Date(9999, time.December, 31, 23, 59, 59, 999999999, time.UTC)) {
		return nil
	}
	return t
}
