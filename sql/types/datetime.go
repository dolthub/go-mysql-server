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
	"math"
	"reflect"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

const zeroDateStr = "0000-00-00"

const zeroTimestampDatetimeStr = "0000-00-00 00:00:00"

var (
	// ErrConvertingToTime is thrown when a value cannot be converted to a Time
	ErrConvertingToTime = errors.NewKind("Incorrect datetime value: '%s'")

	ErrConvertingToTimeOutOfRange = errors.NewKind("value %q is outside of %v range")

	// datetimeTypeMaxDatetime is the maximum representable Datetime/Date value.
	datetimeTypeMaxDatetime = time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)

	// datetimeTypeMinDatetime is the minimum representable Datetime/Date value.
	datetimeTypeMinDatetime = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)

	// datetimeTypeMaxTimestamp is the maximum representable Timestamp value, which is the maximum 32-bit integer as a Unix time.
	datetimeTypeMaxTimestamp = time.Unix(math.MaxInt32, 999999000)

	// datetimeTypeMinTimestamp is the minimum representable Timestamp value, which is one second past the epoch.
	datetimeTypeMinTimestamp = time.Unix(1, 0)

	// TimestampDatetimeLayouts hold extra timestamps allowed for parsing. It does
	// not have all the layouts supported by mysql. Missing are two digit year
	// versions of common cases and dates that use non common separators.
	//
	// https://github.com/MariaDB/server/blob/mysql-5.5.36/sql-common/my_time.c#L124
	TimestampDatetimeLayouts = []string{
		"2006-01-02 15:4",
		"2006-01-02 15:04",
		"2006-01-02 15:04:",
		"2006-01-02 15:04:.",
		"2006-01-02 15:04:05.",
		"2006-01-02 15:04:05.999999",
		"2006-01-02",
		"2006-1-2",
		"2006-1-2 15:4:5.999999",
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05",
		"20060102150405",
		"20060102",
		"2006/01/02",
		"2006-01-02 15:04:05.999999999 -0700 MST", // represents standard Time.time.UTC()
	}

	// zeroTime is 0000-01-01 00:00:00 UTC which is the closest Go can get to 0000-00-00 00:00:00
	zeroTime = time.Unix(-62167219200, 0).UTC()

	// Date is a date with day, month and year.
	Date = MustCreateDatetimeType(sqltypes.Date)
	// Datetime is a date and a time
	Datetime = MustCreateDatetimeType(sqltypes.Datetime)
	// Timestamp is an UNIX timestamp.
	Timestamp = MustCreateDatetimeType(sqltypes.Timestamp)

	datetimeValueType = reflect.TypeOf(time.Time{})
)

type datetimeType struct {
	baseType query.Type
}

// CreateDatetimeType creates a Type dealing with all temporal types that are not TIME nor YEAR.
func CreateDatetimeType(baseType query.Type) (sql.DatetimeType, error) {
	switch baseType {
	case sqltypes.Date, sqltypes.Datetime, sqltypes.Timestamp:
		return datetimeType{
			baseType: baseType,
		}, nil
	}
	return nil, sql.ErrInvalidBaseType.New(baseType.String(), "datetime")
}

// MustCreateDatetimeType is the same as CreateDatetimeType except it panics on errors.
func MustCreateDatetimeType(baseType query.Type) sql.DatetimeType {
	dt, err := CreateDatetimeType(baseType)
	if err != nil {
		panic(err)
	}
	return dt
}

// Compare implements Type interface.
func (t datetimeType) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := CompareNulls(a, b); hasNulls {
		return res, nil
	}

	var at time.Time
	var bt time.Time
	var ok bool
	var err error
	if at, ok = a.(time.Time); !ok {
		at, err = ConvertToTime(a, t)
		if err != nil {
			return 0, err
		}
	} else if t.baseType == sqltypes.Date {
		at = at.Truncate(24 * time.Hour)
	}
	if bt, ok = b.(time.Time); !ok {
		bt, err = ConvertToTime(b, t)
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

// Convert implements Type interface.
func (t datetimeType) Convert(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	res, err := ConvertToTime(v, t)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func ConvertToTime(v interface{}, t datetimeType) (time.Time, error) {
	if v == nil {
		return time.Time{}, nil
	}

	res, err := t.ConvertWithoutRangeCheck(v)
	if err != nil {
		return time.Time{}, err
	}

	if res.Equal(zeroTime) {
		return zeroTime, nil
	}

	switch t.baseType {
	case sqltypes.Date:
		if res.Year() < 0 || res.Year() > 9999 {
			return time.Time{}, ErrConvertingToTimeOutOfRange.New(res.Format(sql.DateLayout), t.String())
		}
	case sqltypes.Datetime:
		if res.Year() < 0 || res.Year() > 9999 {
			return time.Time{}, ErrConvertingToTimeOutOfRange.New(res.Format(sql.TimestampDatetimeLayout), t.String())
		}
	case sqltypes.Timestamp:
		if res.Before(datetimeTypeMinTimestamp) || res.After(datetimeTypeMaxTimestamp) {
			return time.Time{}, ErrConvertingToTimeOutOfRange.New(res.Format(sql.TimestampDatetimeLayout), t.String())
		}
	}
	return res, nil
}

// ConvertWithoutRangeCheck converts the parameter to time.Time without checking the range.
func (t datetimeType) ConvertWithoutRangeCheck(v interface{}) (time.Time, error) {
	var res time.Time

	if bs, ok := v.([]byte); ok {
		v = string(bs)
	}
	switch value := v.(type) {
	case string:
		if value == zeroDateStr || value == zeroTimestampDatetimeStr {
			return zeroTime, nil
		}
		// TODO: consider not using time.Parse if we want to match MySQL exactly ('2010-06-03 11:22.:.:.:.:' is a valid timestamp)
		parsed := false
		for _, fmt := range TimestampDatetimeLayouts {
			if t, err := time.Parse(fmt, value); err == nil {
				res = t.UTC()
				parsed = true
				break
			}
		}
		if !parsed {
			return zeroTime, ErrConvertingToTime.New(v)
		}
	case time.Time:
		res = value.UTC()
		// For most integer values, we just return an error (but MySQL is more lenient for some of these). A special case
		// is zero values, which are important when converting from postgres defaults.
	case int:
		if value == 0 {
			return zeroTime, nil
		}
		return zeroTime, ErrConvertingToTime.New(v)
	case int8:
		if value == 0 {
			return zeroTime, nil
		}
		return zeroTime, ErrConvertingToTime.New(v)
	case int16:
		if value == 0 {
			return zeroTime, nil
		}
		return zeroTime, ErrConvertingToTime.New(v)
	case int32:
		if value == 0 {
			return zeroTime, nil
		}
		return zeroTime, ErrConvertingToTime.New(v)
	case int64:
		if value == 0 {
			return zeroTime, nil
		}
		return zeroTime, ErrConvertingToTime.New(v)
	case uint:
		if value == 0 {
			return zeroTime, nil
		}
		return zeroTime, ErrConvertingToTime.New(v)
	case uint8:
		if value == 0 {
			return zeroTime, nil
		}
		return zeroTime, ErrConvertingToTime.New(v)
	case uint16:
		if value == 0 {
			return zeroTime, nil
		}
		return zeroTime, ErrConvertingToTime.New(v)
	case uint32:
		if value == 0 {
			return zeroTime, nil
		}
		return zeroTime, ErrConvertingToTime.New(v)
	case uint64:
		if value == 0 {
			return zeroTime, nil
		}
		return zeroTime, ErrConvertingToTime.New(v)
	case float32:
		if value == 0 {
			return zeroTime, nil
		}
		return zeroTime, ErrConvertingToTime.New(v)
	case float64:
		if value == 0 {
			return zeroTime, nil
		}
		return zeroTime, ErrConvertingToTime.New(v)
	case decimal.Decimal:
		if value.IsZero() {
			return zeroTime, nil
		}
		return zeroTime, ErrConvertingToTime.New(v)
	case decimal.NullDecimal:
		if value.Valid && value.Decimal.IsZero() {
			return zeroTime, nil
		}
		return zeroTime, ErrConvertingToTime.New(v)
	case Timespan:
		// when receiving TIME, MySQL fills in date with today
		nowTimeStr := sql.Now().Format("2006-01-02")
		nowTime, err := time.Parse("2006-01-02", nowTimeStr)
		if err != nil {
			return zeroTime, ErrConvertingToTime.New(v)
		}
		return nowTime.Add(value.AsTimeDuration()), nil
	default:
		return zeroTime, sql.ErrConvertToSQL.New(t)
	}

	if t.baseType == sqltypes.Date {
		res = res.Truncate(24 * time.Hour)
	}

	return res, nil
}

func (t datetimeType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Equals implements the Type interface.
func (t datetimeType) Equals(otherType sql.Type) bool {
	return t.baseType == otherType.Type()
}

// MaxTextResponseByteLength implements the Type interface
func (t datetimeType) MaxTextResponseByteLength() uint32 {
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
	return Datetime
}

// SQL implements Type interface.
func (t datetimeType) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	vt := v.(time.Time)

	var typ query.Type
	var val string

	switch t.baseType {
	case sqltypes.Date:
		typ = sqltypes.Date
		if vt.Equal(zeroTime) {
			val = vt.Format(zeroDateStr)
		} else {
			val = vt.Format(sql.DateLayout)
		}
	case sqltypes.Datetime:
		typ = sqltypes.Datetime
		if vt.Equal(zeroTime) {
			val = vt.Format(zeroTimestampDatetimeStr)
		} else {
			val = vt.Format(sql.TimestampDatetimeLayout)
		}
	case sqltypes.Timestamp:
		typ = sqltypes.Timestamp
		if vt.Equal(zeroTime) {
			val = vt.Format(zeroTimestampDatetimeStr)
		} else {
			val = vt.Format(sql.TimestampDatetimeLayout)
		}
	default:
		panic(sql.ErrInvalidBaseType.New(t.baseType.String(), "datetime"))
	}

	valBytes := AppendAndSliceString(dest, val)

	return sqltypes.MakeTrusted(typ, valBytes), nil
}

func (t datetimeType) String() string {
	switch t.baseType {
	case sqltypes.Date:
		return "date"
	case sqltypes.Datetime:
		return "datetime"
	case sqltypes.Timestamp:
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
	return zeroTime
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
	if t.After(time.Date(9999, time.December, 31, 23, 59, 59, 999999999, time.UTC)) {
		return nil
	}
	return t
}
