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

package function

import (
	"fmt"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ErrTimeUnexpectedlyNil is thrown when a function encounters and unexpectedly nil time
var ErrTimeUnexpectedlyNil = errors.NewKind("time in function '%s' unexpectedly nil")

// ErrUnknownType is thrown when a function encounters and unknown type
var ErrUnknownType = errors.NewKind("function '%s' encountered unknown type %T")

// ErrTooHighPrecision is thrown when a function is given a fractional seconds precision that is larger than the maximum supported
var ErrTooHighPrecision = errors.NewKind("Too-big precision %d for '%s'. Maximum is %d.")

// getDate converts |val| to a datetime value, returning a nil value and warning the session if it cannot be converted.
func getDate(ctx *sql.Context, val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}

	date, _, err := types.DatetimeMaxPrecision.Convert(ctx, val)
	if err != nil {
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
		return nil, nil
	}

	return date, nil
}

// getDatePart evaluates the child expression of |u| as a date and returns the part of it extracted by |f|.
func getDatePart(ctx *sql.Context,
	u expression.UnaryExpressionStub,
	row sql.Row,
	f func(interface{}) interface{}) (interface{}, error) {
	val, err := u.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	date, err := getDate(ctx, val)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}

	part := f(date)
	if part == nil {
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", sql.ErrTruncatedIncorrect.New("datetime", val).Error())
	}
	return part, nil
}

// datePartFunc converts |fn| into a function that extracts a part of a date value, passing nil values through untouched.
func datePartFunc(fn func(time.Time) interface{}) func(interface{}) interface{} {
	return func(v interface{}) interface{} {
		if v == nil {
			return nil
		}

		return fn(v.(time.Time))
	}
}

var (
	year = datePartFunc(func(t time.Time) interface{} {
		if t.Equal(types.ZeroTime) {
			return 0
		}
		return t.Year()
	})
	month = datePartFunc(func(t time.Time) interface{} {
		if t.Equal(types.ZeroTime) {
			return 0
		}
		return int(t.Month())
	})
	day = datePartFunc(func(t time.Time) interface{} {
		if t.Equal(types.ZeroTime) {
			return 0
		}
		return t.Day()
	})
	weekday = datePartFunc(func(t time.Time) interface{} {
		if t.Equal(types.ZeroTime) {
			return nil
		}
		return (int(t.Weekday()) + 6) % 7
	})
	hour      = datePartFunc(func(t time.Time) interface{} { return t.Hour() })
	minute    = datePartFunc(func(t time.Time) interface{} { return t.Minute() })
	second    = datePartFunc(func(t time.Time) interface{} { return t.Second() })
	dayOfWeek = datePartFunc(func(t time.Time) interface{} {
		if t.Equal(types.ZeroTime) {
			return nil
		}
		return int(t.Weekday()) + 1
	})
	dayOfYear = datePartFunc(func(t time.Time) interface{} {
		if t.Equal(types.ZeroTime) {
			return nil
		}
		return t.YearDay()
	})
	quarter = datePartFunc(func(t time.Time) interface{} {
		if t.Equal(types.ZeroTime) {
			return 0
		}
		return (int(t.Month())-1)/3 + 1
	})
	microsecond = datePartFunc(func(t time.Time) interface{} {
		return uint64(t.Nanosecond()) / uint64(time.Microsecond)
	})
)

// SessionTimeZone returns a MySQL timezone offset string for the value of @@session_time_zone. If the session
// timezone is set to SYSTEM, then the system timezone offset is calculated and returned.
func SessionTimeZone(ctx *sql.Context) (string, error) {
	sessionTimeZoneVar, err := ctx.GetSessionVariable(ctx, "time_zone")
	if err != nil {
		return "", err
	}

	sessionTimeZone, ok := sessionTimeZoneVar.(string)
	if !ok {
		return "", fmt.Errorf("invalid type for @@session.time_zone: %T", sessionTimeZoneVar)
	}

	if sessionTimeZone == "SYSTEM" {
		sessionTimeZone = sql.SystemTimezoneOffset()
	}
	return sessionTimeZone, nil
}
