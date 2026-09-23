// Copyright 2026 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Week is a function that returns year and week for a date.
// The year in the result may be different from the year in the date argument for the first and the last week of the year.
// Details: https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_yearweek
type Week struct {
	date sql.Expression
	mode sql.Expression
}

var _ sql.FunctionExpression = (*Week)(nil)
var _ sql.CollationCoercible = (*Week)(nil)

// NewWeek creates a new Week UDF
func NewWeek(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("YEARWEEK", "1 or more", 0)
	}

	w := &Week{date: args[0]}
	if len(args) > 1 && args[1].Resolved() && types.IsInteger(args[1].Type(ctx)) {
		w.mode = args[1]
	} else {
		w.mode = expression.NewLiteral(0, types.Int64)
	}

	return w, nil
}

// Name implements sql.FunctionExpression
func (d *Week) Name() string {
	return "week"
}

// Description implements sql.FunctionExpression
func (d *Week) Description() string {
	return "returns the week number."
}

// String implements the sql.Expression interface.
func (d *Week) String() string { return fmt.Sprintf("WEEK(%s, %s)", d.date, d.mode.String()) }

// DebugString implements the sql.DebugStringer interface.
func (d *Week) DebugString(ctx *sql.Context) string {
	return fmt.Sprintf("WEEK(%s, %s)", sql.DebugString(ctx, d.date), sql.DebugString(ctx, d.mode))
}

// Type implements the Expression interface.
func (d *Week) Type(ctx *sql.Context) sql.Type { return types.Int32 }

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Week) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the Expression interface.
func (d *Week) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	dateVal, err := d.date.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	date, err := getDate(ctx, dateVal)
	if err != nil {
		return nil, err
	}
	if date == nil {
		return nil, nil
	}

	dateTime, ok := date.(time.Time)
	if !ok || dateTime.Equal(types.ZeroTime) {
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", sql.ErrIncorrectValue.New(dateVal).Error())
		return nil, nil
	}

	yyyy, ok := year(date).(int)
	if !ok {
		return nil, sql.ErrInvalidArgumentDetails.New("WEEK", "invalid year")
	}
	mm, ok := month(date).(int)
	if !ok {
		return nil, sql.ErrInvalidArgumentDetails.New("WEEK", "invalid month")
	}
	dd, ok := day(date).(int)
	if !ok {
		return nil, sql.ErrInvalidArgumentDetails.New("WEEK", "invalid day")
	}

	mode := int64(0)
	val, err := d.mode.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if val != nil {
		if i64, _, err := types.Int64.Convert(ctx, val); err == nil {
			if mode, ok = i64.(int64); ok {
				mode %= 8 // mode in [0, 7]
			}
		}
	}

	yr := int32(yyyy)
	yearForWeek, week := calcWeek(yr, int32(mm), int32(dd), weekMode(mode)|weekBehaviourYear)

	if yearForWeek < yr {
		week = 0
	} else if yearForWeek > yr {
		week = 53
	}

	return week, nil
}

// Resolved implements the Expression interface.
func (d *Week) Resolved() bool {
	return d.date.Resolved() && d.mode.Resolved()
}

// Children implements the Expression interface.
func (d *Week) Children() []sql.Expression { return []sql.Expression{d.date, d.mode} }

// IsNullable implements the Expression interface.
func (d *Week) IsNullable(ctx *sql.Context) bool {
	return true
}

// WithChildren implements the Expression interface.
func (*Week) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewWeek(ctx, children...)
}

// Following solution of YearWeek was taken from tidb: https://github.com/pingcap/tidb/blob/master/types/mytime.go
type weekBehaviour int64

const (
	// weekBehaviourMondayFirst set Monday as first day of week; otherwise Sunday is first day of week
	weekBehaviourMondayFirst weekBehaviour = 1 << iota
	// If set, Week is in range 1-53, otherwise Week is in range 0-53.
	// Note that this flag is only relevant if WEEK_JANUARY is not set.
	weekBehaviourYear
	// If not set, Weeks are numbered according to ISO 8601:1988.
	// If set, the week that contains the first 'first-day-of-week' is week 1.
	weekBehaviourFirstWeekday
)

// test returns whether |flag| is set in |v|.
func (v weekBehaviour) test(flag weekBehaviour) bool {
	return (v & flag) != 0
}

// weekMode returns the weekBehaviour flags enabled by the given MySQL week mode.
func weekMode(mode int64) weekBehaviour {
	weekFormat := weekBehaviour(mode & 7)
	if (weekFormat & weekBehaviourMondayFirst) == 0 {
		weekFormat ^= weekBehaviourFirstWeekday
	}
	return weekFormat
}

// calcWeekday calculates weekday from daynr, returns 0 for Monday, 1 for Tuesday ...
func calcWeekday(daynr int32, sundayFirstDayOfWeek bool) int32 {
	daynr += 5
	if sundayFirstDayOfWeek {
		daynr++
	}
	return daynr % 7
}

// calcWeek calculates week and year for the time.
func calcWeek(yyyy, mm, dd int32, wb weekBehaviour) (int32, int32) {
	daynr := calcDaynr(yyyy, mm, dd)
	firstDaynr := calcDaynr(yyyy, 1, 1)
	mondayFirst := wb.test(weekBehaviourMondayFirst)
	weekYear := wb.test(weekBehaviourYear)
	firstWeekday := wb.test(weekBehaviourFirstWeekday)
	weekday := calcWeekday(firstDaynr, !mondayFirst)

	week, days := int32(0), int32(0)
	if mm == 1 && dd <= 7-weekday {
		if !weekYear &&
			((firstWeekday && weekday != 0) || (!firstWeekday && weekday >= 4)) {
			return yyyy, week
		}
		weekYear = true
		yyyy--
		days = calcDaysInYear(yyyy)
		firstDaynr -= days
		weekday = (weekday + 53*7 - days) % 7
	}

	if (firstWeekday && weekday != 0) ||
		(!firstWeekday && weekday >= 4) {
		days = daynr - (firstDaynr + 7 - weekday)
	} else {
		days = daynr - (firstDaynr - weekday)
	}

	if weekYear && days >= 52*7 {
		weekday = (weekday + calcDaysInYear(yyyy)) % 7
		if (!firstWeekday && weekday < 4) ||
			(firstWeekday && weekday == 0) {
			yyyy++
			week = 1
			return yyyy, week
		}
	}
	week = days/7 + 1
	return yyyy, week
}

// calcDaysInYear calculates days in one year, it works with 0 <= yyyy <= 99.
func calcDaysInYear(yyyy int32) int32 {
	if (yyyy&3) == 0 && (yyyy%100 != 0 || (yyyy%400 == 0 && (yyyy != 0))) {
		return 366
	}
	return 365
}

// calcDaynr calculates days since 0000-00-00.
func calcDaynr(yyyy, mm, dd int32) int32 {
	if yyyy == 0 && mm == 0 {
		return 0
	}

	delsum := 365*yyyy + 31*(mm-1) + dd
	if mm <= 2 {
		yyyy--
	} else {
		delsum -= (mm*4 + 23) / 10
	}
	return delsum + yyyy/4 - ((yyyy/100+1)*3)/4
}
