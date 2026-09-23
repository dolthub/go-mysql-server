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

// YearWeek is a function that returns year and week for a date.
// The year in the result may be different from the year in the date argument for the first and the last week of the year.
// Details: https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_yearweek
type YearWeek struct {
	date sql.Expression
	mode sql.Expression
}

var _ sql.FunctionExpression = (*YearWeek)(nil)
var _ sql.CollationCoercible = (*YearWeek)(nil)

// NewYearWeek creates a new YearWeek UDF
func NewYearWeek(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New("YEARWEEK", "1 or more", 0)
	}

	yw := &YearWeek{date: args[0]}
	if len(args) > 1 && args[1].Resolved() && types.IsInteger(args[1].Type(ctx)) {
		yw.mode = args[1]
	} else if len(args) > 1 && expression.IsBindVar(args[1]) {
		yw.mode = args[1]
	} else {
		yw.mode = expression.NewLiteral(0, types.Int64)
	}

	return yw, nil
}

// Name implements sql.FunctionExpression
func (d *YearWeek) Name() string {
	return "yearweek"
}

// Description implements sql.FunctionExpression
func (d *YearWeek) Description() string {
	return "returns year and week for a date. The year in the result may be different from the year in the date argument for the first and the last week of the year."
}

// String implements the sql.Expression interface.
func (d *YearWeek) String() string { return fmt.Sprintf("YEARWEEK(%s, %s)", d.date, d.mode) }

// Type implements the Expression interface.
func (d *YearWeek) Type(ctx *sql.Context) sql.Type { return types.Int32 }

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*YearWeek) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Eval implements the Expression interface.
func (d *YearWeek) Eval(ctx *sql.Context, row sql.Row) (any, error) {
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
		return nil, sql.ErrInvalidArgumentDetails.New("YEARWEEK", "invalid year")
	}
	mm, ok := month(date).(int)
	if !ok {
		return nil, sql.ErrInvalidArgumentDetails.New("YEARWEEK", "invalid month")
	}
	dd, ok := day(date).(int)
	if !ok {
		return nil, sql.ErrInvalidArgumentDetails.New("YEARWEEK", "invalid day")
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
	yr, week := calcWeek(int32(yyyy), int32(mm), int32(dd), weekMode(mode)|weekBehaviourYear)

	return (yr * 100) + week, nil
}

// Resolved implements the Expression interface.
func (d *YearWeek) Resolved() bool {
	return d.date.Resolved() && d.mode.Resolved()
}

// Children implements the Expression interface.
func (d *YearWeek) Children() []sql.Expression { return []sql.Expression{d.date, d.mode} }

// IsNullable implements the Expression interface.
func (d *YearWeek) IsNullable(ctx *sql.Context) bool {
	return true
}

// WithChildren implements the Expression interface.
func (*YearWeek) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewYearWeek(ctx, children...)
}
