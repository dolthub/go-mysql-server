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

	"github.com/cockroachdb/apd/v3"
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// MakeTime constructs a time value from hour, minute, and second arguments.
type MakeTime struct {
	hour   sql.Expression
	minute sql.Expression
	second sql.Expression
}

var _ sql.FunctionExpression = (*MakeTime)(nil)
var _ sql.CollationCoercible = (*MakeTime)(nil)

// NewMakeTime returns a new MakeTime expression from |hour|, |minute| and |second| arguments, producing a
// [types.Timespan] result.
func NewMakeTime(ctx *sql.Context, hour, minute, second sql.Expression) sql.Expression {
	return &MakeTime{hour: hour, minute: minute, second: second}
}

// FunctionName implements [sql.FunctionExpression].
func (m *MakeTime) FunctionName() string { return "maketime" }

// Description implements [sql.FunctionExpression].
func (m *MakeTime) Description() string {
	return "creates a time from the hour, minute, and second arguments."
}

// Type implements [sql.Expression].
func (m *MakeTime) Type(ctx *sql.Context) sql.Type {
	return types.MustCreateTimespanType(secondsPrecision(ctx, m.second))
}

// CollationCoercibility implements [sql.CollationCoercible].
func (m *MakeTime) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// IsNullable implements [sql.Expression].
func (m *MakeTime) IsNullable(ctx *sql.Context) bool { return true }

// Resolved implements [sql.Expression].
func (m *MakeTime) Resolved() bool {
	return m.hour.Resolved() && m.minute.Resolved() && m.second.Resolved()
}

// Children implements [sql.Expression].
func (m *MakeTime) Children() []sql.Expression {
	return []sql.Expression{m.hour, m.minute, m.second}
}

// String implements [fmt.Stringer].
func (m *MakeTime) String() string {
	return fmt.Sprintf("MAKETIME(%s, %s, %s)", m.hour, m.minute, m.second)
}

// WithChildren implements [sql.Expression].
func (m *MakeTime) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(m, len(children), 3)
	}
	return NewMakeTime(ctx, children[0], children[1], children[2]), nil
}

var (
	tenSeconds   = apd.New(10, 0)
	sixtySeconds = apd.New(60, 0)
)

// Eval implements [sql.Expression].
func (m *MakeTime) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	hour, ok, err := evalInt64(ctx, m.hour, row)
	if err != nil || !ok {
		return nil, err
	}
	minute, ok, err := evalInt64(ctx, m.minute, row)
	if err != nil || !ok {
		return nil, err
	}
	second, ok, err := evalDecimal(ctx, m.second, row)
	if err != nil || !ok {
		return nil, err
	}
	if minute < 0 || minute > 59 || second.Sign() < 0 || second.Cmp(sixtySeconds) >= 0 {
		return nil, nil
	}

	clipped := hour > types.MaxTimeHour || hour < -types.MaxTimeHour
	var res types.Timespan
	if !clipped {
		// The seconds are below 60, so they are never clipped, but rounding them may carry into the next minute.
		secondMicros, _ := secondsToTimespan(second)
		res = types.Timespan((max(hour, -hour)*3600+minute*60)*int64(time.Second/time.Microsecond)) + secondMicros
		clipped = res > types.MaxTimespan
	}
	if clipped {
		res = types.MaxTimespan
		// MySQL shows decimal seconds as given, and other seconds with the precision of the result.
		if !sql.IsDecimalType(m.second.Type(ctx)) {
			if second, err = sql.DecimalRound(second, int32(secondsPrecision(ctx, m.second))); err != nil {
				return nil, err
			}
		}
		secondStr := second.Text('f')
		if second.Cmp(tenSeconds) < 0 {
			secondStr = "0" + secondStr
		}
		value := fmt.Sprintf("%d:%02d:%s", hour, minute, secondStr)
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", sql.ErrTruncatedIncorrect.New("time", value).Error())
	}
	if hour < 0 {
		return -res, nil
	}
	return res, nil
}
