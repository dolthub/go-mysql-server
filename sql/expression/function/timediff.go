// Copyright 2021 Dolthub, Inc.
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
	"math"
	"strings"
	"time"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TimeDiff subtracts the second argument from the first expressed as a time value.
type TimeDiff struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*TimeDiff)(nil)
var _ sql.CollationCoercible = (*TimeDiff)(nil)

// NewTimeDiff creates a new NewTimeDiff expression.
func NewTimeDiff(e1, e2 sql.Expression) sql.Expression {
	return &TimeDiff{
		expression.BinaryExpression{
			Left:  e1,
			Right: e2,
		},
	}
}

// FunctionName implements sql.FunctionExpression
func (td *TimeDiff) FunctionName() string {
	return "timediff"
}

// Description implements sql.FunctionExpression
func (td *TimeDiff) Description() string {
	return "returns expr1 − expr2 expressed as a time value. expr1 and expr2 are time or date-and-time expressions, but both must be of the same type."
}

// Type implements the Expression interface.
func (td *TimeDiff) Type() sql.Type { return types.Time }

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*TimeDiff) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (td *TimeDiff) String() string {
	return fmt.Sprintf("%s(%s,%s)", td.FunctionName(), td.Left, td.Right)
}

// WithChildren implements the Expression interface.
func (td *TimeDiff) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(td, len(children), 2)
	}
	return NewTimeDiff(children[0], children[1]), nil
}

func convToDateOrTime(val interface{}) (interface{}, error) {
	date, err := types.Datetime.Convert(val)
	if err == nil {
		return date, nil
	}
	tim, err := types.Time.Convert(val)
	if err == nil {
		return tim, err
	}
	return nil, err
}

// Eval implements the Expression interface.
func (td *TimeDiff) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if td.Left == nil || td.Right == nil {
		return nil, nil
	}

	left, err := td.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	right, err := td.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil || right == nil {
		return nil, nil
	}

	// always convert string types
	if _, ok := left.(string); ok {
		left, err = convToDateOrTime(left)
		if err != nil {
			ctx.Warn(1292, err.Error())
			return nil, nil
		}
	}
	if _, ok := right.(string); ok {
		right, err = convToDateOrTime(right)
		if err != nil {
			ctx.Warn(1292, err.Error())
			return nil, nil
		}
	}

	// handle as date
	if leftDatetime, ok := left.(time.Time); ok {
		rightDatetime, ok := right.(time.Time)
		if !ok {
			return nil, nil
		}
		if leftDatetime.Location() != rightDatetime.Location() {
			rightDatetime = rightDatetime.In(leftDatetime.Location())
		}
		return types.Time.Convert(leftDatetime.Sub(rightDatetime))
	}

	// handle as time
	if leftTime, ok := left.(types.Timespan); ok {
		rightTime, ok := right.(types.Timespan)
		if !ok {
			return nil, nil
		}
		return leftTime.Subtract(rightTime), nil
	}
	return nil, sql.ErrInvalidArgumentType.New("timediff")
}

// DateDiff returns expr1 − expr2 expressed as a value in days from one date to the other.
type DateDiff struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*DateDiff)(nil)
var _ sql.CollationCoercible = (*DateDiff)(nil)

// NewDateDiff creates a new DATEDIFF() function.
func NewDateDiff(expr1, expr2 sql.Expression) sql.Expression {
	return &DateDiff{
		expression.BinaryExpression{
			Left:  expr1,
			Right: expr2,
		},
	}
}

// FunctionName implements sql.FunctionExpression
func (d *DateDiff) FunctionName() string {
	return "datediff"
}

// Description implements sql.FunctionExpression
func (d *DateDiff) Description() string {
	return "gets difference between two dates in result of days."
}

// Type implements the sql.Expression interface.
func (d *DateDiff) Type() sql.Type { return types.Int64 }

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*DateDiff) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// WithChildren implements the Expression interface.
func (d *DateDiff) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 2)
	}
	return NewDateDiff(children[0], children[1]), nil
}

// Eval implements the sql.Expression interface.
func (d *DateDiff) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if d.Left == nil || d.Right == nil {
		return nil, nil
	}

	expr1, err := d.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if expr1 == nil {
		return nil, nil
	}

	expr1, err = types.Datetime.Convert(expr1)
	if err != nil {
		return nil, err
	}

	expr1str := expr1.(time.Time).String()[:10]
	expr1, _ = types.Datetime.Convert(expr1str)

	expr2, err := d.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if expr2 == nil {
		return nil, nil
	}

	expr2, err = types.Datetime.Convert(expr2)
	if err != nil {
		return nil, err
	}

	expr2str := expr2.(time.Time).String()[:10]
	expr2, _ = types.Datetime.Convert(expr2str)

	date1 := expr1.(time.Time)
	date2 := expr2.(time.Time)

	diff := int64(math.Round(date1.Sub(date2).Hours() / 24))

	return diff, nil
}

func (d *DateDiff) String() string {
	return fmt.Sprintf("DATEDIFF(%s, %s)", d.Left, d.Right)
}

// TimestampDiff returns expr1 − expr2 expressed as a value in unit specified.
type TimestampDiff struct {
	unit  sql.Expression
	expr1 sql.Expression
	expr2 sql.Expression
}

var _ sql.FunctionExpression = (*TimestampDiff)(nil)
var _ sql.CollationCoercible = (*TimestampDiff)(nil)

// NewTimestampDiff creates a new TIMESTAMPDIFF() function.
func NewTimestampDiff(u, e1, e2 sql.Expression) sql.Expression {
	return &TimestampDiff{u, e1, e2}
}

// FunctionName implements sql.FunctionExpression
func (t *TimestampDiff) FunctionName() string {
	return "timestampdiff"
}

// Description implements sql.FunctionExpression
func (t *TimestampDiff) Description() string {
	return "gets difference between two dates in result of units specified."
}

// Children implements the sql.Expression interface.
func (t *TimestampDiff) Children() []sql.Expression {
	return []sql.Expression{t.unit, t.expr1, t.expr2}
}

// Resolved implements the sql.Expression interface.
func (t *TimestampDiff) Resolved() bool {
	return t.unit.Resolved() && t.expr1.Resolved() && t.expr2.Resolved()
}

// IsNullable implements the sql.Expression interface.
func (t *TimestampDiff) IsNullable() bool {
	return t.unit.IsNullable() && t.expr1.IsNullable() && t.expr2.IsNullable()
}

// Type implements the sql.Expression interface.
func (t *TimestampDiff) Type() sql.Type { return types.Int64 }

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*TimestampDiff) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// WithChildren implements the Expression interface.
func (t *TimestampDiff) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 3)
	}
	return NewTimestampDiff(children[0], children[1], children[2]), nil
}

// Eval implements the sql.Expression interface.
func (t *TimestampDiff) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if t.unit == nil {
		return nil, errors.NewKind("unit cannot be null").New(t.unit)
	}
	if t.expr1 == nil || t.expr2 == nil {
		return nil, nil
	}

	expr1, err := t.expr1.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if expr1 == nil {
		return nil, nil
	}

	expr2, err := t.expr2.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if expr2 == nil {
		return nil, nil
	}

	expr1, err = types.Datetime.Convert(expr1)
	if err != nil {
		return nil, err
	}

	expr2, err = types.Datetime.Convert(expr2)
	if err != nil {
		return nil, err
	}

	unit, err := t.unit.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if unit == nil {
		return nil, errors.NewKind("unit cannot be null").New(unit)
	}

	unit = strings.TrimPrefix(strings.ToLower(unit.(string)), "sql_tsi_")

	date1 := expr1.(time.Time)
	date2 := expr2.(time.Time)

	diff := date2.Sub(date1)

	var res int64
	switch unit {
	case "microsecond":
		res = diff.Microseconds()
	case "second":
		res = int64(diff.Seconds())
	case "minute":
		res = int64(diff.Minutes())
	case "hour":
		res = int64(diff.Hours())
	case "day":
		res = int64(diff.Hours() / 24)
	case "week":
		res = int64(diff.Hours() / (24 * 7))
	case "month":
		res = int64(diff.Hours() / (24 * 30))
		if res > 0 {
			if date2.Day()-date1.Day() < 0 {
				res -= 1
			} else if date2.Hour()-date1.Hour() < 0 {
				res -= 1
			} else if date2.Minute()-date1.Minute() < 0 {
				res -= 1
			} else if date2.Second()-date1.Second() < 0 {
				res -= 1
			}
		}
	case "quarter":
		monthRes := int64(diff.Hours() / (24 * 30))
		if monthRes > 0 {
			if date2.Day()-date1.Day() < 0 {
				monthRes -= 1
			} else if date2.Hour()-date1.Hour() < 0 {
				monthRes -= 1
			} else if date2.Minute()-date1.Minute() < 0 {
				monthRes -= 1
			} else if date2.Second()-date1.Second() < 0 {
				monthRes -= 1
			}
		}
		res = monthRes / 3
	case "year":
		yearRes := int64(diff.Hours() / (24 * 365))
		if yearRes > 0 {
			monthRes := int64(diff.Hours() / (24 * 30))
			if monthRes > 0 {
				if date2.Day()-date1.Day() < 0 {
					monthRes -= 1
				} else if date2.Hour()-date1.Hour() < 0 {
					monthRes -= 1
				} else if date2.Minute()-date1.Minute() < 0 {
					monthRes -= 1
				} else if date2.Second()-date1.Second() < 0 {
					monthRes -= 1
				}
			}
			res = monthRes / 12
		} else {
			res = yearRes
		}

	default:
		return nil, errors.NewKind("invalid interval unit: %s").New(unit)
	}

	return res, nil
}

func (t *TimestampDiff) String() string {
	return fmt.Sprintf("TIMESTAMPDIFF(%s, %s, %s)", t.unit, t.expr1, t.expr2)
}
