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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// TimeDiff subtracts the second argument from the first expressed as a time value.
type TimeDiff struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*TimeDiff)(nil)

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
func (td *TimeDiff) Type() sql.Type { return sql.Time }

// IsNullable implements the Expression interface.
func (td *TimeDiff) IsNullable() bool { return false }

func (td *TimeDiff) String() string {
	return fmt.Sprintf("TIMEDIFF(%s, %s)", td.Left, td.Right)
}

// WithChildren implements the Expression interface.
func (td *TimeDiff) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(td, len(children), 2)
	}
	return NewTimeDiff(children[0], children[1]), nil
}

// Eval implements the Expression interface.
func (td *TimeDiff) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	left, err := td.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	right, err := td.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil || right == nil {
		return nil, ErrTimeUnexpectedlyNil.New("TIMEDIFF")
	}

	if leftDatetimeInt, err := sql.Datetime.Convert(left); err == nil {
		rightDatetimeInt, err := sql.Datetime.Convert(right)
		if err != nil {
			return nil, err
		}
		leftDatetime := leftDatetimeInt.(time.Time)
		rightDatetime := rightDatetimeInt.(time.Time)
		if leftDatetime.Location() != rightDatetime.Location() {
			rightDatetime = rightDatetime.In(leftDatetime.Location())
		}
		return sql.Time.Convert(leftDatetime.Sub(rightDatetime))
	} else if leftTime, err := sql.Time.ConvertToTimeDuration(left); err == nil {
		rightTime, err := sql.Time.ConvertToTimeDuration(right)
		if err != nil {
			return nil, err
		}
		resTime := leftTime - rightTime
		return sql.Time.Convert(resTime)
	} else {
		return nil, ErrInvalidArgumentType.New("timediff")
	}
}

// DateDiff returns expr1 − expr2 expressed as a value in days from one date to the other.
type DateDiff struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*DateDiff)(nil)

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

// Children implements the sql.Expression interface.
func (d *DateDiff) Children() []sql.Expression {
	return []sql.Expression{d.Left, d.Right}
}

// Resolved implements the sql.Expression interface.
func (d *DateDiff) Resolved() bool {
	return d.Left.Resolved() && d.Right.Resolved()
}

// IsNullable implements the sql.Expression interface.
func (d *DateDiff) IsNullable() bool {
	return d.Left.IsNullable() && d.Right.IsNullable()
}

// Type implements the sql.Expression interface.
func (d *DateDiff) Type() sql.Type { return sql.Int64 }

// WithChildren implements the Expression interface.
func (d *DateDiff) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) == 2 {
		return NewDateDiff(children[0], children[1]), nil
	} else {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 2)
	}
}

// Eval implements the sql.Expression interface.
func (d *DateDiff) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	expr1, err := d.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if expr1 == nil {
		return nil, nil
	}

	expr1, err = sql.Datetime.Convert(expr1)
	if err != nil {
		return nil, err
	}

	expr1str := expr1.(time.Time).String()[:10]
	expr1, _ = sql.Datetime.Convert(expr1str)

	expr2, err := d.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if expr2 == nil {
		return nil, nil
	}

	expr2, err = sql.Datetime.Convert(expr2)
	if err != nil {
		return nil, err
	}

	expr2str := expr2.(time.Time).String()[:10]
	expr2, _ = sql.Datetime.Convert(expr2str)

	date1 := expr1.(time.Time)
	date2 := expr2.(time.Time)

	diff := int64(math.Round(date1.Sub(date2).Hours() / 24))

	return diff, nil
}

func (d *DateDiff) String() string {
	return fmt.Sprintf("DATEDIFF(%s, %s)", d.Left, d.Right)
}
