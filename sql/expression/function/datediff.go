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
