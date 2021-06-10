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

package expression

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// Between checks a value is between two given values.
type Between struct {
	Val   sql.Expression
	Lower sql.Expression
	Upper sql.Expression
}

// NewBetween creates a new Between expression.
func NewBetween(val, lower, upper sql.Expression) *Between {
	return &Between{val, lower, upper}
}

func (b *Between) String() string {
	return fmt.Sprintf("(%s BETWEEN %s AND %s)", b.Val, b.Lower, b.Upper)
}

func (b *Between) DebugString() string {
	return fmt.Sprintf("(%s BETWEEN %s AND %s)", sql.DebugString(b.Val), sql.DebugString(b.Lower), sql.DebugString(b.Upper))
}

// Children implements the Expression interface.
func (b *Between) Children() []sql.Expression {
	return []sql.Expression{b.Val, b.Lower, b.Upper}
}

// Type implements the Expression interface.
func (*Between) Type() sql.Type { return sql.Boolean }

// IsNullable implements the Expression interface.
func (b *Between) IsNullable() bool {
	return b.Val.IsNullable() || b.Lower.IsNullable() || b.Upper.IsNullable()
}

// Resolved implements the Expression interface.
func (b *Between) Resolved() bool {
	return b.Val.Resolved() && b.Lower.Resolved() && b.Upper.Resolved()
}

// Eval implements the Expression interface.
func (b *Between) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	typ := b.Val.Type().Promote()

	val, err := b.Val.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	val, err = typ.Convert(val)
	if err != nil {
		return nil, err
	}

	lower, err := b.Lower.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if lower == nil {
		return nil, nil
	}

	lower, err = typ.Convert(lower)
	if err != nil {
		return nil, err
	}

	upper, err := b.Upper.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if upper == nil {
		return nil, nil
	}

	upper, err = typ.Convert(upper)
	if err != nil {
		return nil, err
	}

	cmpLower, err := typ.Compare(val, lower)
	if err != nil {
		return nil, err
	}

	cmpUpper, err := typ.Compare(val, upper)
	if err != nil {
		return nil, err
	}

	return cmpLower >= 0 && cmpUpper <= 0, nil
}

// WithChildren implements the Expression interface.
func (b *Between) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(b, len(children), 3)
	}
	return NewBetween(children[0], children[1], children[2]), nil
}
