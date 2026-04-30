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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Between checks a value is between two given values.
type Between struct {
	Val   sql.Expression
	Lower sql.Expression
	Upper sql.Expression
}

var _ sql.Expression = (*Between)(nil)
var _ sql.CollationCoercible = (*Between)(nil)

// NewBetween creates a new Between expression.
// TODO: have this implement ValueExpression.
func NewBetween(val, lower, upper sql.Expression) *Between {
	return &Between{val, lower, upper}
}

func (b *Between) String() string {
	return fmt.Sprintf("(%s BETWEEN %s AND %s)", b.Val, b.Lower, b.Upper)
}

func (b *Between) DebugString(ctx *sql.Context) string {
	return fmt.Sprintf("(%s BETWEEN %s AND %s)", sql.DebugString(ctx, b.Val), sql.DebugString(ctx, b.Lower), sql.DebugString(ctx, b.Upper))
}

// Children implements the Expression interface.
func (b *Between) Children() []sql.Expression {
	return []sql.Expression{b.Val, b.Lower, b.Upper}
}

// Type implements the Expression interface.
func (*Between) Type(ctx *sql.Context) sql.Type { return types.Boolean }

// CollationCoercibility implements the interface sql.CollationCoercible.
func (b *Between) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, b.Val)
}

// IsNullable implements the Expression interface.
func (b *Between) IsNullable(ctx *sql.Context) bool {
	return b.Val.IsNullable(ctx) || b.Lower.IsNullable(ctx) || b.Upper.IsNullable(ctx)
}

// Resolved implements the Expression interface.
func (b *Between) Resolved() bool {
	return b.Val.Resolved() && b.Lower.Resolved() && b.Upper.Resolved()
}

// Eval implements the Expression interface.
func (b *Between) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// TODO: implement between without reusing LTE/GTE expressions
	lower, err := b.Lower.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	upper, err := b.Upper.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	value, err := b.Val.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if lower == nil || upper == nil || value == nil {
		return nil, nil
	}

	lower, err = sql.UnwrapAny(ctx, lower)
	if err != nil {
		return nil, err
	}
	upper, err = sql.UnwrapAny(ctx, upper)
	if err != nil {
		return nil, err
	}
	value, err = sql.UnwrapAny(ctx, value)
	if err != nil {
		return nil, err
	}

	var cmp int
	lTyp, uTyp, vTyp := b.Lower.Type(ctx), b.Upper.Type(ctx), b.Val.Type(ctx)
	if types.TypesEqual(lTyp, vTyp) && types.TypesEqual(uTyp, vTyp) {
		cmp, err = vTyp.Compare(ctx, value, lower)
		if err != nil {
			return nil, err
		}
		if cmp < 0 {
			return false, nil
		}

		cmp, err = vTyp.Compare(ctx, value, upper)
		if err != nil {
			return nil, err
		}
		if cmp > 0 {
			return false, nil
		}

		return true, nil
	}

	// TODO: refactor to get rid of repeated work
	low, lowVal, lowCmpType, err := (&comparison{}).castLeftAndRight(ctx, lower, value)
	if err != nil {
		return nil, err
	}
	upp, uppVal, uppCmpType, err := (&comparison{}).castLeftAndRight(ctx, upper, value)
	if err != nil {
		return nil, err
	}
	// TODO: set and string logic

	cmp, err = lowCmpType.Compare(ctx, lowVal, low)
	if err != nil {
		return nil, err
	}
	if cmp < 0 {
		return false, nil
	}

	cmp, err = uppCmpType.Compare(ctx, uppVal, upp)
	if err != nil {
		return nil, err
	}
	if cmp > 0 {
		return false, nil
	}

	return true, nil
}

// WithChildren implements the Expression interface.
func (b *Between) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(b, len(children), 3)
	}
	return NewBetween(children[0], children[1], children[2]), nil
}
