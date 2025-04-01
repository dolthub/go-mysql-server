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

package function

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Conv function converts numbers between different number bases. Returns a string representation of the number N, converted from base from_base to base to_base.
type Conv struct {
	n        sql.Expression
	fromBase sql.Expression
	toBase   sql.Expression
}

var _ sql.FunctionExpression = (*Conv)(nil)
var _ sql.CollationCoercible = (*Conv)(nil)

// NewConv returns a new Conv expression.
func NewConv(n, from, to sql.Expression) sql.Expression {
	return &Conv{n, from, to}
}

// FunctionName implements sql.FunctionExpression
func (c *Conv) FunctionName() string {
	return "conv"
}

// Description implements sql.FunctionExpression
func (c *Conv) Description() string {
	return "returns a string representation of the number N, converted from base from_base to base to_base."
}

// Type implements the Expression interface.
func (c *Conv) Type() sql.Type { return types.LongText }

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Conv) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return ctx.GetCollation(), 4
}

// IsNullable implements the Expression interface.
func (c *Conv) IsNullable() bool {
	return c.n.IsNullable() || c.fromBase.IsNullable() || c.toBase.IsNullable()
}

func (c *Conv) String() string {
	return fmt.Sprintf("%s(%s,%s,%s)", c.FunctionName(), c.n, c.fromBase, c.toBase)
}

// Eval implements the Expression interface.
func (c *Conv) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	n, err := c.n.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, nil
	}

	from, err := c.fromBase.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if from == nil {
		return nil, nil
	}

	to, err := c.toBase.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if to == nil {
		return nil, nil
	}

	n, _, err = types.LongText.Convert(ctx, n)
	if err != nil {
		return nil, nil
	}

	// valConvertedFrom is unsigned if n input is positive, signed if negative.
	valConvertedFrom := convertFromBase(n.(string), from)
	switch valConvertedFrom {
	case nil:
		return nil, nil
	case 0:
		return "0", nil
	}

	result := convertToBase(valConvertedFrom, to)
	if result == "" {
		return nil, nil
	}

	return strings.ToUpper(result), nil
}

// Resolved implements the Expression interface.
func (c *Conv) Resolved() bool {
	return c.n.Resolved() && c.fromBase.Resolved() && c.toBase.Resolved()
}

// Children implements the Expression interface.
func (c *Conv) Children() []sql.Expression {
	return []sql.Expression{c.n, c.fromBase, c.toBase}
}

// WithChildren implements the Expression interface.
func (c *Conv) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 3)
	}
	return NewConv(children[0], children[1], children[2]), nil
}

// convertFromBase returns nil if fromBase input is invalid, 0 if nVal input is invalid and converted result if nVal and fromBase inputs are valid.
// This conversion truncates nVal as its first subpart that is convertable.
// nVal is treated as unsigned except nVal is negative.
func convertFromBase(nVal string, fromBase interface{}) interface{} {
	fromBase, _, err := types.Int64.Convert(ctx, fromBase)
	if err != nil {
		return nil
	}

	fromVal := int(math.Abs(float64(fromBase.(int64))))
	if fromVal < 2 || fromVal > 36 {
		return nil
	}

	negative := false
	var upper string
	var lower string
	if nVal[0] == '-' {
		negative = true
		nVal = nVal[1:]
	} else if nVal[0] == '+' {
		nVal = nVal[1:]
	}

	// check for upper and lower bound for given fromBase
	if negative {
		upper = strconv.FormatInt(math.MaxInt64, fromVal)
		lower = strconv.FormatInt(math.MinInt64, fromVal)
		if len(nVal) > len(lower) {
			nVal = lower
		} else if len(nVal) > len(upper) {
			nVal = upper
		}
	} else {
		upper = strconv.FormatUint(math.MaxUint64, fromVal)
		lower = "0"
		if len(nVal) < len(lower) {
			nVal = lower
		} else if len(nVal) > len(upper) {
			nVal = upper
		}
	}

	truncate := false
	result := uint64(0)
	i := 1
	for !truncate && i <= len(nVal) {
		val, err := strconv.ParseUint(nVal[:i], fromVal, 64)
		if err != nil {
			truncate = true
			return result
		}
		result = val
		i++
	}

	if negative {
		return int64(result) * -1
	}

	return result
}

// convertToBase returns result of whole CONV function in string format, empty string if to input is invalid.
// The sign of toBase decides whether result is formatted as signed or unsigned.
func convertToBase(val interface{}, toBase interface{}) string {
	toBase, _, err := types.Int64.Convert(ctx, toBase)
	if err != nil {
		return ""
	}

	toVal := int(math.Abs(float64(toBase.(int64))))
	if toVal < 2 || toVal > 36 {
		return ""
	}

	var result string
	switch v := val.(type) {
	case int64:
		if toBase.(int64) < 0 {
			result = strconv.FormatInt(v, toVal)
			if err != nil {
				return ""
			}
		} else {
			result = strconv.FormatUint(uint64(v), toVal)
			if err != nil {
				return ""
			}
		}
	case uint64:
		if toBase.(int64) < 0 {
			result = strconv.FormatInt(int64(v), toVal)
			if err != nil {
				return ""
			}
		} else {
			result = strconv.FormatUint(v, toVal)
			if err != nil {
				return ""
			}
		}
	}

	return result
}
