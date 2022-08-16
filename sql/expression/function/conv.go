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
	"github.com/dolthub/go-mysql-server/sql"
	"math"
	"strconv"
	"strings"
)

// Conv function converts numbers between different number bases. Returns a string representation of the number N, converted from base from_base to base to_base.
type Conv struct {
	n        sql.Expression
	fromBase sql.Expression
	toBase   sql.Expression
}

var _ sql.FunctionExpression = (*Conv)(nil)

// NewConv returns a new Conv expression.
func NewConv(n, from, to sql.Expression) sql.Expression {
	return &Conv{n, from, to}
}

// FunctionName implements sql.FunctionExpression
func (c *Conv) FunctionName() string {
	return "Conv"
}

// Description implements sql.FunctionExpression
func (c *Conv) Description() string {
	return "returns a string representation of the number N, converted from base from_base to base to_base."
}

// Type implements the Expression interface.
func (c *Conv) Type() sql.Type { return sql.LongText }

// IsNullable implements the Expression interface.
func (c *Conv) IsNullable() bool {
	return c.n.IsNullable() || c.fromBase.IsNullable() || c.toBase.IsNullable()
}

func (c *Conv) String() string {
	return fmt.Sprintf("Conv(%s, %s, %s)", c.n, c.fromBase, c.toBase)
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

	n, err = sql.LongText.Convert(n)
	if err != nil {
		return nil, nil
	}

	fromValue := getIntValueForBase(from)
	if fromValue == nil {
		return nil, nil
	}

	val, err := strconv.ParseInt(n.(string), fromValue.(int), 64)
	if err != nil {
		return "0", nil
	}

	toValue := getIntValueForBase(to)
	if toValue == nil {
		return nil, nil
	}

	result := strconv.FormatInt(val, toValue.(int))
	if err != nil {
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

func getIntValueForBase(num interface{}) interface{} {
	num, err := sql.Int64.Convert(num)
	if err != nil {
		return nil
	}
	numVal := int(math.Abs(float64(num.(int64))))
	if numVal < 2 || numVal > 36 {
		return nil
	}

	return numVal
}
