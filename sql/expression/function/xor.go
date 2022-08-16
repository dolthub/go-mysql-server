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

package function

import (
	"github.com/dolthub/go-mysql-server/sql"
	"math"
)

// BitXOR is the BIT_XOR function
type BitXOR struct {
	*UnaryFunc
}

var _ sql.FunctionExpression = (*BitXOR)(nil)

// NewSin returns a new SIN function expression
func NewBitXOR(arg sql.Expression) sql.Expression {
	return &BitXOR{NewUnaryFunc(arg, "BIT_XOR", sql.Int64)}
}

// Description implements sql.FunctionExpression
func (b *BitXOR) Description() string {
	return "returns the bitwise xor of the expression given."
}

// Eval implements sql.Expression
func (b *BitXOR) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := b.EvalChild(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	n, err := sql.Int64.Convert(val)
	if err != nil {
		return nil, err
	}

	return math.Sin(n.(int64)), nil
}

// WithChildren implements sql.Expression
func (b *BitXOR) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}
	return NewSin(children[0]), nil
}
