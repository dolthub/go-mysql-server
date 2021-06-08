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
	"fmt"
	"math"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Sqrt is a function that returns the square value of the number provided.
type Sqrt struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Sqrt)(nil)

// NewSqrt creates a new Sqrt expression.
func NewSqrt(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &Sqrt{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (s *Sqrt) FunctionName() string {
	return "sqrt"
}

func (s *Sqrt) String() string {
	return fmt.Sprintf("sqrt(%s)", s.Child.String())
}

// Type implements the Expression interface.
func (s *Sqrt) Type() sql.Type {
	return sql.Float64
}

// IsNullable implements the Expression interface.
func (s *Sqrt) IsNullable() bool {
	return s.Child.IsNullable()
}

// WithChildren implements the Expression interface.
func (s *Sqrt) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}
	return NewSqrt(ctx, children[0]), nil
}

// Eval implements the Expression interface.
func (s *Sqrt) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	child, err := s.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if child == nil {
		return nil, nil
	}

	child, err = sql.Float64.Convert(child)
	if err != nil {
		return nil, err
	}

	return math.Sqrt(child.(float64)), nil
}

// Power is a function that returns value of X raised to the power of Y.
type Power struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*Power)(nil)

// NewPower creates a new Power expression.
func NewPower(ctx *sql.Context, e1, e2 sql.Expression) sql.Expression {
	return &Power{
		expression.BinaryExpression{
			Left:  e1,
			Right: e2,
		},
	}
}

// FunctionName implements sql.FunctionExpression
func (p *Power) FunctionName() string {
	return "power"
}

// Type implements the Expression interface.
func (p *Power) Type() sql.Type { return sql.Float64 }

// IsNullable implements the Expression interface.
func (p *Power) IsNullable() bool { return p.Left.IsNullable() || p.Right.IsNullable() }

func (p *Power) String() string {
	return fmt.Sprintf("power(%s, %s)", p.Left, p.Right)
}

// WithChildren implements the Expression interface.
func (p *Power) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 2)
	}
	return NewPower(ctx, children[0], children[1]), nil
}

// Eval implements the Expression interface.
func (p *Power) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	left, err := p.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, nil
	}

	left, err = sql.Float64.Convert(left)
	if err != nil {
		return nil, err
	}

	right, err := p.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if right == nil {
		return nil, nil
	}

	right, err = sql.Float64.Convert(right)
	if err != nil {
		return nil, err
	}

	return math.Pow(left.(float64), right.(float64)), nil
}
