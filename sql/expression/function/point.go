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
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// TODO: consider using a binary expression
// Point is a function that returns a point type containing values Y and Y.
type Point struct {
	X sql.Expression
	Y sql.Expression
}

var _ sql.FunctionExpression = (*Point)(nil)

// NewPoint creates a new point expression.
func NewPoint(e1, e2 sql.Expression) sql.Expression {
	return &Point{e1, e2}
}

// FunctionName implements sql.FunctionExpression
func (p *Point) FunctionName() string {
	return "point"
}

// Description implements sql.FunctionExpression
func (p *Point) Description() string {
	return "returns a new point."
}

// Children implements the sql.Expression interface.
func (p *Point) Children() []sql.Expression {
	return []sql.Expression{p.X, p.Y}
}

// Resolved implements the sql.Expression interface.
func (p *Point) Resolved() bool {
	return p.X.Resolved() && p.Y.Resolved()
}

// IsNullable implements the sql.Expression interface.
func (p *Point) IsNullable() bool {
	return p.X.IsNullable() || p.Y.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *Point) Type() sql.Type {
	return sql.Point
}

func (p *Point) String() string {
	return fmt.Sprintf("POINT(%d, %d)", p.X, p.Y)
}

// WithChildren implements the Expression interface.
func (p *Point) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 2)
	}
	return NewPoint(children[0], children[1]), nil
}

func convertToFloat64(x interface{}) (float64, error) {
	switch x.(type) {
	case int:
		return float64(x.(int)), nil
	case int8:
		return float64(x.(int8)), nil
	case int16:
		return float64(x.(int16)), nil
	case int32:
		return float64(x.(int32)), nil
	case int64:
		return float64(x.(int64)), nil
	case uint:
		return float64(x.(uint)), nil
	case uint8:
		return float64(x.(uint8)), nil
	case uint16:
		return float64(x.(uint16)), nil
	case uint32:
		return float64(x.(uint32)), nil
	case uint64:
		return float64(x.(uint64)), nil
	case float32:
		return float64(x.(float32)), nil
	case float64:
		return x.(float64), nil
	default:
		return 0, errors.New("point: wrong type")
	}
}

// Eval implements the sql.Expression interface.
func (p *Point) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate X
	x, err := p.X.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if x == nil {
		return nil, nil
	}

	// Convert to float64
	_x, err := convertToFloat64(x)
	if err != nil {
		return nil, err
	}

	// Evaluate Y
	y, err := p.Y.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if y == nil {
		return nil, nil
	}

	// Convert to float64
	_y, err := convertToFloat64(y)
	if err != nil {
		return nil, err
	}

	return sql.PointValue{X: _x, Y: _y}, nil
}
