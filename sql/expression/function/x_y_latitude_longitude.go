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
	"strings"

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

// STX is a function that the x value from a given point.
type STX struct {
	args []sql.Expression
}

var _ sql.FunctionExpression = (*STX)(nil)

var ErrInvalidType = errors.NewKind("%s received non-point type")

// NewSTX creates a new STX expression.
func NewSTX(args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_X", "1 or 2", len(args))
	}
	return &STX{args}, nil
}

// FunctionName implements sql.FunctionExpression
func (s *STX) FunctionName() string {
	return "st_x"
}

// Description implements sql.FunctionExpression
func (s *STX) Description() string {
	return "returns the x value of given point. If given a second argument, returns a new point with second argument as x value."
}

// Children implements the sql.Expression interface.
func (s *STX) Children() []sql.Expression {
	return s.args
}

// Resolved implements the sql.Expression interface.
func (s *STX) Resolved() bool {
	for _, arg := range s.args {
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

// IsNullable implements the sql.Expression interface.
func (s *STX) IsNullable() bool {
	for _, arg := range s.args {
		if arg.IsNullable() {
			return true
		}
	}
	return false
}

// Type implements the sql.Expression interface.
func (s *STX) Type() sql.Type {
	if len(s.args) == 1 {
		return sql.Float64
	} else {
		return sql.PointType{}
	}
}

func (s *STX) String() string {
	var args = make([]string, len(s.args))
	for i, arg := range s.args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_X(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (s *STX) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewSTX(children...)
}

// Eval implements the sql.Expression interface.
func (s *STX) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate point
	p, err := s.args[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Return null if geometry is null
	if p == nil {
		return nil, nil
	}

	// Check that it is a point
	_p, ok := p.(sql.Point)
	if !ok {
		return nil, ErrInvalidType.New(s.FunctionName())
	}

	// If just one argument, return X
	if len(s.args) == 1 {
		return _p.X, nil
	}

	// Evaluate second argument
	x, err := s.args[1].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Return null if second argument is null
	if x == nil {
		return nil, nil
	}

	// Convert to float64
	_x, err := sql.Float64.Convert(x)
	if err != nil {
		return nil, err
	}

	// Create point with new X and old Y
	return sql.Point{X: _x.(float64), Y: _p.Y}, nil
}

// STY is a function that the x value from a given point.
type STY struct {
	args []sql.Expression
}

var _ sql.FunctionExpression = (*STY)(nil)

// NewSTY creates a new STY expression.
func NewSTY(args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_Y", "1 or 2", len(args))
	}
	return &STY{args}, nil
}

// FunctionName implements sql.FunctionExpression
func (s *STY) FunctionName() string {
	return "st_y"
}

// Description implements sql.FunctionExpression
func (s *STY) Description() string {
	return "returns the y value of given point. If given a second argument, returns a new point with second argument as y value."
}

// Children implements the sql.Expression interface.
func (s *STY) Children() []sql.Expression {
	return s.args
}

// Resolved implements the sql.Expression interface.
func (s *STY) Resolved() bool {
	for _, arg := range s.args {
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

// IsNullable implements the sql.Expression interface.
func (s *STY) IsNullable() bool {
	for _, arg := range s.args {
		if arg.IsNullable() {
			return true
		}
	}
	return false
}

// Type implements the sql.Expression interface.
func (s *STY) Type() sql.Type {
	if len(s.args) == 1 {
		return sql.Float64
	} else {
		return sql.PointType{}
	}
}

func (s *STY) String() string {
	var args = make([]string, len(s.args))
	for i, arg := range s.args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_Y(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (s *STY) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewSTY(children...)
}

// Eval implements the sql.Expression interface.
func (s *STY) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate point
	p, err := s.args[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Return null if geometry is null
	if p == nil {
		return nil, nil
	}

	// Check that it is a point
	_p, ok := p.(sql.Point)
	if !ok {
		return nil, ErrInvalidType.New(s.FunctionName())
	}

	// If just one argument, return Y
	if len(s.args) == 1 {
		return _p.Y, nil
	}

	// Evaluate second argument
	y, err := s.args[1].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Return null if second argument is null
	if y == nil {
		return nil, nil
	}

	// Convert to float64
	_y, err := sql.Float64.Convert(y)
	if err != nil {
		return nil, err
	}

	// Create point with old X and new Ys
	return sql.Point{X: _p.X, Y: _y.(float64)}, nil
}
