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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// MultiPoint is a function that returns an array containing Points
type MultiPoint struct {
	args []sql.Expression
}

var _ sql.FunctionExpression = (*MultiPoint)(nil)

// NewMultiPoint creates a new point expression.
func NewMultiPoint(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("MultiPoint", "1 or more", len(args))
	}
	return &MultiPoint{args}, nil
}

// FunctionName implements sql.FunctionExpression
func (l *MultiPoint) FunctionName() string {
	return "MultiPoint"
}

// Description implements sql.FunctionExpression
func (l *MultiPoint) Description() string {
	return "returns a new multipoint."
}

// Children implements the sql.Expression interface.
func (l *MultiPoint) Children() []sql.Expression {
	return l.args
}

// Resolved implements the sql.Expression interface.
func (l *MultiPoint) Resolved() bool {
	for _, arg := range l.args {
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

// IsNullable implements the sql.Expression interface.
func (l *MultiPoint) IsNullable() bool {
	for _, arg := range l.args {
		if arg.IsNullable() {
			return true
		}
	}
	return false
}

// Type implements the sql.Expression interface.
func (l *MultiPoint) Type() sql.Type {
	return sql.MultiPoint
}

func (l *MultiPoint) String() string {
	var args = make([]string, len(l.args))
	for i, arg := range l.args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("MULTIPOINT(%s)", strings.Join(args, ", "))
}

// WithChildren implements the Expression interface.
func (l *MultiPoint) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewMultiPoint(children...)
}

// Eval implements the sql.Expression interface.
func (l *MultiPoint) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	var points []sql.PointValue

	for _, arg := range l.args {
		val, err := arg.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		switch v := val.(type) {
		case sql.PointValue:
			points = append(points, v)
		default:
			return nil, errors.New("MultiPoint constructor encountered a non-point")
		}
	}

	return sql.MultiPointValue{Points: points}, nil
}
