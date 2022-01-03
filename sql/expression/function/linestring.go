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

// Linestring is a function that returns a point type containing values Y and Y.
type Linestring struct {
	args []sql.Expression
}

var _ sql.FunctionExpression = (*Linestring)(nil)

// NewLinestring creates a new point expression.
func NewLinestring(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("Linestring", "2 or more", len(args))
	}
	return &Linestring{args}, nil
}

// FunctionName implements sql.FunctionExpression
func (l *Linestring) FunctionName() string {
	return "linestring"
}

// Description implements sql.FunctionExpression
func (l *Linestring) Description() string {
	return "returns a new linestring."
}

// Children implements the sql.Expression interface.
func (l *Linestring) Children() []sql.Expression {
	return l.args
}

// Resolved implements the sql.Expression interface.
func (l *Linestring) Resolved() bool {
	for _, arg := range l.args {
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

// IsNullable implements the sql.Expression interface.
func (l *Linestring) IsNullable() bool {
	for _, arg := range l.args {
		if arg.IsNullable() {
			return true
		}
	}
	return false
}

// Type implements the sql.Expression interface.
func (l *Linestring) Type() sql.Type {
	return sql.LinestringType{}
}

func (l *Linestring) String() string {
	var args = make([]string, len(l.args))
	for i, arg := range l.args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("LINESTRING(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (l *Linestring) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewLinestring(children...)
}

// Eval implements the sql.Expression interface.
func (l *Linestring) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Allocate array of points
	var points = make([]sql.Point, len(l.args))

	// Go through each argument
	for i, arg := range l.args {
		// Evaluate argument
		val, err := arg.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		// Must be of type point, throw error otherwise
		switch v := val.(type) {
		case sql.Point:
			points[i] = v
		default:
			return nil, errors.New("linestring constructor encountered a non-point")
		}
	}

	return sql.Linestring{Points: points}, nil
}
