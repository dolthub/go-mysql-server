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

// Polygon is a function that returns a point type containing values Y and Y.
type Polygon struct {
	args []sql.Expression
}

var _ sql.FunctionExpression = (*Polygon)(nil)

// NewPolygon creates a new point expression.
func NewPolygon(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("LineString", "1 or more", len(args))
	}
	return &Polygon{args}, nil
}

// FunctionName implements sql.FunctionExpression
func (l *Polygon) FunctionName() string {
	return "polygon"
}

// Description implements sql.FunctionExpression
func (l *Polygon) Description() string {
	return "returns a new polygon."
}

// Children implements the sql.Expression interface.
func (l *Polygon) Children() []sql.Expression {
	return l.args
}

// Resolved implements the sql.Expression interface.
func (l *Polygon) Resolved() bool {
	for _, arg := range l.args {
		if !arg.Resolved() {
			return false
		}
	}
	return true
}

// IsNullable implements the sql.Expression interface.
func (l *Polygon) IsNullable() bool {
	for _, arg := range l.args {
		if arg.IsNullable() {
			return true
		}
	}
	return false
}

// Type implements the sql.Expression interface.
func (l *Polygon) Type() sql.Type {
	return sql.Polygon
}

func (l *Polygon) String() string {
	var args = make([]string, len(l.args))
	for i, arg := range l.args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("POLYGON(%s)", strings.Join(args, ", "))
}

// WithChildren implements the Expression interface.
func (l *Polygon) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewPolygon(children...)
}

// Eval implements the sql.Expression interface.
func (l *Polygon) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	var lines []sql.LinestringValue

	for _, arg := range l.args {
		val, err := arg.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		switch v := val.(type) {
		case sql.LinestringValue:
			lines = append(lines, v)
		default:
			return nil, errors.New("LineString constructor encountered a non-point")
		}
	}

	return sql.PolygonValue{Lines: lines}, nil
}
