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

package spatial

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/go-mysql-server/sql"
)

// Polygon is a function that returns a Polygon.
type Polygon struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*Polygon)(nil)

// NewPolygon creates a new polygon expression.
func NewPolygon(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("Polygon", "1 or more", len(args))
	}
	return &Polygon{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (p *Polygon) FunctionName() string {
	return "polygon"
}

// Description implements sql.FunctionExpression
func (p *Polygon) Description() string {
	return "returns a new polygon."
}

// Type implements the sql.Expression interface.
func (p *Polygon) Type() sql.Type {
	return sql.PolygonType{}
}

func (p *Polygon) String() string {
	var args = make([]string, len(p.ChildExpressions))
	for i, arg := range p.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("%s(%s)", p.FunctionName(), strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (p *Polygon) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewPolygon(children...)
}

// TODO: should go in line?
func isLinearRing(line sql.LineString) bool {
	// Get number of points
	numPoints := len(line.Points)
	// Check length of LineString (must be 0 or 4+) points
	if numPoints != 0 && numPoints < 4 {
		return false
	}
	// Check if it is closed (first and last point are the same)
	if line.Points[0] != line.Points[numPoints-1] {
		return false
	}
	return true
}

// Eval implements the sql.Expression interface.
func (p *Polygon) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Allocate array of lines
	var lines = make([]sql.LineString, len(p.ChildExpressions))

	// Go through each argument
	for i, arg := range p.ChildExpressions {
		// Evaluate argument
		val, err := arg.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		// Must be of type linestring, throw error otherwise
		switch v := val.(type) {
		case sql.LineString:
			// Check that line is a linear ring
			if isLinearRing(v) {
				lines[i] = v
			} else {
				return nil, errors.New("Invalid GIS data provided to function polygon.")
			}
		case sql.GeometryValue:
			return nil, sql.ErrInvalidArgumentDetails.New(p.FunctionName(), v)
		default:
			return nil, sql.ErrIllegalGISValue.New(v)
		}
	}

	return sql.Polygon{Lines: lines}, nil
}
