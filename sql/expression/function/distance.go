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
	"math"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Distance is a function that returns a point type from a WKT string
type Distance struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*Distance)(nil)

// NewDistance creates a new point expression.
func NewDistance(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_DISTANCE", "2 or 3", len(args))
	}
	return &Distance{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (d *Distance) FunctionName() string {
	return "st_distance"
}

// Description implements sql.FunctionExpression
func (d *Distance) Description() string {
	return "returns the minimum distance between any two geometries."
}

// Type implements the sql.Expression interface.
func (d *Distance) Type() sql.Type {
	return sql.Float64
}

func (d *Distance) String() string {
	var args = make([]string, len(d.ChildExpressions))
	for i, arg := range d.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_DISTANCE(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (d *Distance) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDistance(children...)
}

// FindGeometryDistance finds the minimum distance between points of any geometry types a and b
func FindGeometryDistance(a, b interface{}) float64 {
	// Recurse until a is a Point
	_a := sql.Point{}
	switch a := a.(type) {
	case sql.Point:
		_a = a
	case sql.Linestring:
		res := math.MaxFloat64
		for _, p := range a.Points {
			res = math.Min(res, FindGeometryDistance(p, b))
		}
		return res
	case sql.Polygon:
		res := math.MaxFloat64
		for _, l := range a.Lines {
			res = math.Min(res, FindGeometryDistance(l, b))
		}
		return res
	}

	// Recurse until b is a Point
	_b := sql.Point{}
	switch b := b.(type) {
	case sql.Point:
		_b = b
	case sql.Linestring:
		res := math.MaxFloat64
		for _, p := range b.Points {
			res = math.Min(res, FindGeometryDistance(a, p))
		}
		return res
	case sql.Polygon:
		res := math.MaxFloat64
		for _, l := range b.Lines {
			res = math.Min(res, FindGeometryDistance(a, l))
		}
		return res
	}

	// Return distance between two points
	return math.Sqrt(math.Pow(_a.X-_b.X, 2) + math.Pow(_a.Y-_b.Y, 2))

}

// Eval implements the sql.Expression interface.
func (d *Distance) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate first geometry
	g1, err := d.ChildExpressions[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Return nil when geometry is nil
	if g1 == nil {
		return nil, nil
	}

	// Must be a geometry type
	switch g1.(type) {
	case sql.Point, sql.Linestring, sql.Polygon:
	default:
		return nil, sql.ErrInvalidGISData.New(d.FunctionName())
	}

	// Evaluate second geometry
	g2, err := d.ChildExpressions[1].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Return nil when geometry is nil
	if g2 == nil {
		return nil, nil
	}

	// Must be a geometry type
	switch g2.(type) {
	case sql.Point, sql.Linestring, sql.Polygon:
	default:
		return nil, sql.ErrInvalidGISData.New(d.FunctionName())
	}

	// Find distance between geometry
	dist := FindGeometryDistance(g1, g2)

	// If no unit argument, return
	if len(d.ChildExpressions) == 2 {
		return dist, nil
	}

	// Evaluate units
	unit, err := d.ChildExpressions[2].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// if unit is null, return null
	if unit == nil {
		return nil, nil
	}

	// unit must be string
	_unit, err := sql.LongText.Convert(unit)
	if err != nil {
		return nil, err
	}

	// Must be a valid unit
	switch _unit.(string) {
	case "inches":
		return nil, errors.New("Not yet implemented unit: " + _unit.(string))
	default:
		return nil, errors.New("There is no unit of measure named" + _unit.(string))
	}
}
