// Copyright 2023 Dolthub, Inc.
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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// STEquals is a function that returns the STEquals of a LineString
type STEquals struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*STEquals)(nil)

// NewSTEquals creates a new STEquals expression.
func NewSTEquals(g1, g2 sql.Expression) sql.Expression {
	return &STEquals{
		expression.BinaryExpression{
			Left:  g1,
			Right: g2,
		},
	}
}

// FunctionName implements sql.FunctionExpression
func (s *STEquals) FunctionName() string {
	return "st_equals"
}

// Description implements sql.FunctionExpression
func (s *STEquals) Description() string {
	return "returns 1 or 0 to indicate whether g1 is spatially equal to g2."
}

// Type implements the sql.Expression interface.
func (s *STEquals) Type() sql.Type {
	return types.Boolean
}

func (s *STEquals) String() string {
	return fmt.Sprintf("ST_EQUALS(%s, %s)", s.Left, s.Right)
}

// WithChildren implements the Expression interface.
func (s *STEquals) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 2)
	}
	return NewSTEquals(children[0], children[1]), nil
}

// extractPoints recursively "flattens" the geometry value into all its points
func extractPoints(g types.GeometryValue, points map[types.Point]bool) {
	switch g := g.(type) {
	case types.Point:
		points[g] = true
	case types.LineString:
		for _, p := range g.Points {
			extractPoints(p, points)
		}
	case types.Polygon:
		for _, l := range g.Lines {
			extractPoints(l, points)
		}
	case types.MultiPoint:
		for _, p := range g.Points {
			extractPoints(p, points)
		}
	case types.MultiLineString:
		for _, l := range g.Lines {
			extractPoints(l, points)
		}
	case types.MultiPolygon:
		for _, p := range g.Polygons {
			extractPoints(p, points)
		}
	case types.GeomColl:
		for _, gg := range g.Geoms {
			extractPoints(gg, points)
		}
	}
}

// isEqual checks if the set of types.Points in g1 is equal to g2
func isEqual(g1 types.GeometryValue, g2 types.GeometryValue) bool {
	switch g1 := g1.(type) {
	case types.Point:
		return isPointWithin(g1, g2)
	case types.LineString:
	case types.Polygon:
	case types.MultiPoint:
	case types.MultiLineString:
	case types.MultiPolygon:
	case types.GeomColl:
		// TODO (james): implement these
	}
	return false
}

// Eval implements the sql.Expression interface.
func (s *STEquals) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	g1, err := s.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if g1 == nil {
		return nil, nil
	}

	g2, err := s.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if g2 == nil {
		return nil, nil
	}

	var geom1, geom2 types.GeometryValue
	var ok bool
	geom1, ok = g1.(types.GeometryValue)
	if !ok {
		return nil, sql.ErrInvalidGISData.New(s.FunctionName())
	}
	geom2, ok = g2.(types.GeometryValue)
	if !ok {
		return nil, sql.ErrInvalidGISData.New(s.FunctionName())
	}

	if geom1.GetSRID() != geom2.GetSRID() {
		return nil, sql.ErrDiffSRIDs.New(s.FunctionName(), geom1.GetSRID(), geom2.GetSRID())
	}

	return isEqual(geom1, geom2), nil
}
