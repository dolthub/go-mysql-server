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

package function

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
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
	return sql.Int8 // TODO: bool?
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
func extractPoints(g sql.GeometryValue, points map[sql.Point]bool) {
	switch g := g.(type) {
	case sql.Point:
		points[g] = true
	case sql.LineString:
		for _, p := range g.Points {
			extractPoints(p, points)
		}
	case sql.Polygon:
		for _, l := range g.Lines {
			extractPoints(l, points)
		}
	case sql.MultiPoint:
		for _, p := range g.Points {
			extractPoints(p, points)
		}
	case sql.MultiLineString:
		for _, l := range g.Lines {
			extractPoints(l, points)
		}
	case sql.MultiPolygon:
		for _, p := range g.Polygons {
			extractPoints(p, points)
		}
	case sql.GeomColl:
		for _, gg := range g.Geoms {
			extractPoints(gg, points)
		}
	}
}

// isSpatiallyEqual checks if the set of sql.Points in g1 is equal to g2
func isSpatiallyEqual(g1 sql.GeometryValue, g2 sql.GeometryValue) int8 {
	m1 := map[sql.Point]bool{}
	m2 := map[sql.Point]bool{}
	extractPoints(g1, m1)
	extractPoints(g2, m2)

	if len(m1) != len(m2) {
		return 0
	}

	for k := range m1 {
		if !m2[k] {
			return 0
		}
	}

	return 1
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

	var geom1, geom2 sql.GeometryValue
	var ok bool
	geom1, ok = g1.(sql.GeometryValue)
	if !ok {
		return nil, sql.ErrInvalidGISData.New(s.FunctionName())
	}
	geom2, ok = g2.(sql.GeometryValue)
	if !ok {
		return nil, sql.ErrInvalidGISData.New(s.FunctionName())
	}

	if geom1.GetGeomType() != geom2.GetGeomType() {
		return 0, nil
	}

	if geom1.GetSRID() != geom2.GetSRID() {
		return nil, sql.ErrDiffSRIDs.New(s.FunctionName(), geom1.GetSRID(), geom2.GetSRID())
	}

	return isSpatiallyEqual(geom1, geom2), nil
}
