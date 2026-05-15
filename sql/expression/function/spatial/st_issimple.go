// Copyright 2025 Dolthub, Inc.
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

// IsSimple is a function that returns whether a geometry value is simple.
// A geometry is simple if it has no anomalous geometric points, such as self-intersection or self-tangency.
type IsSimple struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*IsSimple)(nil)
var _ sql.CollationCoercible = (*IsSimple)(nil)

// NewIsSimple creates a new IsSimple expression.
func NewIsSimple(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &IsSimple{expression.UnaryExpressionStub{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (s *IsSimple) FunctionName() string {
	return "st_issimple"
}

// Description implements sql.FunctionExpression
func (s *IsSimple) Description() string {
	return "returns whether the geometry value is simple (has no anomalous geometric points)."
}

// IsNullable implements the sql.Expression interface.
func (s *IsSimple) IsNullable(ctx *sql.Context) bool {
	return s.Child.IsNullable(ctx)
}

// Type implements the sql.Expression interface.
func (s *IsSimple) Type(ctx *sql.Context) sql.Type {
	return types.Boolean
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*IsSimple) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (s *IsSimple) String() string {
	return fmt.Sprintf("%s(%s)", s.FunctionName(), s.Child.String())
}

// WithChildren implements the Expression interface.
func (s *IsSimple) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}
	return NewIsSimple(ctx, children[0]), nil
}

// segmentsProperlyIntersect checks whether two line segments (p1-p2) and (p3-p4)
// properly intersect (cross each other, not just share endpoints).
func segmentsProperlyIntersect(p1, p2, p3, p4 types.Point) bool {
	d1 := cross2D(p3, p4, p1)
	d2 := cross2D(p3, p4, p2)
	d3 := cross2D(p1, p2, p3)
	d4 := cross2D(p1, p2, p4)

	if ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
		((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0)) {
		return true
	}

	// Check collinear overlap cases
	if d1 == 0 && d2 == 0 {
		// Segments are collinear; check for overlap
		return collinearOverlap(p1, p2, p3, p4)
	}

	return false
}

// collinearOverlap checks if two collinear segments overlap (share more than an endpoint).
func collinearOverlap(p1, p2, p3, p4 types.Point) bool {
	minX1 := min(p1.X, p2.X)
	maxX1 := max(p1.X, p2.X)
	minX3 := min(p3.X, p4.X)
	maxX3 := max(p3.X, p4.X)
	minY1 := min(p1.Y, p2.Y)
	maxY1 := max(p1.Y, p2.Y)
	minY3 := min(p3.Y, p4.Y)
	maxY3 := max(p3.Y, p4.Y)

	overlapX := maxX1 > minX3 && maxX3 > minX1
	overlapY := maxY1 > minY3 && maxY3 > minY1

	// For vertical lines, check Y overlap; for horizontal, check X overlap; general: both
	if minX1 == maxX1 && minX3 == maxX3 {
		return overlapY
	}
	return overlapX
}

// isLineStringSimple checks if a LineString is simple (no self-intersections except at endpoints).
func isLineStringSimple(l types.LineString) bool {
	n := len(l.Points)
	if n <= 2 {
		return true
	}
	// A linestring is closed if the first and last points coincide
	closed := l.Points[0].X == l.Points[n-1].X && l.Points[0].Y == l.Points[n-1].Y

	// Check all pairs of non-adjacent segments for intersection
	for i := 0; i < n-1; i++ {
		for j := i + 2; j < n-1; j++ {
			// For closed linestrings, skip the pair of first and last segments (they share the closing vertex)
			if closed && i == 0 && j == n-2 {
				continue
			}
			if segmentsProperlyIntersect(l.Points[i], l.Points[i+1], l.Points[j], l.Points[j+1]) {
				return false
			}
		}
	}
	return true
}

// Eval implements the sql.Expression interface.
func (s *IsSimple) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := s.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	gv, err := types.UnwrapGeometry(ctx, val)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New(s.FunctionName())
	}

	switch v := gv.(type) {
	case types.Point:
		return true, nil

	case types.LineString:
		return isLineStringSimple(v), nil

	case types.Polygon:
		// A valid polygon is always simple by definition
		return true, nil

	case types.MultiPoint:
		// MultiPoint is simple if no two points are identical
		seen := make(map[[2]float64]bool)
		for _, p := range v.Points {
			key := [2]float64{p.X, p.Y}
			if seen[key] {
				return false, nil
			}
			seen[key] = true
		}
		return true, nil

	case types.MultiLineString:
		for _, line := range v.Lines {
			if !isLineStringSimple(line) {
				return false, nil
			}
		}
		return true, nil

	case types.MultiPolygon:
		return true, nil

	case types.GeomColl:
		// A geometry collection is simple if all its elements are simple
		for _, geom := range v.Geoms {
			childResult, err := NewIsSimple(ctx, nil).(*IsSimple).evalRaw(ctx, geom)
			if err != nil {
				return nil, err
			}
			if childResult == false {
				return false, nil
			}
		}
		return true, nil

	default:
		return nil, sql.ErrInvalidGISData.New(s.FunctionName())
	}
}

// evalRaw checks simplicity for an already-unwrapped geometry value.
func (s *IsSimple) evalRaw(ctx *sql.Context, gv types.GeometryValue) (interface{}, error) {
	switch v := gv.(type) {
	case types.Point:
		return true, nil
	case types.LineString:
		return isLineStringSimple(v), nil
	case types.Polygon:
		return true, nil
	case types.MultiPoint:
		seen := make(map[[2]float64]bool)
		for _, p := range v.Points {
			key := [2]float64{p.X, p.Y}
			if seen[key] {
				return false, nil
			}
			seen[key] = true
		}
		return true, nil
	case types.MultiLineString:
		for _, line := range v.Lines {
			if !isLineStringSimple(line) {
				return false, nil
			}
		}
		return true, nil
	case types.MultiPolygon:
		return true, nil
	case types.GeomColl:
		for _, geom := range v.Geoms {
			childResult, err := s.evalRaw(ctx, geom)
			if err != nil {
				return nil, err
			}
			if childResult == false {
				return false, nil
			}
		}
		return true, nil
	default:
		return true, nil
	}
}
