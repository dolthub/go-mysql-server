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
	"sort"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ConvexHull is a function that returns the convex hull of a geometry value.
type ConvexHull struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*ConvexHull)(nil)
var _ sql.CollationCoercible = (*ConvexHull)(nil)

// NewConvexHull creates a new ConvexHull expression.
func NewConvexHull(e sql.Expression) sql.Expression {
	return &ConvexHull{expression.UnaryExpressionStub{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (c *ConvexHull) FunctionName() string {
	return "st_convexhull"
}

// Description implements sql.FunctionExpression
func (c *ConvexHull) Description() string {
	return "returns the convex hull of the geometry value."
}

// IsNullable implements the sql.Expression interface.
func (c *ConvexHull) IsNullable() bool {
	return c.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (c *ConvexHull) Type() sql.Type {
	return types.GeometryType{}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*ConvexHull) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 4
}

func (c *ConvexHull) String() string {
	return fmt.Sprintf("%s(%s)", c.FunctionName(), c.Child.String())
}

// WithChildren implements the Expression interface.
func (c *ConvexHull) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewConvexHull(children[0]), nil
}

// collectPoints extracts all points from any geometry type.
func collectPoints(gv types.GeometryValue) []types.Point {
	switch v := gv.(type) {
	case types.Point:
		return []types.Point{v}
	case types.LineString:
		return v.Points
	case types.Polygon:
		var pts []types.Point
		for _, ring := range v.Lines {
			pts = append(pts, ring.Points...)
		}
		return pts
	case types.MultiPoint:
		return v.Points
	case types.MultiLineString:
		var pts []types.Point
		for _, line := range v.Lines {
			pts = append(pts, line.Points...)
		}
		return pts
	case types.MultiPolygon:
		var pts []types.Point
		for _, poly := range v.Polygons {
			for _, ring := range poly.Lines {
				pts = append(pts, ring.Points...)
			}
		}
		return pts
	case types.GeomColl:
		var pts []types.Point
		for _, geom := range v.Geoms {
			pts = append(pts, collectPoints(geom)...)
		}
		return pts
	default:
		return nil
	}
}

// cross2D returns the 2D cross product of vectors OA and OB where O is the origin.
func cross2D(o, a, b types.Point) float64 {
	return (a.X-o.X)*(b.Y-o.Y) - (a.Y-o.Y)*(b.X-o.X)
}

// convexHull computes the convex hull of a set of points using Andrew's monotone chain algorithm.
// Returns the hull points in counter-clockwise order, with the first point repeated at the end.
func convexHull(points []types.Point) []types.Point {
	n := len(points)
	if n == 0 {
		return nil
	}

	// Sort points lexicographically (by X, then by Y)
	sort.Slice(points, func(i, j int) bool {
		if points[i].X != points[j].X {
			return points[i].X < points[j].X
		}
		return points[i].Y < points[j].Y
	})

	// Remove duplicates
	unique := points[:1]
	for i := 1; i < n; i++ {
		if points[i].X != unique[len(unique)-1].X || points[i].Y != unique[len(unique)-1].Y {
			unique = append(unique, points[i])
		}
	}
	points = unique
	n = len(points)

	if n <= 2 {
		return points
	}

	// Build lower hull
	hull := make([]types.Point, 0, 2*n)
	for _, p := range points {
		for len(hull) >= 2 && cross2D(hull[len(hull)-2], hull[len(hull)-1], p) <= 0 {
			hull = hull[:len(hull)-1]
		}
		hull = append(hull, p)
	}

	// Build upper hull
	lower := len(hull) + 1
	for i := n - 2; i >= 0; i-- {
		p := points[i]
		for len(hull) >= lower && cross2D(hull[len(hull)-2], hull[len(hull)-1], p) <= 0 {
			hull = hull[:len(hull)-1]
		}
		hull = append(hull, p)
	}

	return hull
}

// Eval implements the sql.Expression interface.
func (c *ConvexHull) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := c.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	gv, err := types.UnwrapGeometry(ctx, val)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New(c.FunctionName())
	}

	srid := gv.GetSRID()
	points := collectPoints(gv)

	if len(points) == 0 {
		// Empty geometry collection
		return types.GeomColl{SRID: srid, Geoms: []types.GeometryValue{}}, nil
	}

	// Deduplicate and check for degenerate cases
	hull := convexHull(points)

	if len(hull) == 1 {
		return types.Point{SRID: srid, X: hull[0].X, Y: hull[0].Y}, nil
	}

	// Andrew's algorithm returns hull with first point repeated at end.
	// Count distinct points (exclude the closing duplicate).
	distinct := len(hull) - 1
	if distinct <= 0 {
		distinct = len(hull)
	}

	if distinct <= 2 {
		pts := make([]types.Point, distinct)
		for i := 0; i < distinct; i++ {
			pts[i] = types.Point{SRID: srid, X: hull[i].X, Y: hull[i].Y}
		}
		if distinct == 1 {
			return pts[0], nil
		}
		return types.LineString{SRID: srid, Points: pts}, nil
	}

	// General case: the hull forms a polygon
	hullPoints := make([]types.Point, len(hull))
	for i, p := range hull {
		hullPoints[i] = types.Point{SRID: srid, X: p.X, Y: p.Y}
	}

	return types.Polygon{
		SRID: srid,
		Lines: []types.LineString{
			{SRID: srid, Points: hullPoints},
		},
	}, nil
}
