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
	"math"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Within is a function that true if left is spatially within right
type Within struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*Within)(nil)

// NewWithin creates a new Within expression.
func NewWithin(g1, g2 sql.Expression) sql.Expression {
	return &Within{
		expression.BinaryExpression{
			Left:  g1,
			Right: g2,
		},
	}
}

// FunctionName implements sql.FunctionExpression
func (w *Within) FunctionName() string {
	return "st_within"
}

// Description implements sql.FunctionExpression
func (w *Within) Description() string {
	return "returns 1 or 0 to indicate whether g1 is spatially within g2. This tests the opposite relationship as st_contains()."
}

// Type implements the sql.Expression interface.
func (w *Within) Type() sql.Type {
	return sql.Boolean
}

func (w *Within) String() string {
	return fmt.Sprintf("%s(%s,%s)", w.FunctionName(), w.Left, w.Right)
}

// WithChildren implements the Expression interface.
func (w *Within) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(w, len(children), 2)
	}
	return NewWithin(children[0], children[1]), nil
}

// given 3 collinear points, check if they are intersecting
func collinearIntersect(a, b, c sql.Point) bool {
	return c.X > math.Min(a.X, b.X) && c.X < math.Max(a.X, b.X) && c.Y > math.Min(a.Y, b.Y) && c.Y < math.Max(a.Y, b.Y)
}

// TODO: https://www.geeksforgeeks.org/orientation-3-ordered-points/
// checks the orientation of points a, b, c
func orientation(a, b, c sql.Point) int {
	// compare slopes of line(a, b) and line(b, c)
	val := (b.Y-a.Y)*(c.X-b.X) - (b.X-a.X)*(c.Y-b.Y)
	// check orientation
	if val > 0 {
		return 1 // clockwise
	} else if val < 0 {
		return 2 // counter-clockwise
	} else {
		return 0 // collinear or both on axis and perpendicular
	}
}

// TODO: https://www.geeksforgeeks.org/check-if-two-given-line-segments-intersect/
// linesIntersect checks if line ab intersects line cd
func linesIntersect(a, b, c, d sql.Point) bool {
	abc := orientation(a, b, c)
	abd := orientation(a, b, d)
	cda := orientation(c, d, a)
	cdb := orientation(c, d, b)

	// different orientations mean they intersect
	if (abc != abd) && (cda != cdb) {
		return true
	}

	// if orientation is collinear, check if point is inside segment
	if abc == 0 && collinearIntersect(a, b, c) {
		return true
	}
	if abd == 0 && collinearIntersect(a, b, d) {
		return true
	}
	if cda == 0 && collinearIntersect(c, d, a) {
		return true
	}
	if cdb == 0 && collinearIntersect(c, d, b) {
		return true
	}

	// no intersection
	return false
}

// TODO: many of these functions are used in rasterization, so they can be parallelized

func pointsEqual(a, b sql.Point) bool {
	return a.X == b.X && a.Y == b.Y
}

func isInBBox(a, b, c sql.Point) bool {
	return c.X >= math.Min(a.X, b.X) &&
		c.X <= math.Max(a.X, b.X) &&
		c.Y >= math.Min(a.Y, b.Y) &&
		c.Y <= math.Max(a.Y, b.Y)
}

// isPointWithin checks if sql.Point p is within geometry g
func isPointWithin(p sql.Point, g sql.GeometryValue) bool {
	switch g := g.(type) {
	case sql.Point:
		return pointsEqual(p, g)
	case sql.LineString:
		// closed LineStrings contain their terminal points, and terminal points are not within linestring
		if !isClosed(g) && (pointsEqual(p, startPoint(g)) || pointsEqual(p, endPoint(g))) {
			return false
		}

		// alternatively, we could calculate if dist(ap) + dist(ab) == dist(ap)
		for i := 1; i < len(g.Points); i++ {
			a, b := g.Points[i-1], g.Points[i]
			if !isInBBox(a, b, p) {
				continue
			}
			if orientation(a, b, p) != 0 {
				continue
			}
			return true
		}
		return false
	case sql.Polygon:
		// TODO: a simpler, but possible more compute intensive option is to sum angles, and check if equal to 2pi
		// Draw a horizontal ray starting at p to infinity, and count number of line segments it intersects.
		// Handle special edge cases for crossing with vertexes.
		for _, line := range g.Lines {
			if isPointWithin(p, line) {
				return false
			}
		}
		// TODO: check holes in polygon
		// TODO: combine all linestrings into one?
		numInters := 0
		outerLine := g.Lines[0]
		for i := 0; i < len(outerLine.Points)-1; i++ {
			a := outerLine.Points[i]
			b := outerLine.Points[i+1]
			// ignore horizontal line segments
			if a.Y == b.Y {
				continue
			}
			// p is either above or below line segment, will never intersect
			// we use >, but not >= for Max, because of vertex intersections
			if p.Y <= math.Min(a.Y, b.Y) || p.Y > math.Max(a.Y, b.Y) {
				continue
			}
			// p is to the right of entire line segment, will never intersect
			if p.X >= math.Max(a.X, b.X) {
				continue
			}
			numInters += 1
		}
		return numInters%2 == 1
	case sql.MultiPoint:
		for _, pp := range g.Points {
			if isPointWithin(p, pp) {
				return true
			}
		}
		return false
	case sql.MultiLineString:
		return false
	case sql.MultiPolygon:
		return false
	case sql.GeomColl:
		for _, gg := range g.Geoms {
			if isPointWithin(p, gg) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func isLineWithin(l sql.LineString, g sql.GeometryValue) bool {
	switch g := g.(type) {
	case sql.Point:
		g=g
		return false
	case sql.LineString:
		return false
	default:
		return false
	}
}

// want to verify that all points of g1 are within g2
func isWithin(g1, g2 sql.GeometryValue) bool {
	// TODO: implement a bunch of combination of comparisons
	// TODO: point v point easy
	// TODO: point v linestring somewhat easy
	// TODO: point v polygon somewhat easy
	// TODO: come up with some generalization...might not be possible :/
	// TODO: g1.GetGeomType() < g2.GetGeomType() except for the case of geometrycollection
	switch g1 := g1.(type) {
	case sql.Point:
		return isPointWithin(g1, g2)
	case sql.LineString:
		return false
	case sql.MultiPoint:
		checked := map[sql.Point]bool{}
		for _, p := range g1.Points {
			if checked[p] {
				continue
			}
			checked[p] = true
			if !isPointWithin(p, g2) {
				return false
			}
		}
		return false
	default:
		return false
	}
	return true
}

// For geometry A to be within geometry B:
// 1. The interior of A and interior of B must intersect
// 2. The interior of A and exterior of B must NOT intersect
// 3. The boundary of A and exterior of B must NOT intersect

// For points: interior and boundary are the same
// For lines: boundary are the end points, closed lines have no boundary
// For polygons: boundary are the lines (including end points)

// There's this thing called the DE9-IM? And it's useful apparently

// Eval implements the sql.Expression interface.
func (w *Within) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	g1, err := w.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	g2, err := w.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if g1 == nil || g2 == nil {
		return nil, nil
	}

	// TODO: convert this to helper method validateGeometryValue
	var geom1, geom2 sql.GeometryValue
	var ok bool
	geom1, ok = g1.(sql.GeometryValue)
	if !ok {
		return nil, sql.ErrInvalidGISData.New(w.FunctionName())
	}
	geom2, ok = g2.(sql.GeometryValue)
	if !ok {
		return nil, sql.ErrInvalidGISData.New(w.FunctionName())
	}

	if geom1.GetSRID() != geom2.GetSRID() {
		return nil, sql.ErrDiffSRIDs.New(w.FunctionName(), geom1.GetSRID(), geom2.GetSRID())
	}

	return isWithin(geom1, geom2), nil
}
