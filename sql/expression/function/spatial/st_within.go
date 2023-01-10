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

// isPointWithinClosedLineString checks if a point lies inside of a Closed LineString
func isPointWithinClosedLineString(p sql.Point, l sql.LineString) bool {
	numInters := 0
	for i := 1; i < len(l.Points); i++ {
		a := l.Points[i-1]
		b := l.Points[i]
		// ignore horizontal line segments
		if a.Y == b.Y {
			continue
		}
		// p is either above or below line segment, will never intersect
		// we use >, but not >= for max, because of vertex intersections
		if p.Y <= math.Min(a.Y, b.Y) || p.Y > math.Max(a.Y, b.Y) {
			continue
		}
		// p is to the right of entire line segment, will never intersect
		if p.X >= math.Max(a.X, b.X) {
			continue
		}
		q := sql.Point{X: math.Max(a.X, b.X), Y: p.Y}
		if !linesIntersect(a, b, p, q) {
			continue
		}
		numInters += 1
	}
	return numInters%2 == 1
}

// isPointWithin checks if sql.Point p is within geometry g
func isPointWithin(p sql.Point, g sql.GeometryValue) bool {
	switch g := g.(type) {
	case sql.Point:
		return pointsEqual(p, g)
	case sql.LineString:
		// Closed LineStrings contain their terminal points, and terminal points are not within linestring
		if !isClosed(g) && (pointsEqual(p, startPoint(g)) || pointsEqual(p, endPoint(g))) {
			return false
		}
		// Alternatively, we could calculate if dist(ap) + dist(ab) == dist(ap)
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
		// Points on the Polygon Boundary are not considered part of the Polygon
		for _, line := range g.Lines {
			if isPointWithin(p, line){
				return false
			}
		}
		outerLine := g.Lines[0]
		if !isPointWithinClosedLineString(p, outerLine) {
			return false
		}
		// Points in the holes of Polygon are outside of Polygon
		for i := 1; i < len(g.Lines); i++ {
			if isPointWithinClosedLineString(p, g.Lines[i]) {
				return false
			}
		}
		return true
	case sql.MultiPoint:
		// Point is considered within MultiPoint if it is within at least one Point
		for _, pp := range g.Points {
			if isPointWithin(p, pp) {
				return true
			}
		}
		return false
	case sql.MultiLineString:
		// Point is considered within MultiLineString if it is within at least one LineString
		for _, line := range g.Lines {
			// Edge Case: if the point is any of the terminal points, it's not in the entire MultiLineString
			if !isClosed(line) && (pointsEqual(p, startPoint(line)) || pointsEqual(p, endPoint(line))) {
				return false
			}
			if isPointWithin(p, line) {
				return true
			}
		}
		return false
	case sql.MultiPolygon:
		// Point is considered within MultiPolygon if it is within at least one Polygon
		for _, poly := range g.Polygons {
			if isPointWithin(p, poly) {
				return true
			}
		}
		return false
	case sql.GeomColl:
		// Point is considered within GeometryCollection if it is within at least one Geometry
		for _, gg := range g.Geoms {
			if isPointWithin(p, gg){
				return true
			}
		}
		return false
	default:
		return false
	}
}

// simplifyLineString condenses a LineString into its simplest line segments
func simplifyLineString(l sql.LineString) sql.LineString {
	// smallest possible point, closed or not
	if len(l.Points) == 2 {
		return l
	}

	// smallest possible closed point
	closed := isClosed(l)
	if closed && len(l.Points) == 3 {
		return l
	}

	var a, b, c sql.Point
	a = l.Points[0]
	points := []sql.Point{a}
	for i := 1; i < len(l.Points) - 1; i++ {
		b = l.Points[i]
		c = l.Points[i+1]

		// TODO: check for perpendicular, horizontal, and vertical?
		if orientation(a, b, c) != 0 {
			points = append(points, b)
			a = b
		}
	}

	if isClosed(l) && len(points) > 3 {
		if orientation(points[len(points)-1], points[0], points[1]) == 0 {
			points = append(points[1:], points[1])
		}
	} else {
		points = append(points, c)
	}

	return sql.LineString{SRID: l.SRID, Points: points}
}

func isLineWithin(l sql.LineString, g sql.GeometryValue) bool {
	switch g := g.(type) {
	case sql.Point:
		// A LineString is never within a Point
		return false
	case sql.LineString:
		// A LineString is within g, if it's Boundary and Interior are both inside the Interior of g
		// So, every line segment of l and its terminal points, must be within at least 1 line segment of g
		l1 := simplifyLineString(l)
		l2 := simplifyLineString(g)
		for i := 1; i < len(l1.Points); i++ {
			c := l1.Points[i-1]
			d := l1.Points[i]
			isIntersects := false
			for j := 1; j < len(l2.Points); j++ {
				a := l2.Points[j-1]
				b := l2.Points[j]
				if orientation(a, b, c) != 0 || orientation(a, b, d) != 0 {
					continue
				}
				if !collinearIntersect(a, b, c) || !collinearIntersect(a, b, d) {
					continue
				}
				isIntersects = true
			}
			if !isIntersects {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// countConcreteGeomes recursively counts all the GeomTypes that are not GeomColl inside a GeomColl
func countConcreteGeoms(gc sql.GeomColl) int {
	count := 0
	for _, g := range gc.Geoms {
		if innerGC, ok := g.(sql.GeomColl); ok {
			count += countConcreteGeoms(innerGC)
		}
		count++
	}
	return count
}

// want to verify that all points of g1 are within g2
func isWithin(g1, g2 sql.GeometryValue) bool {
	// TODO: g1.GetGeomType() < g2.GetGeomType() except for the case of geometrycollection
	switch g1 := g1.(type) {
	case sql.Point:
		return isPointWithin(g1, g2)
	case sql.LineString:
		return isLineWithin(g1, g2)
	case sql.MultiPoint:
		// A MultiPoint is considered within g2 if all points are within g2
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
		return true
	case sql.GeomColl:
		// A GeomColl is within g2 if all geometries are within g2
		for _, g := range g1.Geoms {
			if !isWithin(g, g2) {
				return false
			}
		}
		return true
	default:
		return false
	}
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

	// Empty GeomColls return nil
	if gc, ok := geom1.(sql.GeomColl); ok && countConcreteGeoms(gc) == 0 {
		return nil, nil
	}
	if gc, ok := geom2.(sql.GeomColl); ok && countConcreteGeoms(gc) == 0 {
		return nil, nil
	}

	return isWithin(geom1, geom2), nil
}
