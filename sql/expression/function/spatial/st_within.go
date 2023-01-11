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

// orientation returns the orientation of points: a, b, c in that order
// 0 = points are collinear
// 1 = points are clockwise
// 2 = points are counter-clockwise
// Reference: https://www.geeksforgeeks.org/orientation-3-ordered-points/
func orientation(a, b, c sql.Point) int {
	// compare slopes of line(a, b) and line(b, c)
	val := (b.Y-a.Y)*(c.X-b.X) - (b.X-a.X)*(c.Y-b.Y)
	if val > 0 {
		return 1
	} else if val < 0 {
		return 2
	} else {
		return 0
	}
}

func isInBBox(a, b, c sql.Point) bool {
	return c.X >= math.Min(a.X, b.X) &&
		c.X <= math.Max(a.X, b.X) &&
		c.Y >= math.Min(a.Y, b.Y) &&
		c.Y <= math.Max(a.Y, b.Y)
}

func isInStrictBBox(a, b, c sql.Point) bool {
	return c.X > math.Min(a.X, b.X) &&
		c.X < math.Max(a.X, b.X) &&
		c.Y > math.Min(a.Y, b.Y) &&
		c.Y < math.Max(a.Y, b.Y)
}

// Closed LineStrings have no Terminal Points, so will always return false for Closed LineStrings
func isTerminalPoint(p sql.Point, l sql.LineString) bool {
	return !isClosed(l) && (isPointEqual(p, startPoint(l)) || isPointEqual(p, endPoint(l)))
}

// linesIntersect checks if line ab intersects line cd
// Create
// Reference: https://www.geeksforgeeks.org/check-if-two-given-line-segments-intersect/
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
	if abc == 0 && isInBBox(a, b, c) {
		return true
	}
	if abd == 0 && isInBBox(a, b, d) {
		return true
	}
	if cda == 0 && isInBBox(c, d, a) {
		return true
	}
	if cdb == 0 && isInBBox(c, d, b) {
		return true
	}

	return false
}

// isPointWithinClosedLineString checks if a point lies inside a Closed LineString
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
	for i := 1; i < len(l.Points)-1; i++ {
		b = l.Points[i]
		c = l.Points[i+1]

		// TODO: check for perpendicular, horizontal, and vertical?
		if orientation(a, b, c) != 0 {
			points = append(points, b)
			a = b
		}
	}

	if isClosed(l) && len(points) >= 3 && orientation(points[len(points)-1], points[0], points[1]) == 0 {
		points = append(points[1:], points[1])
	} else {
		points = append(points, c)
	}

	return sql.LineString{SRID: l.SRID, Points: points}
}

// isLineSegmentEqual checks if line segment ab is equal to line segment cd
func isLineSegmentEqual(a, b, c, d sql.Point) bool {
	return (isPointEqual(a, c) && isPointEqual(b, d)) ||
		(isPointEqual(a, d) && isPointEqual(b, c))
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

func isPointWithin(p sql.Point, g sql.GeometryValue) bool {
	switch g := g.(type) {
	case sql.Point:
		return isPointEqual(p, g)
	case sql.LineString:
		// Terminal Points of LineStrings are not considered a part of their Interior
		if isTerminalPoint(p, g) {
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
	case sql.Polygon:
		// Points on the Polygon Boundary are not considered part of the Polygon
		// TODO: a simpler, but possibly more compute intensive option is to sum angles, and check if equal to 2pi
		for _, line := range g.Lines {
			if isPointWithin(p, line) {
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
		// Point is considered within MultiPoint if it's equal to at least one Point
		for _, pp := range g.Points {
			if isPointWithin(p, pp) {
				return true
			}
		}
	case sql.MultiLineString:
		// Point is considered within MultiLineString if it is within at least one LineString
		// Edge Case: If point is a terminal point for an odd number of lines,
		//            then it's not within the entire MultiLineString.
		//            This is the case regardless of how many other LineStrings the point is in
		isOddTerminalPoint := false
		for _, l := range g.Lines {
			if isTerminalPoint(p, l) {
				isOddTerminalPoint = !isOddTerminalPoint
			}
		}
		if isOddTerminalPoint {
			return false
		}

		for _, l := range g.Lines {
			if isPointWithin(p, l) {
				return true
			}
		}
	case sql.MultiPolygon:
		// Point is considered within MultiPolygon if it is within at least one Polygon
		for _, poly := range g.Polygons {
			if isPointWithin(p, poly) {
				return true
			}
		}
	case sql.GeomColl:
		// Point is considered within GeomColl if it is within at least one Geometry
		for _, gg := range g.Geoms {
			if isPointWithin(p, gg) {
				return true
			}
		}
	}
	return false
}

func isLineWithin(l sql.LineString, g sql.GeometryValue) bool {
	switch g := g.(type) {
	case sql.Point: // A LineString is never within a Point
	case sql.LineString:
		// A LineString is within a LineString, if its Interior is inside the Interior of g
		// Every line segment of l must be collinear and within the bounding box of at least 1 line segment of g
		// Edge Case: zero length LineStrings at the terminal points have no interior, so can't be within anything
		l2 := simplifyLineString(g)
		for i := 1; i < len(l.Points); i++ {
			c := l.Points[i-1]
			d := l.Points[i]
			isIntersects := false
			for j := 1; j < len(l2.Points); j++ {
				a := l2.Points[j-1]
				b := l2.Points[j]
				if isLineSegmentEqual(a, b, c, d) {
					isIntersects = true
					break
				}
				if orientation(a, b, c) != 0 || orientation(a, b, d) != 0 {
					continue
				}
				if !isInStrictBBox(a, b, c) || !isInStrictBBox(a, b, d) {
					continue
				}
				isIntersects = true
				break
			}
			if !isIntersects {
				return false
			}
		}
		return true
	case sql.Polygon:
		// A LineString is within a Polygon, if
		// 1. at least one point is inside the Polygon
		// 2. any number of points are on the boundaries
		// 3. there are no points on the outside of the polygon

		// check for points on the outside of the polygon
		for _, p := range l.Points {
			for i, line := range g.Lines {
				// it's fine if the point is on the boundary
				if isPointWithin(p, line) {
					continue
				}
				// point is on the outside of polygon
				if i == 0 && !isPointWithinClosedLineString(p, line) {
					return false
				}
				// point is in a hole
				if i > 0 && isPointWithinClosedLineString(p, line) {
					return false
				}
			}
		}

		// TODO: check that there is an interior point
		// all points are either on boundary or inside polygon
		// check that all intersections of line with polygon lines are collinear
		for _, line := range g.Lines {
			l2 := simplifyLineString(line)
			var a, b, c, d sql.Point
			for i := 1; i < len(l.Points); i++ {
				a = l.Points[i-1]
				b = l.Points[i]
				for j := 1; j < len(l2.Points); j++ {
					c = l2.Points[j-1]
					d = l2.Points[j]
					// if one of the end points is in the linestring it's fine
					if !linesIntersect(a, b, c, d) {
						continue
					}
					abc := orientation(a, b, c)
					abd := orientation(a, b, d)
					if abc == 0 && abd == 0 {
						continue
					}

					return false
				}
			}
		}
	case sql.MultiPoint: // A LineString is never within a MultiPoint
	case sql.MultiLineString:
		// A LineString is within a MultiLineString if it is within at least one LineString
		for _, line := range g.Lines {
			if isLineWithin(l, line) {
				return true
			}
		}
	case sql.MultiPolygon:
		// A LineString is within a MultiPolygon if it is within at least one Polygon
		for _, p := range g.Polygons {
			if isLineWithin(l, p) {
				return true
			}
		}
	case sql.GeomColl:
		// A LineString is within a GeomColl if it is within at least one Geometry
		for _, geom := range g.Geoms {
			if isLineWithin(l, geom) {
				return true
			}
		}
	}
	return false
}

func isPolyWithin(p sql.Polygon, g sql.GeometryValue) bool {
	switch g := g.(type) {
	case sql.Point: // A Polygon is never within a Point
	case sql.LineString: // A Polygon is never within a LineString
	case sql.Polygon:
		// TODO: implement me
	case sql.MultiPoint: // A Polygon is never within a MultiPoint
	case sql.MultiLineString: // A Polygon is never within a MultiLineString
	case sql.MultiPolygon:
		// A Polygon is within a MultiPolygon if it is within at least one Polygon
		for _, poly := range g.Polygons {
			if isPolyWithin(p, poly) {
				return true
			}
		}
	case sql.GeomColl:
		// A Polygon is within a GeomColl if it is within at least one Geometry
		for _, geom := range g.Geoms {
			if isPolyWithin(p, geom) {
				return true
			}
		}
	}
	return false
}

// TODO: consider parallelization
func isWithin(g1, g2 sql.GeometryValue) bool {
	switch g1 := g1.(type) {
	case sql.Point:
		return isPointWithin(g1, g2)
	case sql.LineString:
		return isLineWithin(simplifyLineString(g1), g2)
	case sql.Polygon:
		return isPolyWithin(g1, g2)
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
	case sql.MultiLineString:
		// A MultiLineString is within g2 if all LineStrings are within g2
		for _, l := range g1.Lines {
			if !isLineWithin(l, g2) {
				return false
			}
		}
	case sql.MultiPolygon:
		// A MultiPolygon is within g2 if all Polygons are within g2
		for _, p := range g1.Polygons {
			if !isPolyWithin(p, g2) {
				return false
			}
		}
	case sql.GeomColl:
		// A GeomColl is within g2 if all geometries are within g2
		for _, g := range g1.Geoms {
			if !isWithin(g, g2) {
				return false
			}
		}
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

	// Empty GeomColls return nil
	if gc, ok := geom1.(sql.GeomColl); ok && countConcreteGeoms(gc) == 0 {
		return nil, nil
	}
	if gc, ok := geom2.(sql.GeomColl); ok && countConcreteGeoms(gc) == 0 {
		return nil, nil
	}

	return isWithin(geom1, geom2), nil
}
