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
)

// Intersects is a function that returns true if the two geometries intersect
type Intersects struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*Intersects)(nil)

// NewIntersects creates a new Intersects expression.
func NewIntersects(g1, g2 sql.Expression) sql.Expression {
	return &Intersects{
		expression.BinaryExpression{
			Left:  g1,
			Right: g2,
		},
	}
}

// FunctionName implements sql.FunctionExpression
func (i *Intersects) FunctionName() string {
	return "st_intersects"
}

// Description implements sql.FunctionExpression
func (i *Intersects) Description() string {
	return "returns 1 or 0 to indicate whether g1 spatially intersects g2."
}

// Type implements the sql.Expression interface.
func (i *Intersects) Type() sql.Type {
	return sql.Boolean
}

func (i *Intersects) String() string {
	return fmt.Sprintf("%s(%s,%s)", i.FunctionName(), i.Left, i.Right)
}

// WithChildren implements the Expression interface.
func (i *Intersects) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 2)
	}
	return NewWithin(children[0], children[1]), nil
}
// isPointIntersectLine checks if Point p intersects the LineString l
// Note: this will return true if p is a terminal point of l
// Alternatively, we could calculate if dist(ap) + dist(ab) == dist(ap)
func isPointIntersectLine(p sql.Point, l sql.LineString) bool {
	for i := 1; i < len(l.Points); i++ {
		a, b := l.Points[i-1], l.Points[i]
		if isInBBox(a, b, p) && orientation(a, b, p) == 0 {
			return true
		}
	}
	return false
}

// isPointIntersectPolyBoundary checks if Point p intersects the Polygon boundary
func isPointIntersectPolyBoundary(point sql.Point, poly sql.Polygon) bool {
	for _, line := range poly.Lines {
		if isPointIntersectLine(point, line) {
			return true
		}
	}
	return false
}

// isPointIntersectPolyInterior checks if a Point p intersects the Polygon Interior
// Point outside the first LineString is not in Polygon Interior
// Point inside the other LineStrings is not in Polygon Interior
func isPointIntersectPolyInterior(point sql.Point, poly sql.Polygon) bool {
	if !isPointWithinClosedLineString(point, poly.Lines[0]) {
		return false
	}
	for i := 1; i < len(poly.Lines); i++ {
		if isPointWithinClosedLineString(point, poly.Lines[i]) {
			return false
		}
	}
	return true
}

func isPointIntersectMultiPoint(point sql.Point, multiPoint sql.MultiPoint) bool {
	for _, p := range multiPoint.Points {
		if isPointEqual(point, p) {
			return true
		}
	}
	return false
}

func isPointIntersects(p sql.Point, g sql.GeometryValue) bool {
	switch g := g.(type) {
	case sql.Point:
		return isPointEqual(p, g)
	case sql.LineString:
		return isPointIntersectLine(p, g)
	case sql.Polygon:
		return isPointIntersectPolyBoundary(p, g) || isPointIntersectPolyInterior(p, g)
	case sql.MultiPoint:
		for _, pp := range g.Points {
			if isPointWithin(p, pp) {
				return true
			}
		}
	case sql.MultiLineString:
		for _, l := range g.Lines {
			if isPointIntersects(p, l) {
				return true
			}
		}
	case sql.MultiPolygon:
		for _, pp := range g.Polygons {
			if isPointIntersects(p, pp) {
				return true
			}
		}
	case sql.GeomColl:
		for _, gg := range g.Geoms {
			if isPointIntersects(p, gg) {
				return true
			}
		}
	}
	return false
}

// linesIntersect checks if line ab intersects line cd
// Edge case for collinear points is to check if they are within the bounding box
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

func isLineIntersectLine(l1, l2 sql.LineString) bool {
	for i := 1; i < len(l1.Points); i++ {
		for j := 1; j < len(l2.Points); j++ {
			if linesIntersect(l1.Points[i-1], l1.Points[i], l2.Points[j-1], l2.Points[j]) {
				return true
			}
		}
	}
	return false
}

func isLineIntersects(l sql.LineString, g sql.GeometryValue) bool {
	switch g := g.(type) {
	case sql.Point:
		return isPointIntersects(g, l)
	case sql.LineString:
		return isLineIntersectLine(l, g)
	case sql.Polygon:
		if isLineIntersectLine()
	}
	return false
}

func isIntersects(g1, g2 sql.GeometryValue) bool {
	switch g1 := g1.(type) {
	case sql.Point:
		return isPointIntersects(g1, g2)
	case sql.LineString:
	case sql.Polygon:
	case sql.MultiPoint:
	case sql.MultiLineString:
	case sql.MultiPolygon:
	case sql.GeomColl:
		// TODO (james): implement these
	}
	return true
}

// Eval implements the sql.Expression interface.
func (i *Intersects) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	g1, err := i.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	g2, err := i.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if g1 == nil || g2 == nil {
		return nil, nil
	}

	var geom1, geom2 sql.GeometryValue
	var ok bool
	geom1, ok = g1.(sql.GeometryValue)
	if !ok {
		return nil, sql.ErrInvalidGISData.New(i.FunctionName())
	}
	geom2, ok = g2.(sql.GeometryValue)
	if !ok {
		return nil, sql.ErrInvalidGISData.New(i.FunctionName())
	}

	if geom1.GetSRID() != geom2.GetSRID() {
		return nil, sql.ErrDiffSRIDs.New(i.FunctionName(), geom1.GetSRID(), geom2.GetSRID())
	}

	// Empty GeomColls return nil
	if gc, ok := geom1.(sql.GeomColl); ok && countConcreteGeoms(gc) == 0 {
		return nil, nil
	}
	if gc, ok := geom2.(sql.GeomColl); ok && countConcreteGeoms(gc) == 0 {
		return nil, nil
	}

	// TODO (james): remove this switch block when the other comparisons are implemented
	switch geom1.(type) {
	case sql.LineString:
		return nil, sql.ErrUnsupportedGISTypeForSpatialFunc.New("LineString", i.FunctionName())
	case sql.Polygon:
		return nil, sql.ErrUnsupportedGISTypeForSpatialFunc.New("Polygon", i.FunctionName())
	case sql.MultiPoint:
		return nil, sql.ErrUnsupportedGISTypeForSpatialFunc.New("MultiPoint", i.FunctionName())
	case sql.MultiLineString:
		return nil, sql.ErrUnsupportedGISTypeForSpatialFunc.New("MultiLineString", i.FunctionName())
	case sql.MultiPolygon:
		return nil, sql.ErrUnsupportedGISTypeForSpatialFunc.New("MultiPolygon", i.FunctionName())
	case sql.GeomColl:
		return nil, sql.ErrUnsupportedGISTypeForSpatialFunc.New("GeomColl", i.FunctionName())
	}

	return isWithin(geom1, geom2), nil
}
