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
			Left: g1,
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

func calcUnsquaredDist(a, b sql.Point) float64 {
	dx := b.X - a.X
	dy := b.Y - a.Y
	return dx * dx + dy * dy
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

func countIntersections(a, b sql.Point, line sql.LineString) int {
	for i := 0; i < len(line.Points) - 1; i++ {

	}
}

func isPointWithin(p sql.Point, g sql.GeometryValue) bool {
	switch g := g.(type) {
	case sql.Point:
		return p.X == g.X && p.Y == g.Y
	case sql.LineString:
		// TODO: perform distance check
		// TODO: alternatively could perform cross product and bounds check
		// for each line segment, check if point lies within it
		for i := 0; i < len(g.Points) - 1; i++ {
			totalDist := calcUnsquaredDist(g.Points[i], g.Points[i+1])
			dist1 := calcUnsquaredDist(p, g.Points[i])
			dist2 := calcUnsquaredDist(p, g.Points[i+1])
			if dist1 + dist2 == totalDist {
				return true
			}
		}
		return false
	case sql.Polygon:
		// TODO: draw a ray from point outwards, if it crosses an odd number of line segments, it's inside
		// TODO: watch out for corners; check what MySQL does

		outerLine := g.Lines[0]
		vals := FindBBox(outerLine) // minX, minY, maxX, maxY
		q := sql.Point{SRID: p.SRID, X: vals[2], Y: vals[3]}
		intersectCount := countIntersections(p, q, outerLine)
		if intersectCount % 2 == 0 {
			return false
		}
		// TODO: check if it's one of the holes

		return false
	default:
		return false
	}
}

func isWithin(g1, g2 sql.GeometryValue) bool {
	// TODO: implement a bunch of combination of comparisons
	// TODO: point v point easy
	// TODO: point v linestring somewhat easy
	// TODO: point v polygon somewhat easy
	// TODO: come up with some generalization...might not be possible :/
	var ok1, ok2 bool
	p1, ok1 := g1.(sql.Point)
	p2, ok2 := g2.(sql.Point)


	// TODO: g1.GetGeomType() < g2.GetGeomType() except for the case of geometrycollection


	if ok1 && ok2 {
		return
	}

}

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

	if geom1.GetGeomType() != geom2.GetGeomType() {
		return 0, nil
	}

	if geom1.GetSRID() != geom2.GetSRID() {
		return nil, sql.ErrDiffSRIDs.New(w.FunctionName(), geom1.GetSRID(), geom2.GetSRID())
	}

	return isWithin(), nil

	return nil, nil
}
