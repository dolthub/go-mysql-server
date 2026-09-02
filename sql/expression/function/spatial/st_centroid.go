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
	"math"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Centroid is a function that returns the mathematical centroid of a geometry.
type Centroid struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*Centroid)(nil)
var _ sql.CollationCoercible = (*Centroid)(nil)

// NewCentroid creates a new Centroid expression.
func NewCentroid(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &Centroid{expression.UnaryExpressionStub{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (c *Centroid) FunctionName() string {
	return "st_centroid"
}

// Description implements sql.FunctionExpression
func (c *Centroid) Description() string {
	return "returns the mathematical centroid for the geometry value."
}

// IsNullable implements the sql.Expression interface.
func (c *Centroid) IsNullable(ctx *sql.Context) bool {
	return c.Child.IsNullable(ctx)
}

// Type implements the sql.Expression interface.
func (c *Centroid) Type(ctx *sql.Context) sql.Type {
	return types.PointType{}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Centroid) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 4
}

func (c *Centroid) String() string {
	return fmt.Sprintf("%s(%s)", c.FunctionName(), c.Child.String())
}

// WithChildren implements the Expression interface.
func (c *Centroid) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return NewCentroid(ctx, children[0]), nil
}

// ringCentroidAndArea computes the centroid and signed area of a polygon ring using the shoelace formula.
func ringCentroidAndArea(ring types.LineString) (cx, cy, area float64) {
	n := len(ring.Points)
	if n < 3 {
		return 0, 0, 0
	}
	for i := 0; i < n-1; i++ {
		xi := ring.Points[i].X
		yi := ring.Points[i].Y
		xj := ring.Points[i+1].X
		yj := ring.Points[i+1].Y
		cross := xi*yj - xj*yi
		area += cross
		cx += (xi + xj) * cross
		cy += (yi + yj) * cross
	}
	area /= 2
	if area != 0 {
		cx /= (6 * area)
		cy /= (6 * area)
	}
	return cx, cy, area
}

// Eval implements the sql.Expression interface.
func (c *Centroid) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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

	switch v := gv.(type) {
	case types.Point:
		return v, nil

	case types.LineString:
		if len(v.Points) == 0 {
			return types.Point{SRID: srid}, nil
		}
		// Centroid of a linestring: weighted average by segment length
		var totalLen, cx, cy float64
		for i := 0; i < len(v.Points)-1; i++ {
			p1 := v.Points[i]
			p2 := v.Points[i+1]
			dx := p2.X - p1.X
			dy := p2.Y - p1.Y
			segLen := dx*dx + dy*dy // squared is fine for weight ratio
			if segLen > 0 {
				segLen = sqrt(segLen)
			}
			cx += (p1.X + p2.X) / 2 * segLen
			cy += (p1.Y + p2.Y) / 2 * segLen
			totalLen += segLen
		}
		if totalLen == 0 {
			return v.Points[0], nil
		}
		return types.Point{SRID: srid, X: cx / totalLen, Y: cy / totalLen}, nil

	case types.Polygon:
		if len(v.Lines) == 0 {
			return types.Point{SRID: srid}, nil
		}
		// Use the signed-area centroid formula for the exterior ring,
		// then subtract contributions from holes.
		cx, cy, area := ringCentroidAndArea(v.Lines[0])
		totalArea := area
		weightedX := cx * area
		weightedY := cy * area
		for i := 1; i < len(v.Lines); i++ {
			hx, hy, ha := ringCentroidAndArea(v.Lines[i])
			// Holes have opposite-sign area; subtract their contribution
			totalArea += ha
			weightedX += hx * ha
			weightedY += hy * ha
		}
		if totalArea == 0 {
			return types.Point{SRID: srid, X: cx, Y: cy}, nil
		}
		return types.Point{SRID: srid, X: weightedX / totalArea, Y: weightedY / totalArea}, nil

	case types.MultiPoint:
		if len(v.Points) == 0 {
			return types.Point{SRID: srid}, nil
		}
		var cx, cy float64
		for _, p := range v.Points {
			cx += p.X
			cy += p.Y
		}
		n := float64(len(v.Points))
		return types.Point{SRID: srid, X: cx / n, Y: cy / n}, nil

	case types.MultiLineString:
		var totalLen, cx, cy float64
		for _, line := range v.Lines {
			for i := 0; i < len(line.Points)-1; i++ {
				p1 := line.Points[i]
				p2 := line.Points[i+1]
				dx := p2.X - p1.X
				dy := p2.Y - p1.Y
				segLen := sqrt(dx*dx + dy*dy)
				cx += (p1.X + p2.X) / 2 * segLen
				cy += (p1.Y + p2.Y) / 2 * segLen
				totalLen += segLen
			}
		}
		if totalLen == 0 {
			return types.Point{SRID: srid}, nil
		}
		return types.Point{SRID: srid, X: cx / totalLen, Y: cy / totalLen}, nil

	case types.MultiPolygon:
		var totalArea, weightedX, weightedY float64
		for _, poly := range v.Polygons {
			if len(poly.Lines) == 0 {
				continue
			}
			cx, cy, area := ringCentroidAndArea(poly.Lines[0])
			polyArea := area
			wx := cx * area
			wy := cy * area
			for i := 1; i < len(poly.Lines); i++ {
				hx, hy, ha := ringCentroidAndArea(poly.Lines[i])
				polyArea += ha
				wx += hx * ha
				wy += hy * ha
			}
			totalArea += polyArea
			weightedX += wx
			weightedY += wy
		}
		if totalArea == 0 {
			return types.Point{SRID: srid}, nil
		}
		return types.Point{SRID: srid, X: weightedX / totalArea, Y: weightedY / totalArea}, nil

	case types.GeomColl:
		if len(v.Geoms) == 0 {
			return types.Point{SRID: srid}, nil
		}
		// For geometry collections, MySQL computes centroid as average of all component centroids
		var cx, cy float64
		var n float64
		for _, geom := range v.Geoms {
			// Recursively get centroid of each component
			subResult, err := c.evalGeomCentroid(ctx, geom)
			if err != nil {
				return nil, err
			}
			if subResult != nil {
				cx += subResult.X
				cy += subResult.Y
				n++
			}
		}
		if n == 0 {
			return types.Point{SRID: srid}, nil
		}
		return types.Point{SRID: srid, X: cx / n, Y: cy / n}, nil

	default:
		return nil, sql.ErrInvalidGISData.New(c.FunctionName())
	}
}

// evalGeomCentroid is a helper to compute the centroid of a single geometry value.
func (c *Centroid) evalGeomCentroid(ctx *sql.Context, gv types.GeometryValue) (*types.Point, error) {
	result, err := NewCentroid(ctx, nil).(*Centroid).evalRaw(ctx, gv)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	p := result.(types.Point)
	return &p, nil
}

// evalRaw computes the centroid for an already-unwrapped geometry value.
func (c *Centroid) evalRaw(ctx *sql.Context, gv types.GeometryValue) (interface{}, error) {
	srid := gv.GetSRID()
	switch v := gv.(type) {
	case types.Point:
		return v, nil
	case types.LineString:
		if len(v.Points) == 0 {
			return types.Point{SRID: srid}, nil
		}
		var totalLen, cx, cy float64
		for i := 0; i < len(v.Points)-1; i++ {
			p1 := v.Points[i]
			p2 := v.Points[i+1]
			dx := p2.X - p1.X
			dy := p2.Y - p1.Y
			segLen := sqrt(dx*dx + dy*dy)
			cx += (p1.X + p2.X) / 2 * segLen
			cy += (p1.Y + p2.Y) / 2 * segLen
			totalLen += segLen
		}
		if totalLen == 0 {
			return v.Points[0], nil
		}
		return types.Point{SRID: srid, X: cx / totalLen, Y: cy / totalLen}, nil
	case types.Polygon:
		if len(v.Lines) == 0 {
			return types.Point{SRID: srid}, nil
		}
		cx, cy, _ := ringCentroidAndArea(v.Lines[0])
		return types.Point{SRID: srid, X: cx, Y: cy}, nil
	default:
		return types.Point{SRID: srid}, nil
	}
}

func sqrt(x float64) float64 {
	return math.Sqrt(x)
}
