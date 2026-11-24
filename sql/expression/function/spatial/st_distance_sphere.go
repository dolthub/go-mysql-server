// Copyright 2026 Dolthub, Inc.
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
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// DefaultSphereRadius is MySQL's default radius, in meters, for ST_DISTANCE_SPHERE.
const DefaultSphereRadius = 6370986.0

// ErrNonPositiveRadius is returned when ST_DISTANCE_SPHERE is given a radius that is not positive.
var ErrNonPositiveRadius = errors.NewKind("Invalid radius provided to function %s: Radius must be greater than zero.")

// ErrDistanceSphereType is returned when ST_DISTANCE_SPHERE is given anything other than points or multipoints.
var ErrDistanceSphereType = errors.NewKind("%s(%s, %s) has not been implemented for %s spatial reference systems.")

// DistanceSphere is a function that returns the minimum spherical distance between two points or multipoints.
type DistanceSphere struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*DistanceSphere)(nil)
var _ sql.CollationCoercible = (*DistanceSphere)(nil)

// NewDistanceSphere creates a new DistanceSphere expression.
func NewDistanceSphere(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_DISTANCE_SPHERE", "2 or 3", len(args))
	}
	return &DistanceSphere{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (d *DistanceSphere) FunctionName() string {
	return "st_distance_sphere"
}

// Description implements sql.FunctionExpression
func (d *DistanceSphere) Description() string {
	return "returns the minimum spherical distance between two points or multipoints, in meters."
}

// Type implements the sql.Expression interface.
func (d *DistanceSphere) Type(ctx *sql.Context) sql.Type {
	return types.Float64
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*DistanceSphere) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (d *DistanceSphere) String() string {
	var args = make([]string, len(d.ChildExpressions))
	for i, arg := range d.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("%s(%s)", d.FunctionName(), strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (d *DistanceSphere) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewDistanceSphere(ctx, children...)
}

func spherePoints(name string, g types.GeometryValue) ([]types.Point, error) {
	var points []types.Point
	switch g := g.(type) {
	case types.Point:
		points = []types.Point{g}
	case types.MultiPoint:
		points = g.Points
	default:
		return nil, nil
	}
	for _, p := range points {
		if p.X <= -180.0 || p.X > 180.0 {
			return nil, ErrLongitudeOutOfRange.New(p.X, name)
		}
		if p.Y < -90.0 || p.Y > 90.0 {
			return nil, ErrLatitudeOutOfRange.New(p.Y, name)
		}
	}
	return points, nil
}

func geometryTypeName(g types.GeometryValue) string {
	switch g.(type) {
	case types.Point:
		return "POINT"
	case types.LineString:
		return "LINESTRING"
	case types.Polygon:
		return "POLYGON"
	case types.MultiPoint:
		return "MULTIPOINT"
	case types.MultiLineString:
		return "MULTILINESTRING"
	case types.MultiPolygon:
		return "MULTIPOLYGON"
	default:
		return "GEOMCOLLECTION"
	}
}

func srsKind(srid uint32) string {
	if srid == types.GeoSpatialSRID {
		return "geographic"
	}
	return "Cartesian"
}

func haversine(a, b types.Point, radius float64) float64 {
	lat1, lat2 := a.Y*math.Pi/180, b.Y*math.Pi/180
	dLat := lat2 - lat1
	dLon := (b.X - a.X) * math.Pi / 180
	h := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * radius * math.Asin(math.Min(1, math.Sqrt(h)))
}

// Eval implements the sql.Expression interface.
func (d *DistanceSphere) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	g1, err := d.ChildExpressions[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	g2, err := d.ChildExpressions[1].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if g1 == nil || g2 == nil {
		return nil, nil
	}

	geom1, err := types.UnwrapGeometry(ctx, g1)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New(d.FunctionName())
	}
	geom2, err := types.UnwrapGeometry(ctx, g2)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New(d.FunctionName())
	}

	srid1, srid2 := geom1.GetSRID(), geom2.GetSRID()
	if srid1 != srid2 {
		return nil, sql.ErrDiffSRIDs.New(d.FunctionName(), srid1, srid2)
	}
	if srid1 != types.CartesianSRID && srid1 != types.GeoSpatialSRID {
		return nil, sql.ErrUnsupportedSRID.New(srid1)
	}

	radius := DefaultSphereRadius
	if len(d.ChildExpressions) == 3 {
		r, err := d.ChildExpressions[2].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if r == nil {
			return nil, nil
		}
		r, _, err = types.Float64.Convert(ctx, r)
		if err != nil {
			return nil, err
		}
		radius = r.(float64)
		if radius <= 0 {
			return nil, ErrNonPositiveRadius.New(d.FunctionName())
		}
	}

	points1, err := spherePoints(d.FunctionName(), geom1)
	if err != nil {
		return nil, err
	}
	points2, err := spherePoints(d.FunctionName(), geom2)
	if err != nil {
		return nil, err
	}
	if points1 == nil || points2 == nil {
		return nil, ErrDistanceSphereType.New(d.FunctionName(), geometryTypeName(geom1), geometryTypeName(geom2), srsKind(srid1))
	}
	if len(points1) == 0 || len(points2) == 0 {
		return nil, nil
	}

	minDist := math.MaxFloat64
	for _, a := range points1 {
		for _, b := range points2 {
			minDist = math.Min(minDist, haversine(a, b, radius))
		}
	}
	return minDist, nil
}
