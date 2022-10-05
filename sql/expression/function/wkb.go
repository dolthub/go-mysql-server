// Copyright 2020-2021 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// AsWKB is a function that converts a spatial type into WKB format (alias for AsBinary)
type AsWKB struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*AsWKB)(nil)

// NewAsWKB creates a new point expression.
func NewAsWKB(e sql.Expression) sql.Expression {
	return &AsWKB{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (a *AsWKB) FunctionName() string {
	return "st_aswkb"
}

// Description implements sql.FunctionExpression
func (a *AsWKB) Description() string {
	return "returns binary representation of given spatial type."
}

// IsNullable implements the sql.Expression interface.
func (a *AsWKB) IsNullable() bool {
	return a.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (a *AsWKB) Type() sql.Type {
	return sql.LongBlob
}

func (a *AsWKB) String() string {
	return fmt.Sprintf("ST_ASWKB(%s)", a.Child.String())
}

// WithChildren implements the Expression interface.
func (a *AsWKB) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}
	return NewAsWKB(children[0]), nil
}

// Eval implements the sql.Expression interface.
func (a *AsWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := a.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	switch v := val.(type) {
	case sql.Point:
		return sql.SerializePoint(v)[sql.SRIDSize:], nil
	case sql.LineString:
		return sql.SerializeLineString(v)[sql.SRIDSize:], nil
	case sql.Polygon:
		return sql.SerializePolygon(v)[sql.SRIDSize:], nil
	case sql.MultiPoint:
		return sql.SerializeMultiPoint(v)[sql.SRIDSize:], nil
	default:
		return nil, sql.ErrInvalidGISData.New(a.FunctionName())
	}
}

// GeomFromWKB is a function that returns a geometry type from a WKB byte array
type GeomFromWKB struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*GeomFromWKB)(nil)

// NewGeomFromWKB creates a new geometry expression.
func NewGeomFromWKB(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_GEOMFROMWKB", "1, 2, or 3", len(args))
	}
	return &GeomFromWKB{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (g *GeomFromWKB) FunctionName() string {
	return "st_geomfromwkb"
}

// Description implements sql.FunctionExpression
func (g *GeomFromWKB) Description() string {
	return "returns a new geometry from a WKB string."
}

// Type implements the sql.Expression interface.
func (g *GeomFromWKB) Type() sql.Type {
	return sql.PointType{} // TODO: replace with generic geometry type
}

func (g *GeomFromWKB) String() string {
	var args = make([]string, len(g.ChildExpressions))
	for i, arg := range g.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_GEOMFROMWKB(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (g *GeomFromWKB) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewGeomFromWKB(children...)
}

// ParseAxisOrder takes in a key, value string and determines the order of the xy coords
func ParseAxisOrder(s string) (bool, error) {
	s = strings.ToLower(s)
	s = strings.TrimSpace(s)
	switch s {
	case "axis-order=long-lat":
		return true, nil
	case "axis-order=lat-long", "axis-order=srid-defined":
		return false, nil
	default:
		return false, sql.ErrInvalidArgument.New()
	}
}

func ValidateSRID(srid uint32) error {
	if srid != sql.CartesianSRID && srid != sql.GeoSpatialSRID {
		return ErrInvalidSRID.New(srid)
	}
	return nil
}

// EvalGeomFromWKB takes in arguments for the ST_FROMWKB functions, and parses them to their correspoding geometry type
func EvalGeomFromWKB(ctx *sql.Context, row sql.Row, exprs []sql.Expression, expectedGeomType int) (interface{}, error) {
	val, err := exprs[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	buf, ok := val.([]byte)
	if !ok {
		return nil, sql.ErrInvalidGISData.New()
	}

	isBig, geomType, err := sql.ParseWKBHeader(buf)
	if err != nil {
		return nil, err
	}
	buf = buf[sql.WKBHeaderSize:]

	if expectedGeomType != sql.WKBUnknown && int(geomType) != expectedGeomType {
		return nil, sql.ErrInvalidGISData.New()
	}

	srid := uint32(0)
	if len(exprs) >= 2 {
		s, err := exprs[1].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, nil
		}
		s, err = sql.Uint32.Convert(s)
		if err != nil {
			return nil, err
		}
		srid = s.(uint32)
	}
	if err = ValidateSRID(srid); err != nil {
		return nil, err
	}

	var geom sql.GeometryValue
	switch geomType {
	case sql.WKBPointID:
		geom, err = sql.WKBToPoint(buf, isBig, srid)
	case sql.WKBLineID:
		geom, err = sql.WKBToLine(buf, isBig, srid)
	case sql.WKBPolyID:
		geom, err = sql.WKBToPoly(buf, isBig, srid)
	case sql.WKBMultiPointID:
		geom, err = sql.WKBToMultiPoint(buf, isBig, srid)
	// TODO: add multi geometries here
	default:
		return nil, sql.ErrInvalidGISData.New()
	}
	if err != nil {
		return nil, err
	}

	order := false
	if len(exprs) == 3 {
		o, err := exprs[2].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if o == nil {
			return nil, nil
		}
		order, err = ParseAxisOrder(o.(string))
		if err != nil {
			return nil, sql.ErrInvalidArgument.New()
		}
	}
	if order {
		geom = SwapGeometryXY(geom)
	}

	return geom, nil
}

// Eval implements the sql.Expression interface.
func (g *GeomFromWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	geom, err := EvalGeomFromWKB(ctx, row, g.ChildExpressions, sql.WKBUnknown)
	if sql.ErrInvalidGISData.Is(err) {
		return nil, sql.ErrInvalidGISData.New(g.FunctionName())
	}
	return geom, err
}

// PointFromWKB is a function that returns a point type from a WKB byte array
type PointFromWKB struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*PointFromWKB)(nil)

// NewPointFromWKB creates a new point expression.
func NewPointFromWKB(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 && len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_POINTFROMWKB", "1, 2, or 3", len(args))
	}
	return &PointFromWKB{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (p *PointFromWKB) FunctionName() string {
	return "st_pointfromwkb"
}

// Description implements sql.FunctionExpression
func (p *PointFromWKB) Description() string {
	return "returns a new point from WKB format."
}

// Type implements the sql.Expression interface.
func (p *PointFromWKB) Type() sql.Type {
	return sql.PointType{}
}

func (p *PointFromWKB) String() string {
	var args = make([]string, len(p.ChildExpressions))
	for i, arg := range p.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_POINTFROMWKB(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (p *PointFromWKB) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewPointFromWKB(children...)
}

// Eval implements the sql.Expression interface.
func (p *PointFromWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	point, err := EvalGeomFromWKB(ctx, row, p.ChildExpressions, sql.WKBPointID)
	if sql.ErrInvalidGISData.Is(err) {
		return nil, sql.ErrInvalidGISData.New(p.FunctionName())
	}
	return point, err
}

// LineFromWKB is a function that returns a linestring type from a WKB byte array
type LineFromWKB struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*LineFromWKB)(nil)

// NewLineFromWKB creates a new point expression.
func NewLineFromWKB(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_LINEFROMWKB", "1 or 2", len(args))
	}
	return &LineFromWKB{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (l *LineFromWKB) FunctionName() string {
	return "st_linefromwkb"
}

// Description implements sql.FunctionExpression
func (l *LineFromWKB) Description() string {
	return "returns a new linestring from WKB format."
}

// Type implements the sql.Expression interface.
func (l *LineFromWKB) Type() sql.Type {
	return sql.LineStringType{}
}

func (l *LineFromWKB) String() string {
	var args = make([]string, len(l.ChildExpressions))
	for i, arg := range l.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_LINEFROMWKB(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (l *LineFromWKB) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewLineFromWKB(children...)
}

// Eval implements the sql.Expression interface.
func (l *LineFromWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	line, err := EvalGeomFromWKB(ctx, row, l.ChildExpressions, sql.WKBLineID)
	if sql.ErrInvalidGISData.Is(err) {
		return nil, sql.ErrInvalidGISData.New(l.FunctionName())
	}
	return line, err
}

// PolyFromWKB is a function that returns a polygon type from a WKB byte array
type PolyFromWKB struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*PolyFromWKB)(nil)

// NewPolyFromWKB creates a new point expression.
func NewPolyFromWKB(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_POLYFROMWKB", "1, 2, or 3", len(args))
	}
	return &PolyFromWKB{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (p *PolyFromWKB) FunctionName() string {
	return "st_polyfromwkb"
}

// Description implements sql.FunctionExpression
func (p *PolyFromWKB) Description() string {
	return "returns a new polygon from WKB format."
}

// Type implements the sql.Expression interface.
func (p *PolyFromWKB) Type() sql.Type {
	return sql.PolygonType{}
}

func (p *PolyFromWKB) String() string {
	var args = make([]string, len(p.ChildExpressions))
	for i, arg := range p.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_POLYFROMWKB(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (p *PolyFromWKB) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewPolyFromWKB(children...)
}

// Eval implements the sql.Expression interface.
func (p *PolyFromWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	poly, err := EvalGeomFromWKB(ctx, row, p.ChildExpressions, sql.WKBPolyID)
	if sql.ErrInvalidGISData.Is(err) {
		return nil, sql.ErrInvalidGISData.New(p.FunctionName())
	}
	return poly, err
}
