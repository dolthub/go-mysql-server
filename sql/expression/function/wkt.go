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
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// AsWKT is a function that converts a spatial type into WKT format (alias for AsText)
type AsWKT struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*AsWKT)(nil)

// NewAsWKT creates a new point expression.
func NewAsWKT(e sql.Expression) sql.Expression {
	return &AsWKT{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *AsWKT) FunctionName() string {
	return "st_aswkb"
}

// Description implements sql.FunctionExpression
func (p *AsWKT) Description() string {
	return "returns binary representation of given spatial type."
}

// IsNullable implements the sql.Expression interface.
func (p *AsWKT) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *AsWKT) Type() sql.Type {
	return sql.LongText
}

func (p *AsWKT) String() string {
	return fmt.Sprintf("ST_ASWKT(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *AsWKT) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewAsWKT(children[0]), nil
}

// TODO: these functions could be refactored to be inside the sql.GeometryValue interface

// PointToWKT converts a sql.Point to a string
func PointToWKT(p sql.Point, order bool) string {
	x := strconv.FormatFloat(p.X, 'g', -1, 64)
	y := strconv.FormatFloat(p.Y, 'g', -1, 64)
	if order {
		x, y = y, x
	}
	return fmt.Sprintf("%s %s", x, y)
}

// LineToWKT converts a sql.LineString to a string
func LineToWKT(l sql.LineString, order bool) string {
	points := make([]string, len(l.Points))
	for i, p := range l.Points {
		points[i] = PointToWKT(p, order)
	}
	return strings.Join(points, ",")
}

// PolygonToWKT converts a sql.Polygon to a string
func PolygonToWKT(p sql.Polygon, order bool) string {
	lines := make([]string, len(p.Lines))
	for i, l := range p.Lines {
		lines[i] = "(" + LineToWKT(l, order) + ")"
	}
	return strings.Join(lines, ",")
}

// MultiPointToWKT converts a sql.MultiPoint to a string
func MultiPointToWKT(p sql.MultiPoint, order bool) string {
	points := make([]string, len(p.Points))
	for i, p := range p.Points {
		points[i] = PointToWKT(p, order)
	}
	return strings.Join(points, ",")
}

// MultiLineStringToWKT converts a sql.Polygon to a string
func MultiLineStringToWKT(l sql.MultiLineString, order bool) string {
	lines := make([]string, len(l.Lines))
	for i, line := range l.Lines {
		lines[i] = "(" + LineToWKT(line, order) + ")"
	}
	return strings.Join(lines, ",")
}

// Eval implements the sql.Expression interface.
func (p *AsWKT) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := p.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	var geomType string
	var data string
	switch v := val.(type) {
	case sql.Point:
		geomType = "POINT"
		data = PointToWKT(v, v.SRID == sql.GeoSpatialSRID)
	case sql.LineString:
		geomType = "LINESTRING"
		data = LineToWKT(v, v.SRID == sql.GeoSpatialSRID)
	case sql.Polygon:
		geomType = "POLYGON"
		data = PolygonToWKT(v, v.SRID == sql.GeoSpatialSRID)
	case sql.MultiPoint:
		geomType = "MULTIPOINT"
		data = MultiPointToWKT(v, v.SRID == sql.GeoSpatialSRID)
	case sql.MultiLineString:
		geomType = "MULTILINESTRING"
		data = MultiLineStringToWKT(v, v.SRID == sql.GeoSpatialSRID)
	default:
		return nil, sql.ErrInvalidGISData.New(p.FunctionName())
	}

	return fmt.Sprintf("%s(%s)", geomType, data), nil
}

// GeomFromText is a function that returns a point type from a WKT string
type GeomFromText struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*GeomFromText)(nil)

// NewGeomFromText creates a new point expression.
func NewGeomFromText(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_GEOMFROMTEXT", "1, 2, or 3", len(args))
	}
	return &GeomFromText{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (g *GeomFromText) FunctionName() string {
	return "st_geomfromtext"
}

// Description implements sql.FunctionExpression
func (g *GeomFromText) Description() string {
	return "returns a new point from a WKT string."
}

// Type implements the sql.Expression interface.
func (g *GeomFromText) Type() sql.Type {
	// TODO: return type is determined after Eval, use Geometry for now?
	return sql.GeometryType{}
}

func (g *GeomFromText) String() string {
	var args = make([]string, len(g.ChildExpressions))
	for i, arg := range g.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_GEOMFROMTEXT(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (g *GeomFromText) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewGeomFromText(children...)
}

// ParseWKTHeader should extract the type and data from the geometry string
func ParseWKTHeader(s string) (string, string, error) {
	// Read until first open parenthesis
	end := strings.Index(s, "(")

	// Bad if no parenthesis found
	if end == -1 {
		return "", "", sql.ErrInvalidGISData.New()
	}

	// Get Geometry Type
	geomType := s[:end]
	geomType = strings.TrimSpace(geomType)
	geomType = strings.ToLower(geomType)

	// Get data
	data := s[end:]
	data = strings.TrimSpace(data)

	// Check that data is surrounded by parentheses
	if data[0] != '(' || data[len(data)-1] != ')' {
		return "", "", sql.ErrInvalidGISData.New()
	}
	// Remove parentheses, and trim
	data = data[1 : len(data)-1]
	data = strings.TrimSpace(data)

	return geomType, data, nil
}

// WKTToPoint expects a string like this "1.2 3.4"
func WKTToPoint(s string, srid uint32, order bool) (sql.Point, error) {
	if len(s) == 0 {
		return sql.Point{}, sql.ErrInvalidGISData.New()
	}

	// Get everything between spaces
	args := strings.Fields(s)
	if len(args) != 2 {
		return sql.Point{}, sql.ErrInvalidGISData.New()
	}

	x, err := strconv.ParseFloat(args[0], 64)
	if err != nil {
		return sql.Point{}, sql.ErrInvalidGISData.New()
	}

	y, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return sql.Point{}, sql.ErrInvalidGISData.New()
	}

	if order {
		x, y = y, x
	}

	return sql.Point{SRID: srid, X: x, Y: y}, nil
}

// WKTToLine expects a string like "1.2 3.4, 5.6 7.8, ..."
func WKTToLine(s string, srid uint32, order bool) (sql.LineString, error) {
	if len(s) == 0 {
		return sql.LineString{}, sql.ErrInvalidGISData.New()
	}

	pointStrs := strings.Split(s, ",")
	var points = make([]sql.Point, len(pointStrs))
	var err error
	for i, ps := range pointStrs {
		ps = strings.TrimSpace(ps)
		if points[i], err = WKTToPoint(ps, srid, order); err != nil {
			return sql.LineString{}, sql.ErrInvalidGISData.New()
		}
	}

	// Create LineString object
	return sql.LineString{SRID: srid, Points: points}, nil
}

// WKTToPoly Expects a string like "(1 2, 3 4), (5 6, 7 8), ..."
func WKTToPoly(s string, srid uint32, order bool) (sql.Polygon, error) {
	var lines []sql.LineString
	for {
		// Look for closing parentheses
		end := strings.Index(s, ")")
		if end == -1 {
			return sql.Polygon{}, sql.ErrInvalidGISData.New()
		}

		// Extract linestring string; does not include ")"
		lineStr := s[:end]

		// Must start with open parenthesis
		if len(lineStr) == 0 || lineStr[0] != '(' {
			return sql.Polygon{}, sql.ErrInvalidGISData.New()
		}

		// Remove leading "("
		lineStr = lineStr[1:]

		// Remove leading and trailing whitespace
		lineStr = strings.TrimSpace(lineStr)

		// Parse line
		if line, err := WKTToLine(lineStr, srid, order); err == nil {
			// Check if line is linearring
			if isLinearRing(line) {
				lines = append(lines, line)
			} else {
				return sql.Polygon{}, sql.ErrInvalidGISData.New()
			}
		} else {
			return sql.Polygon{}, sql.ErrInvalidGISData.New()
		}

		// Prepare next string
		s = s[end+1:]
		s = strings.TrimSpace(s)

		// Reached end
		if len(s) == 0 {
			break
		}

		// LineStrings must be comma-separated
		if s[0] != ',' {
			return sql.Polygon{}, sql.ErrInvalidGISData.New()
		}

		// Drop leading comma
		s = s[1:]

		// Trim leading spaces
		s = strings.TrimSpace(s)
	}

	return sql.Polygon{SRID: srid, Lines: lines}, nil
}

// WKTToMPoint expects a string like "1.2 3.4, 5.6 7.8, ..."
func WKTToMPoint(s string, srid uint32, order bool) (sql.MultiPoint, error) {
	if len(s) == 0 {
		return sql.MultiPoint{}, sql.ErrInvalidGISData.New()
	}

	pointStrs := strings.Split(s, ",")
	var points = make([]sql.Point, len(pointStrs))
	var err error
	for i, ps := range pointStrs {
		ps = strings.TrimSpace(ps)
		if points[i], err = WKTToPoint(ps, srid, order); err != nil {
			return sql.MultiPoint{}, sql.ErrInvalidGISData.New()
		}
	}

	return sql.MultiPoint{SRID: srid, Points: points}, nil
}

// WKTToMLine Expects a string like "(1 2, 3 4), (5 6, 7 8), ..."
func WKTToMLine(s string, srid uint32, order bool) (sql.MultiLineString, error) {
	var lines []sql.LineString
	for {
		// Look for closing parentheses
		end := strings.Index(s, ")")
		if end == -1 {
			return sql.MultiLineString{}, sql.ErrInvalidGISData.New()
		}

		// Extract linestring string; does not include ")"
		lineStr := s[:end]

		// Must start with open parenthesis
		if len(lineStr) == 0 || lineStr[0] != '(' {
			return sql.MultiLineString{}, sql.ErrInvalidGISData.New()
		}

		// Remove leading "("
		lineStr = lineStr[1:]

		// Remove leading and trailing whitespace
		lineStr = strings.TrimSpace(lineStr)

		// Parse line
		if line, err := WKTToLine(lineStr, srid, order); err == nil {
			lines = append(lines, line)
		} else {
			return sql.MultiLineString{}, sql.ErrInvalidGISData.New()
		}

		// Prepare next string
		s = s[end+1:]
		s = strings.TrimSpace(s)

		// Reached end
		if len(s) == 0 {
			break
		}

		// LineStrings must be comma-separated
		if s[0] != ',' {
			return sql.MultiLineString{}, sql.ErrInvalidGISData.New()
		}

		// Drop leading comma
		s = s[1:]

		// Trim leading spaces
		s = strings.TrimSpace(s)
	}

	return sql.MultiLineString{SRID: srid, Lines: lines}, nil
}

// WKTToGeom expects a string in WKT format, and converts it to a geometry type
func WKTToGeom(ctx *sql.Context, row sql.Row, exprs []sql.Expression, expectedGeomType string) (sql.GeometryValue, error) {
	val, err := exprs[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	s, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidGISData.New()
	}

	geomType, data, err := ParseWKTHeader(s)
	if err != nil {
		return nil, err
	}

	if expectedGeomType != "" && geomType != expectedGeomType {
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

	order := srid == sql.GeoSpatialSRID
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
			return nil, err
		}
	}

	switch geomType {
	case "point":
		return WKTToPoint(data, srid, order)
	case "linestring":
		return WKTToLine(data, srid, order)
	case "polygon":
		return WKTToPoly(data, srid, order)
	case "multipoint":
		return WKTToMPoint(data, srid, order)
	case "multilinestring":
		return WKTToMLine(data, srid, order)
	default:
		return nil, sql.ErrInvalidGISData.New()
	}
}

// Eval implements the sql.Expression interface.
func (g *GeomFromText) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	geom, err := WKTToGeom(ctx, row, g.ChildExpressions, "")
	if sql.ErrInvalidGISData.Is(err) {
		return nil, sql.ErrInvalidGISData.New(g.FunctionName())
	}
	return geom, err
}

// PointFromText is a function that returns a point type from a WKT string
type PointFromText struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*PointFromText)(nil)

// NewPointFromText creates a new point expression.
func NewPointFromText(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_POINTFROMTEXT", "1, 2, or 3", len(args))
	}
	return &PointFromText{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (p *PointFromText) FunctionName() string {
	return "st_pointfromtext"
}

// Description implements sql.FunctionExpression
func (p *PointFromText) Description() string {
	return "returns a new point from a WKT string."
}

// Type implements the sql.Expression interface.
func (p *PointFromText) Type() sql.Type {
	return sql.PointType{}
}

func (p *PointFromText) String() string {
	var args = make([]string, len(p.ChildExpressions))
	for i, arg := range p.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_POINTFROMTEXT(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (p *PointFromText) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewPointFromText(children...)
}

// Eval implements the sql.Expression interface.
func (p *PointFromText) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	point, err := WKTToGeom(ctx, row, p.ChildExpressions, "point")
	if sql.ErrInvalidGISData.Is(err) {
		return nil, sql.ErrInvalidGISData.New(p.FunctionName())
	}
	return point, err
}

// LineFromText is a function that returns a LineString type from a WKT string
type LineFromText struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*LineFromText)(nil)

// NewLineFromText creates a new point expression.
func NewLineFromText(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_LINEFROMTEXT", "1 or 2", len(args))
	}
	return &LineFromText{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (l *LineFromText) FunctionName() string {
	return "st_linefromtext"
}

// Description implements sql.FunctionExpression
func (l *LineFromText) Description() string {
	return "returns a new line from a WKT string."
}

// Type implements the sql.Expression interface.
func (l *LineFromText) Type() sql.Type {
	return sql.LineStringType{}
}

func (l *LineFromText) String() string {
	var args = make([]string, len(l.ChildExpressions))
	for i, arg := range l.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_LINEFROMTEXT(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (l *LineFromText) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewLineFromText(children...)
}

// Eval implements the sql.Expression interface.
func (l *LineFromText) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	line, err := WKTToGeom(ctx, row, l.ChildExpressions, "linestring")
	if sql.ErrInvalidGISData.Is(err) {
		return nil, sql.ErrInvalidGISData.New(l.FunctionName())
	}
	return line, err
}

// PolyFromText is a function that returns a Polygon type from a WKT string
type PolyFromText struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*PolyFromText)(nil)

// NewPolyFromText creates a new polygon expression.
func NewPolyFromText(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_POLYFROMTEXT", "1, 2, or 3", len(args))
	}
	return &PolyFromText{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (p *PolyFromText) FunctionName() string {
	return "st_polyfromtext"
}

// Description implements sql.FunctionExpression
func (p *PolyFromText) Description() string {
	return "returns a new polygon from a WKT string."
}

// Type implements the sql.Expression interface.
func (p *PolyFromText) Type() sql.Type {
	return sql.PolygonType{}
}

func (p *PolyFromText) String() string {
	var args = make([]string, len(p.ChildExpressions))
	for i, arg := range p.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_POLYFROMTEXT(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (p *PolyFromText) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewPolyFromText(children...)
}

// Eval implements the sql.Expression interface.
func (p *PolyFromText) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	poly, err := WKTToGeom(ctx, row, p.ChildExpressions, "polygon")
	if sql.ErrInvalidGISData.Is(err) {
		return nil, sql.ErrInvalidGISData.New(p.FunctionName())
	}
	return poly, err
}

// MultiPoint is a function that returns a MultiPoint type from a WKT string
type MPointFromText struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*MPointFromText)(nil)

// NewMPointFromText creates a new MultiPoint expression.
func NewMPointFromText(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_MULTIPOINTFROMTEXT", "1 or 2", len(args))
	}
	return &MPointFromText{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (p *MPointFromText) FunctionName() string {
	return "st_mpointfromtext"
}

// Description implements sql.FunctionExpression
func (p *MPointFromText) Description() string {
	return "returns a new multipoint from a WKT string."
}

// Type implements the sql.Expression interface.
func (p *MPointFromText) Type() sql.Type {
	return sql.MultiPointType{}
}

func (p *MPointFromText) String() string {
	var args = make([]string, len(p.ChildExpressions))
	for i, arg := range p.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_MULTIPOINTFROMTEXT(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (p *MPointFromText) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewMPointFromText(children...)
}

// Eval implements the sql.Expression interface.
func (p *MPointFromText) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	line, err := WKTToGeom(ctx, row, p.ChildExpressions, "multipoint")
	if sql.ErrInvalidGISData.Is(err) {
		return nil, sql.ErrInvalidGISData.New(p.FunctionName())
	}
	return line, err
}

// MLineFromText is a function that returns a MultiLineString type from a WKT string
type MLineFromText struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*MLineFromText)(nil)

// NewMLineFromText creates a new multilinestring expression.
func NewMLineFromText(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_MLINEFROMTEXT", "1 or 2", len(args))
	}
	return &MLineFromText{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (l *MLineFromText) FunctionName() string {
	return "st_mlinefromtext"
}

// Description implements sql.FunctionExpression
func (l *MLineFromText) Description() string {
	return "returns a new multi line from a WKT string."
}

// Type implements the sql.Expression interface.
func (l *MLineFromText) Type() sql.Type {
	return sql.MultiLineStringType{}
}

func (l *MLineFromText) String() string {
	var args = make([]string, len(l.ChildExpressions))
	for i, arg := range l.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_MLINEFROMTEXT(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (l *MLineFromText) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewMLineFromText(children...)
}

// Eval implements the sql.Expression interface.
func (l *MLineFromText) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	line, err := WKTToGeom(ctx, row, l.ChildExpressions, "multilinestring")
	if sql.ErrInvalidGISData.Is(err) {
		return nil, sql.ErrInvalidGISData.New(l.FunctionName())
	}
	return line, err
}
