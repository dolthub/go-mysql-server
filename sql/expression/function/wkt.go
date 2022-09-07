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
	// Expect one of the geometry types
	switch v := val.(type) {
	case sql.Point:
		// Mark as point type
		geomType = "POINT"
		data = PointToWKT(v, v.SRID == sql.GeoSpatialSRID)
	case sql.LineString:
		// Mark as linestring type
		geomType = "LINESTRING"
		data = LineToWKT(v, v.SRID == sql.GeoSpatialSRID)
	case sql.Polygon:
		// Mark as Polygon type
		geomType = "POLYGON"
		data = PolygonToWKT(v, v.SRID == sql.GeoSpatialSRID)
	default:
		return nil, sql.ErrInvalidGISData.New("ST_AsWKT")
	}

	return fmt.Sprintf("%s(%s)", geomType, data), nil
}

// GeomFromText is a function that returns a point type from a WKT string
type GeomFromText struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*GeomFromText)(nil)

// NewGeomFromWKT creates a new point expression.
func NewGeomFromWKT(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_GEOMFROMWKT", "1, 2, or 3", len(args))
	}
	return &GeomFromText{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (g *GeomFromText) FunctionName() string {
	return "st_geomfromwkt"
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
	return fmt.Sprintf("ST_GEOMFROMWKT(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (g *GeomFromText) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewGeomFromWKT(children...)
}

// ParseWKTHeader should extract the type from the geometry string
func ParseWKTHeader(s string) (string, string, error) {
	// Read until first open parenthesis
	end := strings.Index(s, "(")

	// Bad if no parenthesis found
	if end == -1 {
		return "", "", sql.ErrInvalidGISData.New("ST_GeomFromText")
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
		return "", "", sql.ErrInvalidGISData.New("ST_GeomFromText")
	}
	// Remove parentheses, and trim
	data = data[1 : len(data)-1]
	data = strings.TrimSpace(data)

	return geomType, data, nil
}

// WKTToPoint expects a string like this "1.2 3.4"
func WKTToPoint(s string, srid uint32, order bool) (sql.Point, error) {
	// Empty string is wrong
	if len(s) == 0 {
		return sql.Point{}, sql.ErrInvalidGISData.New("ST_PointFromText")
	}

	// Get everything between spaces
	args := strings.Fields(s)

	// Check length
	if len(args) != 2 {
		return sql.Point{}, sql.ErrInvalidGISData.New("ST_PointFromText")
	}

	// Parse x
	x, err := strconv.ParseFloat(args[0], 64)
	if err != nil {
		return sql.Point{}, sql.ErrInvalidGISData.New("ST_PointFromText")
	}

	// Parse y
	y, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return sql.Point{}, sql.ErrInvalidGISData.New("ST_PointFromText")
	}

	// See if we need to swap x and y
	if order {
		x, y = y, x
	}

	// Create point object
	return sql.Point{SRID: srid, X: x, Y: y}, nil
}

// WKTToLine expects a string like "1.2 3.4, 5.6 7.8, ..."
func WKTToLine(s string, srid uint32, order bool) (sql.LineString, error) {
	// Empty string is wrong
	if len(s) == 0 {
		return sql.LineString{}, sql.ErrInvalidGISData.New("ST_LineFromText")
	}

	// Separate by comma
	pointStrs := strings.Split(s, ",")

	// Parse each point string
	var points = make([]sql.Point, len(pointStrs))
	for i, ps := range pointStrs {
		// Remove leading and trailing whitespace
		ps = strings.TrimSpace(ps)

		// Parse point
		if p, err := WKTToPoint(ps, srid, order); err == nil {
			points[i] = p
		} else {
			return sql.LineString{}, sql.ErrInvalidGISData.New("ST_LineFromText")
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
			return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromText")
		}

		// Extract linestring string; does not include ")"
		lineStr := s[:end]

		// Must start with open parenthesis
		if len(lineStr) == 0 || lineStr[0] != '(' {
			return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromText")
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
				return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromText")
			}
		} else {
			return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromText")
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
			return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromText")
		}

		// Drop leading comma
		s = s[1:]

		// Trim leading spaces
		s = strings.TrimSpace(s)
	}

	// Create Polygon object
	return sql.Polygon{SRID: srid, Lines: lines}, nil
}

// Eval implements the sql.Expression interface.
func (g *GeomFromText) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := g.ChildExpressions[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Expect a string, throw error otherwise
	s, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidGISData.New("ST_GeomFromText")
	}

	// Determine type, and get data
	geomType, data, err := ParseWKTHeader(s)
	if err != nil {
		return nil, err
	}

	// Determine SRID
	srid := uint32(0)
	if len(g.ChildExpressions) >= 2 {
		s, err := g.ChildExpressions[1].Eval(ctx, row)
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

	// Determine xy order
	order := srid == sql.GeoSpatialSRID
	if len(g.ChildExpressions) == 3 {
		o, err := g.ChildExpressions[2].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if o == nil {
			return nil, nil
		}
		order, err = ParseAxisOrder(o.(string))
		if err != nil {
			return nil, sql.ErrInvalidArgument.New(g.FunctionName())
		}
	}

	// Parse accordingly
	// TODO: define consts instead of string comparison?
	switch geomType {
	case "point":
		return WKTToPoint(data, srid, order)
	case "linestring":
		return WKTToLine(data, srid, order)
	case "polygon":
		return WKTToPoly(data, srid, order)
	default:
		return nil, sql.ErrInvalidGISData.New("ST_GeomFromText")
	}
}

// PointFromWKT is a function that returns a point type from a WKT string
type PointFromWKT struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*PointFromWKT)(nil)

// NewPointFromWKT creates a new point expression.
func NewPointFromWKT(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_POINTFROMWKT", "1, 2, or 3", len(args))
	}
	return &PointFromWKT{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (p *PointFromWKT) FunctionName() string {
	return "st_pointfromwkt"
}

// Description implements sql.FunctionExpression
func (p *PointFromWKT) Description() string {
	return "returns a new point from a WKT string."
}

// Type implements the sql.Expression interface.
func (p *PointFromWKT) Type() sql.Type {
	return sql.PointType{}
}

func (p *PointFromWKT) String() string {
	var args = make([]string, len(p.ChildExpressions))
	for i, arg := range p.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_POINTFROMWKT(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (p *PointFromWKT) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewPointFromWKT(children...)
}

// Eval implements the sql.Expression interface.
func (p *PointFromWKT) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := p.ChildExpressions[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Must be of type string
	s, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidGISData.New("ST_PointFromText")
	}

	// Parse Header
	geomType, data, err := ParseWKTHeader(s)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New("ST_PointFromText")
	}

	// Not a point, throw error
	if geomType == "point" {
		return nil, sql.ErrInvalidGISData.New("ST_PointFromText")
	}

	// Determine SRID
	srid := uint32(0)
	if len(p.ChildExpressions) >= 2 {
		s, err := p.ChildExpressions[1].Eval(ctx, row)
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

	// Determine xy order
	order := false
	if len(p.ChildExpressions) == 3 {
		o, err := p.ChildExpressions[2].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if o == nil {
			return nil, nil
		}
		order, err = ParseAxisOrder(o.(string))
		if err != nil {
			return nil, sql.ErrInvalidArgument.New(p.FunctionName())
		}
	}

	return WKTToPoint(data, srid, order)
}

// LineFromWKT is a function that returns a point type from a WKT string
type LineFromWKT struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*LineFromWKT)(nil)

// NewLineFromWKT creates a new point expression.
func NewLineFromWKT(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_LINEFROMWKT", "1 or 2", len(args))
	}
	return &LineFromWKT{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (l *LineFromWKT) FunctionName() string {
	return "st_linefromwkt"
}

// Description implements sql.FunctionExpression
func (l *LineFromWKT) Description() string {
	return "returns a new line from a WKT string."
}

// Type implements the sql.Expression interface.
func (l *LineFromWKT) Type() sql.Type {
	return sql.LineStringType{}
}

func (l *LineFromWKT) String() string {
	var args = make([]string, len(l.ChildExpressions))
	for i, arg := range l.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_LINEFROMWKT(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (l *LineFromWKT) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewLineFromWKT(children...)
}

// Eval implements the sql.Expression interface.
func (l *LineFromWKT) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := l.ChildExpressions[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Expect a string, throw error otherwise
	s, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidGISData.New("ST_LineFromText")
	}

	// Parse Header
	geomType, data, err := ParseWKTHeader(s)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New("ST_LineFromText")
	}

	// Not a line, throw error
	if geomType != "linestring" {
		return nil, sql.ErrInvalidGISData.New("ST_LineFromText")
	}

	// Evaluate second argument
	srid := uint32(0)
	if len(l.ChildExpressions) >= 2 {
		s, err := l.ChildExpressions[1].Eval(ctx, row)
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

	// Determine xt order
	order := false
	if len(l.ChildExpressions) == 3 {
		o, err := l.ChildExpressions[2].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if o == nil {
			return nil, nil
		}
		order, err = ParseAxisOrder(o.(string))
		if err != nil {
			return nil, sql.ErrInvalidGISData.New("ST_LineFromText")
		}
	}

	return WKTToLine(data, srid, order)
}

// PolyFromWKT is a function that returns a polygon type from a WKT string
type PolyFromWKT struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*PolyFromWKT)(nil)

// NewPolyFromWKT creates a new polygon expression.
func NewPolyFromWKT(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_POLYFROMWKT", "1, 2, or 3", len(args))
	}
	return &PolyFromWKT{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (p *PolyFromWKT) FunctionName() string {
	return "st_polyfromwkt"
}

// Description implements sql.FunctionExpression
func (p *PolyFromWKT) Description() string {
	return "returns a new polygon from a WKT string."
}

// Type implements the sql.Expression interface.
func (p *PolyFromWKT) Type() sql.Type {
	return sql.PolygonType{}
}

func (p *PolyFromWKT) String() string {
	var args = make([]string, len(p.ChildExpressions))
	for i, arg := range p.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_POLYFROMWKT(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (p *PolyFromWKT) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewPolyFromWKT(children...)
}

// Eval implements the sql.Expression interface.
func (p *PolyFromWKT) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := p.ChildExpressions[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Expect a string, throw error otherwise
	s, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidGISData.New("ST_PolyFromWKT")
	}

	// Parse Header
	geomType, data, err := ParseWKTHeader(s)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New("ST_PolyFromWKT")
	}

	// Not a polygon, throw error
	if geomType != "polygon" {
		return nil, sql.ErrInvalidGISData.New("ST_PolyFromText")
	}

	// Determine SRID
	srid := uint32(0)
	if len(p.ChildExpressions) > 2 {
		s, err := p.ChildExpressions[1].Eval(ctx, row)
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

	// Determine xy order
	order := false
	if len(p.ChildExpressions) == 3 {
		o, err := p.ChildExpressions[2].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if o == nil {
			return nil, nil
		}
		order, err = ParseAxisOrder(o.(string))
		if err != nil {
			return nil, sql.ErrInvalidArgument.New(p.FunctionName())
		}
	}

	return WKTToPoly(data, srid, order)
}
