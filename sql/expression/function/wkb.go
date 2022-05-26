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
	"encoding/binary"
	"fmt"
	"math"
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

// serializePoint fills in buf with the values from point
func serializePoint(p sql.Point, buf []byte) {
	// Assumes buf is correct size
	binary.LittleEndian.PutUint64(buf[0:8], math.Float64bits(p.X))
	binary.LittleEndian.PutUint64(buf[8:16], math.Float64bits(p.Y))
}

// PointToBytes converts a sql.Point to a byte array
func PointToBytes(p sql.Point) []byte {
	// Initialize point buffer
	buf := make([]byte, 16)
	serializePoint(p, buf)
	return buf
}

// serializeLine fills in buf with values from linestring
func serializeLine(l sql.LineString, buf []byte) {
	// Write number of points
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(l.Points)))
	// Append each point
	for i, p := range l.Points {
		start, stop := 4+16*i, 4+16*(i+1)
		serializePoint(p, buf[start:stop])
	}
}

// LineToBytes converts a sql.LineString to a byte array
func LineToBytes(l sql.LineString) []byte {
	// Initialize line buffer
	buf := make([]byte, 4+16*len(l.Points))
	serializeLine(l, buf)
	return buf
}

func serializePoly(p sql.Polygon, buf []byte) {
	// Write number of lines
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(p.Lines)))
	// Append each line
	start, stop := 0, 4
	for _, l := range p.Lines {
		start, stop = stop, stop+4+16*len(l.Points)
		serializeLine(l, buf[start:stop])
	}
}

// PolyToBytes converts a sql.Polygon to a byte array
func PolyToBytes(p sql.Polygon) []byte {
	// Initialize polygon buffer
	size := 0
	for _, l := range p.Lines {
		size += 4 + 16*len(l.Points)
	}
	buf := make([]byte, 4+size)
	serializePoly(p, buf)
	return buf
}

// Eval implements the sql.Expression interface.
func (a *AsWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := a.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Initialize buf with space for endianness (1 byte) and type (4 bytes)
	buf := make([]byte, 5)
	// MySQL seems to always use Little Endian
	buf[0] = 1
	var data []byte

	// Expect one of the geometry types
	switch v := val.(type) {
	case sql.Point:
		// Mark as point type
		binary.LittleEndian.PutUint32(buf[1:5], 1)
		data = PointToBytes(v)
	case sql.LineString:
		// Mark as linestring type
		binary.LittleEndian.PutUint32(buf[1:5], 2)
		data = LineToBytes(v)
	case sql.Polygon:
		// Mark as Polygon type
		binary.LittleEndian.PutUint32(buf[1:5], 3)
		data = PolyToBytes(v)
	default:
		return nil, sql.ErrInvalidGISData.New("ST_AsWKB")
	}

	// Append to header
	buf = append(buf, data...)

	return buf, nil
}

// Header contains endianness (1 byte) and geometry type (4 bytes)
const WKBHeaderLength = 5

// Type IDs
const (
	WKBUnknown = iota
	WKBPointID
	WKBLineID
	WKBPolyID
)

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

// ParseWKBHeader parses the header portion of a byte array in WKB format to extract endianness and type
func ParseWKBHeader(buf []byte) (bool, uint32, error) {
	// Header length
	if len(buf) < WKBHeaderLength {
		return false, 0, sql.ErrInvalidGISData.New("ST_GeomFromWKB")
	}

	// Get Endianness
	isBig := buf[0] == 0

	// Get Geometry Type
	var geomType uint32
	if isBig {
		geomType = binary.BigEndian.Uint32(buf[1:5])
	} else {
		geomType = binary.LittleEndian.Uint32(buf[1:5])
	}

	return isBig, geomType, nil
}

// WKBToPoint parses the data portion of a byte array in WKB format to a point object
func WKBToPoint(buf []byte, isBig bool, srid uint32, order bool) (sql.Point, error) {
	// Must be 16 bytes (2 floats)
	if len(buf) != 16 {
		return sql.Point{}, sql.ErrInvalidGISData.New("ST_PointFromWKB")
	}

	// Read floats x and y
	var x, y float64
	if isBig {
		x = math.Float64frombits(binary.BigEndian.Uint64(buf[:8]))
		y = math.Float64frombits(binary.BigEndian.Uint64(buf[8:]))
	} else {
		x = math.Float64frombits(binary.LittleEndian.Uint64(buf[:8]))
		y = math.Float64frombits(binary.LittleEndian.Uint64(buf[8:]))
	}

	// Determine if bool needs to be flipped
	if order {
		x, y = y, x
	}

	return sql.Point{SRID: srid, X: x, Y: y}, nil
}

// WKBToLine parses the data portion of a byte array in WKB format to a point object
func WKBToLine(buf []byte, isBig bool, srid uint32, order bool) (sql.LineString, error) {
	// Must be at least 4 bytes (length of linestring)
	if len(buf) < 4 {
		return sql.LineString{}, sql.ErrInvalidGISData.New("ST_LineFromWKB")
	}

	// Read length of line string
	var numPoints uint32
	if isBig {
		numPoints = binary.BigEndian.Uint32(buf[:4])
	} else {
		numPoints = binary.LittleEndian.Uint32(buf[:4])
	}

	// Extract line data
	lineData := buf[4:]

	// Check length
	if uint32(len(lineData)) < 16*numPoints {
		return sql.LineString{}, sql.ErrInvalidGISData.New("ST_LineFromWKB")
	}

	// Parse points
	points := make([]sql.Point, numPoints)
	for i := uint32(0); i < numPoints; i++ {
		if point, err := WKBToPoint(lineData[16*i:16*(i+1)], isBig, srid, order); err == nil {
			points[i] = point
		} else {
			return sql.LineString{}, sql.ErrInvalidGISData.New("ST_LineFromWKB")
		}
	}

	return sql.LineString{SRID: srid, Points: points}, nil
}

// WKBToPoly parses the data portion of a byte array in WKB format to a point object
func WKBToPoly(buf []byte, isBig bool, srid uint32, order bool) (sql.Polygon, error) {
	// Must be at least 4 bytes (length of polygon)
	if len(buf) < 4 {
		return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromWKB")
	}

	// Get number of lines in polygon
	var numLines uint32
	if isBig {
		numLines = binary.BigEndian.Uint32(buf[:4])
	} else {
		numLines = binary.LittleEndian.Uint32(buf[:4])
	}

	// Extract poly data
	polyData := buf[4:]

	// Parse lines
	s := 0
	lines := make([]sql.LineString, numLines)
	for i := uint32(0); i < numLines; i++ {
		if line, err := WKBToLine(polyData[s:], isBig, srid, order); err == nil {
			if isLinearRing(line) {
				lines[i] = line
				s += 4 + 16*len(line.Points) // shift parsing location over
			} else {
				return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromWKB")
			}
		} else {
			return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromWKB")
		}
	}

	return sql.Polygon{SRID: srid, Lines: lines}, nil
}

// ParseAxisOrder takes in a key, value string and determines the order of the xy coords
func ParseAxisOrder(s string) (bool, error) {
	// TODO: need to deal with whitespace, lowercase, and json-like parsing
	s = strings.ToLower(s)
	s = strings.TrimSpace(s)
	switch s {
	case "axis-order=long-lat":
		return true, nil
	case "axis-order=lat-long", "axis-order=srid-defined":
		return false, nil
	default:
		return false, sql.ErrInvalidArgument.New("placeholder")
	}
}

// Eval implements the sql.Expression interface.
func (g *GeomFromWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := g.ChildExpressions[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Must be of type byte array
	v, ok := val.([]byte)
	if !ok {
		return nil, sql.ErrInvalidGISData.New("ST_GeomFromWKB")
	}

	// Parse Header
	isBig, geomType, err := ParseWKBHeader(v)
	if err != nil {
		return nil, err
	}

	// TODO: convert to this block to helper function
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

	// Convert this block to helper function
	// Determine xy order
	order := false
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
	switch geomType {
	case WKBPointID:
		return WKBToPoint(v[WKBHeaderLength:], isBig, srid, order)
	case WKBLineID:
		return WKBToLine(v[WKBHeaderLength:], isBig, srid, order)
	case WKBPolyID:
		return WKBToPoly(v[WKBHeaderLength:], isBig, srid, order)
	default:
		return nil, sql.ErrInvalidGISData.New("ST_GeomFromWKB")
	}
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
	// Evaluate child
	val, err := p.ChildExpressions[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Must be of type byte array
	v, ok := val.([]byte)
	if !ok {
		return nil, sql.ErrInvalidGISData.New("ST_PointFromWKB")
	}

	// Parse Header
	isBig, geomType, err := ParseWKBHeader(v)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New("ST_PointFromWKB")
	}

	// Not a point, throw error
	if geomType != WKBPointID {
		return nil, sql.ErrInvalidGISData.New("ST_PointFromWKB")
	}

	// TODO: convert to this block to helper function
	// Determine SRID
	srid := sql.CartesianSRID
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

	// Read data
	return WKBToPoint(v[WKBHeaderLength:], isBig, srid, order)
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
	// Evaluate child
	val, err := l.ChildExpressions[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Must be of type byte array
	v, ok := val.([]byte)
	if !ok {
		return nil, sql.ErrInvalidGISData.New("ST_LineFromWKB")
	}

	// Parse Header
	isBig, geomType, err := ParseWKBHeader(v)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New("ST_LineFromWKB")
	}

	// Not a line, throw error
	if geomType != WKBLineID {
		return nil, sql.ErrInvalidGISData.New("ST_LineFromWKB")
	}

	// TODO: convert to this block to helper function
	// Determine SRID
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

	// Determine xy order
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
			return nil, sql.ErrInvalidArgument.New(l.FunctionName())
		}
	}

	// Read data
	return WKBToLine(v[WKBHeaderLength:], isBig, srid, order)
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
	// Evaluate child
	val, err := p.ChildExpressions[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Must be of type byte array
	v, ok := val.([]byte)
	if !ok {
		return nil, sql.ErrInvalidGISData.New("ST_PolyFromWKB")
	}

	// Parse Header
	isBig, geomType, err := ParseWKBHeader(v)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New("ST_PolyFromWKB")
	}

	// Not a polygon, throw error
	if geomType != WKBPolyID {
		return nil, sql.ErrInvalidGISData.New("ST_PolyFromWKB")
	}

	// TODO: convert to this block to helper function
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

	// Read data
	return WKBToPoly(v[WKBHeaderLength:], isBig, srid, order)
}

func ValidateSRID(srid uint32) error {
	if srid != sql.CartesianSRID && srid != sql.GeoSpatialSRID {
		return ErrInvalidSRID.New(srid)
	}
	return nil
}
