package function

import (
	"encoding/binary"
	"fmt"
	"math"

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
	return a.Child.Type()
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
func serializeLine(l sql.Linestring, buf []byte) {
	// Write number of points
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(l.Points)))
	// Append each point
	for i, p := range l.Points {
		start, stop := 4 + 16 * i, 4 + 16 * (i + 1)
		serializePoint(p, buf[start:stop])
	}
}

// LineToBytes converts a sql.Linestring to a byte array
func LineToBytes(l sql.Linestring) []byte {
	// Initialize line buffer
	buf := make([]byte, 4 + 16 * len(l.Points))
	serializeLine(l, buf)
	return buf
}

func serializePoly(p sql.Polygon, buf []byte) {
	// Write number of lines
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(p.Lines)))
	// Append each line
	start, stop := 0, 4
	for _, l := range p.Lines {
		start, stop = stop, stop + 4 + 16 * len(l.Points)
		serializeLine(l, buf[start:stop])
	}
}

// PolyToBytes converts a sql.Polygon to a byte array
func PolyToBytes(p sql.Polygon) []byte {
	// Initialize polygon buffer
	size := 0
	for _, l := range p.Lines {
		size += 4 + 16 * len(l.Points)
	}
	buf := make([]byte, 4 + size)
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
	case sql.Linestring:
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
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*GeomFromWKB)(nil)

// NewGeomFromWKB creates a new geometry expression.
func NewGeomFromWKB(e sql.Expression) sql.Expression {
	return &GeomFromWKB{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *GeomFromWKB) FunctionName() string {
	return "st_geomfromwkb"
}

// Description implements sql.FunctionExpression
func (p *GeomFromWKB) Description() string {
	return "returns a new geometry from a WKB string."
}

// IsNullable implements the sql.Expression interface.
func (p *GeomFromWKB) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *GeomFromWKB) Type() sql.Type {
	return p.Child.Type()
}

func (p *GeomFromWKB) String() string {
	return fmt.Sprintf("ST_GEOMFROMWKB(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *GeomFromWKB) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewGeomFromWKB(children[0]), nil
}

// ParseWKBHeader parses the header portion of a byte array in WKB format to extract endianness and type
func ParseWKBHeader(buf []byte) (bool, uint32, error) {
	// Header length
	if len(buf) < WKBHeaderLength {
		return false, 0, sql.ErrInvalidGISData.New("ST_GeomFromWKB3")
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
func WKBToPoint(buf []byte, isBig bool) (sql.Point, error) {
	// Must be 16 bytes (2 floats)
	if len(buf) != 16 {
		return sql.Point{}, sql.ErrInvalidGISData.New("ST_PointFromWKB1")
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
	return sql.Point{X: x, Y: y}, nil
}

// WKBToLine parses the data portion of a byte array in WKB format to a point object
func WKBToLine(buf []byte, isBig bool) (sql.Linestring, error) {
	// Must be at least 4 bytes (length of linestring)
	if len(buf) < 4 {
		return sql.Linestring{}, sql.ErrInvalidGISData.New("ST_LineFromWKB")
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
		return sql.Linestring{}, sql.ErrInvalidGISData.New("ST_LineFromWKB")
	}

	// Parse points
	points := make([]sql.Point, numPoints)
	for i := uint32(0); i < numPoints; i++ {
		if point, err := WKBToPoint(lineData[16*i:16*(i+1)], isBig); err == nil {
			points[i] = point
		} else {
			return sql.Linestring{}, sql.ErrInvalidGISData.New("ST_LineFromWKB")
		}
	}

	return sql.Linestring{Points: points}, nil
}

// WKBToPoly parses the data portion of a byte array in WKB format to a point object
func WKBToPoly(buf []byte, isBig bool) (sql.Polygon, error) {
	// Must be at least 4 bytes (length of polygon)
	if len(buf) < 4 {
		return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromWKB1")
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
	lines := make([]sql.Linestring, numLines)
	for i := uint32(0); i < numLines; i++ {
		if line, err := WKBToLine(polyData[s:], isBig); err == nil {
			if isLinearRing(line) {
				lines[i] = line
				s += 4 + 16*len(line.Points) // shift parsing location over
			} else {
				return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromWKB2")
			}
		} else {
			return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromWKB3")
		}
	}

	return sql.Polygon{Lines: lines}, nil
}

// Eval implements the sql.Expression interface.
func (p *GeomFromWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := p.Child.Eval(ctx, row)
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

	// Parse accordingly
	switch geomType {
	case WKBPointID:
		return WKBToPoint(v[WKBHeaderLength:], isBig)
	case WKBLineID:
		return WKBToLine(v[WKBHeaderLength:], isBig)
	case WKBPolyID:
		return WKBToPoly(v[WKBHeaderLength:], isBig)
	default:
		return nil, sql.ErrInvalidGISData.New("ST_GeomFromWKB")
	}
}

// PointFromWKB is a function that returns a point type from a WKB byte array
type PointFromWKB struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*PointFromWKB)(nil)

// NewPointFromWKB creates a new point expression.
func NewPointFromWKB(e sql.Expression) sql.Expression {
	return &PointFromWKB{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *PointFromWKB) FunctionName() string {
	return "st_pointfromwkb"
}

// Description implements sql.FunctionExpression
func (p *PointFromWKB) Description() string {
	return "returns a new point from WKB format."
}

// IsNullable implements the sql.Expression interface.
func (p *PointFromWKB) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *PointFromWKB) Type() sql.Type {
	return p.Child.Type()
}

func (p *PointFromWKB) String() string {
	return fmt.Sprintf("ST_POINTFROMWKB(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *PointFromWKB) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewPointFromWKB(children[0]), nil
}

// Eval implements the sql.Expression interface.
func (p *PointFromWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := p.Child.Eval(ctx, row)
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

	// Read data
	return WKBToPoint(v[5:], isBig)
}

// LineFromWKB is a function that returns a linestring type from a WKB byte array
type LineFromWKB struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*LineFromWKB)(nil)

// NewLineFromWKB creates a new point expression.
func NewLineFromWKB(e sql.Expression) sql.Expression {
	return &LineFromWKB{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *LineFromWKB) FunctionName() string {
	return "st_linefromwkb"
}

// Description implements sql.FunctionExpression
func (p *LineFromWKB) Description() string {
	return "returns a new linestring from WKB format."
}

// IsNullable implements the sql.Expression interface.
func (p *LineFromWKB) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *LineFromWKB) Type() sql.Type {
	return p.Child.Type()
}

func (p *LineFromWKB) String() string {
	return fmt.Sprintf("ST_LINEFROMWKB(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *LineFromWKB) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewLineFromWKB(children[0]), nil
}

// Eval implements the sql.Expression interface.
func (p *LineFromWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := p.Child.Eval(ctx, row)
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

	// Not a point, throw error
	if geomType != WKBLineID {
		return nil, sql.ErrInvalidGISData.New("ST_LineFromWKB")
	}

	// Read data
	return WKBToLine(v[WKBHeaderLength:], isBig)
}

// PolyFromWKB is a function that returns a polygon type from a WKB byte array
type PolyFromWKB struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*PolyFromWKB)(nil)

// NewPolyFromWKB creates a new point expression.
func NewPolyFromWKB(e sql.Expression) sql.Expression {
	return &PolyFromWKB{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *PolyFromWKB) FunctionName() string {
	return "st_polyfromwkb"
}

// Description implements sql.FunctionExpression
func (p *PolyFromWKB) Description() string {
	return "returns a new polygon from WKB format."
}

// IsNullable implements the sql.Expression interface.
func (p *PolyFromWKB) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *PolyFromWKB) Type() sql.Type {
	return p.Child.Type()
}

func (p *PolyFromWKB) String() string {
	return fmt.Sprintf("ST_POLYFROMWKB(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *PolyFromWKB) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewPolyFromWKB(children[0]), nil
}

// Eval implements the sql.Expression interface.
func (p *PolyFromWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := p.Child.Eval(ctx, row)
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

	// Not a point, throw error
	if geomType != WKBPolyID {
		return nil, sql.ErrInvalidGISData.New("ST_PolyFromWKB")
	}

	// Read data
	return WKBToPoly(v[WKBHeaderLength:], isBig)
}
