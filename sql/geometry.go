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

package sql

import (
	"encoding/binary"
	"math"
	"reflect"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"
)

// GeometryType represents the GEOMETRY type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-geometry.html
// The type of the returned value is one of the following (each implements GeometryValue): Point, Polygon, LineString.
type GeometryType struct {
	SRID        uint32
	DefinedSRID bool
}

// GeometryValue is the value type returned from GeometryType, which is an interface over the following types:
// Point, Polygon, LineString.
type GeometryValue interface {
	implementsGeometryValue()
}

var _ Type = GeometryType{}
var _ SpatialColumnType = GeometryType{}

var (
	ErrNotGeometry = errors.NewKind("Value of type %T is not a geometry")

	geometryValueType = reflect.TypeOf((*GeometryValue)(nil)).Elem()
)

const (
	CartesianSRID  = uint32(0)
	GeoSpatialSRID = uint32(4326)
)

const (
	SRIDSize       = 4
	EndianSize     = 1
	TypeSize       = 4
	EWKBHeaderSize = SRIDSize + EndianSize + TypeSize

	PointSize = 16
	CountSize = 4
)

// Type IDs
const (
	WKBUnknown = iota
	WKBPointID
	WKBLineID
	WKBPolyID
)

// isLinearRing checks if a LineString is a linear ring
func isLinearRing(line LineString) bool {
	// Get number of points
	numPoints := len(line.Points)
	// Check length of LineString (must be 0 or 4+) points
	if numPoints != 0 && numPoints < 4 {
		return false
	}
	// Check if it is closed (first and last point are the same)
	if line.Points[0] != line.Points[numPoints-1] {
		return false
	}
	return true
}

// TODO: maybe move read_geometry.go and write_geometry.go from dolt to here
// ParseEWKBHeader parses the header portion of a byte array in WKB format to extract endianness and type
func ParseEWKBHeader(buf []byte) (srid uint32, bigEndian bool, typ uint32, err error) {
	// Must be right length
	if len(buf) < EWKBHeaderSize {
		return 0, false, 0, ErrInvalidGISData.New("ParseEWKBHeader")
	}
	srid = binary.LittleEndian.Uint32(buf[0:SRIDSize])                          // First 4 bytes is SRID always in little endian
	bigEndian = buf[SRIDSize] == 0                                              // Next byte is endianness
	typ = binary.LittleEndian.Uint32(buf[SRIDSize+EndianSize : EWKBHeaderSize]) // Next 4 bytes is type
	return
}

// WKBToPoint parses the data portion of a byte array in WKB format to a point object
func WKBToPoint(buf []byte, isBig bool, srid uint32) (Point, error) {
	// Must be 16 bytes (2 floats)
	if len(buf) != 16 {
		return Point{}, ErrInvalidGISData.New("WKBToPoint")
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

	return Point{SRID: srid, X: x, Y: y}, nil
}

// WKBToLine parses the data portion of a byte array in WKB format to a point object
func WKBToLine(buf []byte, isBig bool, srid uint32) (LineString, error) {
	// Must be at least 4 bytes (length of linestring)
	if len(buf) < 4 {
		return LineString{}, ErrInvalidGISData.New("WKBToLine")
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
		return LineString{}, ErrInvalidGISData.New("WKBToLine")
	}

	// Parse points
	points := make([]Point, numPoints)
	for i := uint32(0); i < numPoints; i++ {
		if point, err := WKBToPoint(lineData[16*i:16*(i+1)], isBig, srid); err == nil {
			points[i] = point
		} else {
			return LineString{}, ErrInvalidGISData.New("WKBToLine")
		}
	}

	return LineString{SRID: srid, Points: points}, nil
}

// WKBToPoly parses the data portion of a byte array in WKB format to a point object
func WKBToPoly(buf []byte, isBig bool, srid uint32) (Polygon, error) {
	// Must be at least 4 bytes (length of polygon)
	if len(buf) < 4 {
		return Polygon{}, ErrInvalidGISData.New("WKBToPoly")
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
	lines := make([]LineString, numLines)
	for i := uint32(0); i < numLines; i++ {
		if line, err := WKBToLine(polyData[s:], isBig, srid); err == nil {
			if isLinearRing(line) {
				lines[i] = line
				s += 4 + 16*len(line.Points) // shift parsing location over
			} else {
				return Polygon{}, ErrInvalidGISData.New("WKBToPoly")
			}
		} else {
			return Polygon{}, ErrInvalidGISData.New("WKBToPoly")
		}
	}

	return Polygon{SRID: srid, Lines: lines}, nil
}

// Compare implements Type interface.
func (t GeometryType) Compare(a any, b any) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// TODO: probably define operations for types like []byte and string
	// Expected to receive a geometry type
	switch inner := a.(type) {
	case Point:
		return PointType{}.Compare(inner, b)
	case LineString:
		return LineStringType{}.Compare(inner, b)
	case Polygon:
		return PolygonType{}.Compare(inner, b)
	default:
		return 0, ErrNotGeometry.New(a)
	}
}

// Convert implements Type interface.
func (t GeometryType) Convert(v interface{}) (interface{}, error) {
	// Allow null
	if v == nil {
		return nil, nil
	}
	// Handle conversions
	switch inner := v.(type) {
	case []byte:
		// Parse header
		srid, isBig, geomType, err := ParseEWKBHeader(inner)
		if err != nil {
			return nil, err
		}
		// Parse accordingly
		var geom interface{}
		switch geomType {
		case WKBPointID:
			geom, err = WKBToPoint(inner[EWKBHeaderSize:], isBig, srid)
		case WKBLineID:
			geom, err = WKBToLine(inner[EWKBHeaderSize:], isBig, srid)
		case WKBPolyID:
			geom, err = WKBToPoly(inner[EWKBHeaderSize:], isBig, srid)
		default:
			return nil, ErrInvalidGISData.New("GeometryType.Convert")
		}
		if err != nil {
			return nil, err
		}
		return geom, nil
	case string:
		return t.Convert([]byte(inner))
	case Point, LineString, Polygon:
		if err := t.MatchSRID(inner); err != nil {
			return nil, err
		}
		return inner, nil
	default:
		return nil, ErrSpatialTypeConversion.New()
	}
}

// Equals implements the Type interface.
func (t GeometryType) Equals(otherType Type) (ok bool) {
	_, ok = otherType.(GeometryType)
	return
}

// Promote implements the Type interface.
func (t GeometryType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t GeometryType) SQL(dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	pv, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, nil
	}

	//TODO: pretty sure this is wrong, pv is not a string type
	val := appendAndSliceString(dest, pv.(string))

	return sqltypes.MakeTrusted(sqltypes.Geometry, val), nil
}

// String implements Type interface.
func (t GeometryType) String() string {
	return "GEOMETRY"
}

// Type implements Type interface.
func (t GeometryType) Type() query.Type {
	return sqltypes.Geometry
}

// ValueType implements Type interface.
func (t GeometryType) ValueType() reflect.Type {
	return geometryValueType
}

// Zero implements Type interface.
func (t GeometryType) Zero() interface{} {
	// TODO: it doesn't make sense for geometry to have a zero type
	return nil
}

// GetSpatialTypeSRID implements SpatialColumnType interface.
func (t GeometryType) GetSpatialTypeSRID() (uint32, bool) {
	return t.SRID, t.DefinedSRID
}

// SetSRID implements SpatialColumnType interface.
func (t GeometryType) SetSRID(v uint32) Type {
	t.SRID = v
	t.DefinedSRID = true
	return t
}

// MatchSRID implements SpatialColumnType interface
func (t GeometryType) MatchSRID(v interface{}) error {
	if !t.DefinedSRID {
		return nil
	}
	// if matched with SRID value of row value
	var srid uint32
	switch val := v.(type) {
	case Point:
		srid = val.SRID
	case LineString:
		srid = val.SRID
	case Polygon:
		srid = val.SRID
	default:
		return ErrNotGeometry.New(v)
	}
	if t.SRID == srid {
		return nil
	}
	return ErrNotMatchingSRID.New(srid, t.SRID)
}
