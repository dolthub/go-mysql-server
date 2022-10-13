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
	"encoding/hex"
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
// Point, Polygon, LineString, MultiPoint, MultiLineString, MultiPolygon, GeometryCollection.
type GeometryValue interface {
	implementsGeometryValue()
	GetSRID() uint32
	SetSRID(srid uint32) GeometryValue
	Serialize() []byte
	WriteData(buf []byte)
	Swap() GeometryValue
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
	WKBHeaderSize  = EndianSize + TypeSize

	PointSize             = 16
	CountSize             = 4
	GeometryMaxByteLength = 4*(1024*1024*1024) - 1
)

// Type IDs
const (
	WKBUnknown = iota
	WKBPointID
	WKBLineID
	WKBPolyID
	WKBMultiPointID
	WKBMultiLineID
	WKBMultiPolyID
	WKBGeomCollectionID
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

// DeserializeEWKBHeader parses the header portion of a byte array in EWKB format to extract endianness and type
func DeserializeEWKBHeader(buf []byte) (srid uint32, bigEndian bool, typ uint32, err error) {
	// Must be right length
	if len(buf) < EWKBHeaderSize {
		return 0, false, 0, ErrInvalidGISData.New("DeserializeEWKBHeader")
	}
	srid = binary.LittleEndian.Uint32(buf) // First 4 bytes is SRID always in little endian
	buf = buf[SRIDSize:]                   // Shift pointer over
	bigEndian = buf[0] == 0                // Next byte is endianness
	buf = buf[EndianSize:]                 // Shift pointer over
	if bigEndian {                         // Next 4 bytes is type
		typ = binary.BigEndian.Uint32(buf)
	} else {
		typ = binary.LittleEndian.Uint32(buf)
	}

	return
}

// DeserializeWKBHeader parses the header potion of a byte array in WKB format
// There is no SRID
func DeserializeWKBHeader(buf []byte) (bigEndian bool, typ uint32, err error) {
	// Must be right length
	if len(buf) < (EndianSize + TypeSize) {
		return false, 0, ErrInvalidGISData.New("DeserializeWKBHeader")
	}

	bigEndian = buf[0] == 0 // First byte is byte order
	buf = buf[EndianSize:]  // Shift pointer over
	if bigEndian {          // Next 4 bytes is geometry type
		typ = binary.BigEndian.Uint32(buf)
	} else {
		typ = binary.LittleEndian.Uint32(buf)
	}

	return
}

// DeserializePoint parses the data portion of a byte array in WKB format to a Point object
func DeserializePoint(buf []byte, isBig bool, srid uint32) (Point, error) {
	// Must be 16 bytes (2 floats)
	if len(buf) != PointSize {
		return Point{}, ErrInvalidGISData.New("DeserializePoint")
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

// DeserializeLine parses the data portion of a byte array in WKB format to a LineString object
func DeserializeLine(buf []byte, isBig bool, srid uint32) (LineString, error) {
	// Must be at least CountSize and two points
	if len(buf) < (CountSize + PointSize + PointSize) {
		return LineString{}, ErrInvalidGISData.New("DeserializeLine")
	}

	// Read number of points
	points := make([]Point, readCount(buf, isBig))
	buf = buf[CountSize:]

	// Read points
	var err error
	for i := range points {
		points[i], err = DeserializePoint(buf[:PointSize], isBig, srid)
		if err != nil {
			return LineString{}, ErrInvalidGISData.New("DeserializeLine")
		}
		buf = buf[PointSize:]
	}

	return LineString{SRID: srid, Points: points}, nil
}

// DeserializePoly parses the data portion of a byte array in WKB format to a Polygon object
func DeserializePoly(buf []byte, isBig bool, srid uint32) (Polygon, error) {
	// Must be at least count, count, and four points
	if len(buf) < (CountSize + CountSize + 4*PointSize) {
		return Polygon{}, ErrInvalidGISData.New("DeserializePoly")
	}

	// Read number of lines
	lines := make([]LineString, readCount(buf, isBig))
	buf = buf[CountSize:]

	// Read lines
	var err error
	for i := range lines {
		lines[i], err = DeserializeLine(buf, isBig, srid)
		if err != nil {
			return Polygon{}, ErrInvalidGISData.New("DeserializePoly")
		}
		buf = buf[CountSize+len(lines[i].Points)*PointSize:]
	}

	return Polygon{SRID: srid, Lines: lines}, nil
}

func readCount(buf []byte, isBig bool) uint32 {
	if isBig {
		return binary.BigEndian.Uint32(buf)
	}
	return binary.LittleEndian.Uint32(buf)
}

// DeserializeMPoint parses the data portion of a byte array in WKB format to a MultiPoint object
func DeserializeMPoint(buf []byte, isBig bool, srid uint32) (MultiPoint, error) {
	// Must contain at count, wkb header, and one point
	if len(buf) < (CountSize + WKBHeaderSize + PointSize) {
		return MultiPoint{}, ErrInvalidGISData.New("DeserializeMPoint")
	}

	// Read number of points in MultiPoint
	points := make([]Point, readCount(buf, isBig))
	buf = buf[CountSize:]
	for i := range points {
		// WKBHeaders are inside MultiGeometry Types
		isBig, typ, err := DeserializeWKBHeader(buf)
		if err != nil {
			return MultiPoint{}, err
		}
		if typ != WKBPointID {
			return MultiPoint{}, ErrInvalidGISData.New("DeserializeMPoint")
		}
		buf = buf[WKBHeaderSize:]
		// Read point data
		points[i], err = DeserializePoint(buf[:PointSize], isBig, srid)
		if err != nil {
			return MultiPoint{}, err
		}
		buf = buf[PointSize:]
	}

	return MultiPoint{SRID: srid, Points: points}, nil
}

// DeserializeMLine parses the data portion of a byte array in WKB format to a MultiLineString object
func DeserializeMLine(buf []byte, isBig bool, srid uint32) (MultiLineString, error) {
	// Must contain at least length, wkb header, and two point
	if len(buf) < (CountSize + WKBHeaderSize + PointSize + PointSize) {
		return MultiLineString{}, ErrInvalidGISData.New("MultiLineString")
	}

	// Read number of lines
	lines := make([]LineString, readCount(buf, isBig))
	buf = buf[CountSize:]
	for i := range lines {
		// WKBHeaders are inside MultiGeometry Types
		isBig, typ, err := DeserializeWKBHeader(buf)
		if typ != WKBLineID {
			return MultiLineString{}, ErrInvalidGISData.New("DeserializeLine")
		}
		buf = buf[WKBHeaderSize:]
		lines[i], err = DeserializeLine(buf, isBig, srid)
		if err != nil {
			return MultiLineString{}, ErrInvalidGISData.New("DeserializeLine")
		}
		buf = buf[CountSize+len(lines[i].Points)*PointSize:]
	}

	return MultiLineString{SRID: srid, Lines: lines}, nil
}

func allocateBuffer(numPoints, numCounts, numWKBHeaders int) []byte {
	return make([]byte, EWKBHeaderSize+PointSize*numPoints+CountSize*numCounts+numWKBHeaders*WKBHeaderSize)
}

// WriteEWKBHeader will write EWKB header to the given buffer
func WriteEWKBHeader(buf []byte, srid, typ uint32) {
	binary.LittleEndian.PutUint32(buf, srid) // always write SRID in little endian
	buf = buf[SRIDSize:]                     // shift
	buf[0] = 1                               // always write in little endian
	buf = buf[EndianSize:]                   // shift
	binary.LittleEndian.PutUint32(buf, typ)  // write geometry type
}

// WriteWKBHeader will write WKB header to the given buffer
func WriteWKBHeader(buf []byte, typ uint32) {
	buf[0] = 1                              // always write in little endian
	buf = buf[EndianSize:]                  // shift
	binary.LittleEndian.PutUint32(buf, typ) // write geometry type
}

func writeCount(buf []byte, count uint32) {
	binary.LittleEndian.PutUint32(buf, count)
}

// Compare implements Type interface.
func (t GeometryType) Compare(a any, b any) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	switch inner := a.(type) {
	case Point:
		return PointType{}.Compare(inner, b)
	case LineString:
		return LineStringType{}.Compare(inner, b)
	case Polygon:
		return PolygonType{}.Compare(inner, b)
	case MultiPoint:
		return MultiPointType{}.Compare(inner, b)
	case MultiLineString:
		return MultiLineStringType{}.Compare(inner, b)
	default:
		return 0, ErrNotGeometry.New(a)
	}
}

// Convert implements Type interface.
func (t GeometryType) Convert(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	switch val := v.(type) {
	case []byte:
		srid, isBig, geomType, err := DeserializeEWKBHeader(val)
		if err != nil {
			return nil, err
		}
		val = val[EWKBHeaderSize:]

		var geom interface{}
		switch geomType {
		case WKBPointID:
			geom, err = DeserializePoint(val, isBig, srid)
		case WKBLineID:
			geom, err = DeserializeLine(val, isBig, srid)
		case WKBPolyID:
			geom, err = DeserializePoly(val, isBig, srid)
		case WKBMultiPointID:
			geom, err = DeserializeMPoint(val, isBig, srid)
		case WKBMultiLineID:
			geom, err = DeserializeMLine(val, isBig, srid)
		case WKBMultiPolyID:
			return nil, ErrUnsupportedGISType.New("MultiPolygon", hex.EncodeToString(val))
		case WKBGeomCollectionID:
			return nil, ErrUnsupportedGISType.New("GeometryCollection", hex.EncodeToString(val))
		default:
			return nil, ErrInvalidGISData.New("GeometryType.Convert")
		}
		if err != nil {
			return nil, err
		}
		return geom, nil
	case string:
		return t.Convert([]byte(val))
	case GeometryValue:
		if err := t.MatchSRID(val); err != nil {
			return nil, err
		}
		return val, nil
	default:
		return nil, ErrSpatialTypeConversion.New()
	}
}

// Equals implements the Type interface.
func (t GeometryType) Equals(otherType Type) (ok bool) {
	_, ok = otherType.(GeometryType)
	return
}

// MaxTextResponseByteLength implements the Type interface
func (t GeometryType) MaxTextResponseByteLength() uint32 {
	return GeometryMaxByteLength
}

// Promote implements the Type interface.
func (t GeometryType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t GeometryType) SQL(ctx *Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, nil
	}

	buf := v.(GeometryValue).Serialize()

	return sqltypes.MakeTrusted(sqltypes.Geometry, buf), nil
}

// String implements Type interface.
func (t GeometryType) String() string {
	return "geometry"
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
	case GeometryValue:
		srid = val.GetSRID()
	default:
		return ErrNotGeometry.New(v)
	}
	if t.SRID == srid {
		return nil
	}
	return ErrNotMatchingSRID.New(srid, t.SRID)
}
