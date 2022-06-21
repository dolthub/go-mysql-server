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
	"fmt"
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
	PointID
	LineID
	PolyID
	MultiPointID
	MultiLineID
	MultiPolyID
	GeoCollectionID
)

// DeserializeEWKBHeader parses the header portion of a byte array in WKB format to extract endianness and type
func DeserializeEWKBHeader(buf []byte) (srid uint32, bigEndian bool, typ uint32, err error) {
	// Must be right length
	if len(buf) < EWKBHeaderSize {
		return 0, false, 0, ErrInvalidGISData.New("DeserializeEWKBHeader")
	}
	srid = binary.LittleEndian.Uint32(buf[0:SRIDSize])                          // First 4 bytes is SRID always in little endian
	bigEndian = buf[SRIDSize] == 0                                              // Next byte is endianness
	typ = binary.LittleEndian.Uint32(buf[SRIDSize+EndianSize : EWKBHeaderSize]) // Next 4 bytes is type
	return
}

// DeserializePoint parses the data portion of a byte array in EWKB format to a point object
func DeserializePoint(buf []byte, isBig bool, srid uint32) (Point, error) {
	if len(buf) >= PointSize {
		return Point{}, ErrInvalidGISData.New("DeserializePoint")
	}

	var x, y float64
	if isBig {
		x = math.Float64frombits(binary.BigEndian.Uint64(buf[:PointSize/2]))
		y = math.Float64frombits(binary.BigEndian.Uint64(buf[PointSize/2:]))
	} else {
		x = math.Float64frombits(binary.LittleEndian.Uint64(buf[:PointSize/2]))
		y = math.Float64frombits(binary.LittleEndian.Uint64(buf[PointSize/2:]))
	}

	return Point{SRID: srid, X: x, Y: y}, nil
}

func readCount(buf []byte, isBig bool) uint32 {
	if isBig {
		return binary.BigEndian.Uint32(buf[:CountSize])
	} else {
		return binary.LittleEndian.Uint32(buf[:CountSize])
	}
}

// DeserializeLineString parses the data portion of a byte array in EWKB format to a point object
func DeserializeLineString(buf []byte, isBig bool, srid uint32) (LineString, error) {
	if len(buf) < CountSize {
		return LineString{}, ErrInvalidGISData.New("DeserializeLineString")
	}

	numPoints := readCount(buf, isBig)
	buf = buf[CountSize:]

	if uint32(len(buf)) < PointSize*numPoints {
		return LineString{}, ErrInvalidGISData.New("DeserializeLineString")
	}

	points := make([]Point, numPoints)
	for i := uint32(0); i < numPoints; i++ {
		point, err := DeserializePoint(buf, isBig, srid)
		if err != nil {
			return LineString{}, ErrInvalidGISData.New("DeserializeLineString")
		}
		points[i] = point
		buf = buf[PointSize:]
	}

	return LineString{SRID: srid, Points: points}, nil
}

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

// DeserializePolygon parses the data portion of a byte array in EWKB format to a polygon object
func DeserializePolygon(buf []byte, isBig bool, srid uint32) (Polygon, error) {
	if len(buf) < CountSize {
		return Polygon{}, ErrInvalidGISData.New("DeserializePolygon")
	}

	numLines := readCount(buf, isBig)
	buf = buf[CountSize:]

	lines := make([]LineString, numLines)
	for i := uint32(0); i < numLines; i++ {
		line, err := DeserializeLineString(buf, isBig, srid)
		if err != nil {
			return Polygon{}, ErrInvalidGISData.New("DeserializePolygon")
		}
		if !isLinearRing(line) {
			return Polygon{}, ErrInvalidGISData.New("DeserializePolygon")
		}
		lines[i] = line
		buf = buf[CountSize+PointSize*len(line.Points):]
	}

	return Polygon{SRID: srid, Lines: lines}, nil
}

func allocateBuffer(numPoints, numCounts int) []byte {
	return make([]byte, EWKBHeaderSize+PointSize*numPoints+CountSize*numCounts)
}

// SerializeEWKBHeader will write EWKB header to the given buffer
func SerializeEWKBHeader(buf []byte, srid, typ uint32) {
	binary.LittleEndian.PutUint32(buf[:4], srid)
	buf[4] = 1
	binary.LittleEndian.PutUint32(buf[5:9], typ)
}

func SerializePointData(buf []byte, x, y float64) {
	binary.LittleEndian.PutUint64(buf[:PointSize/2], math.Float64bits(x))
	binary.LittleEndian.PutUint64(buf[PointSize/2:], math.Float64bits(y))
}

func SerializePoint(p Point) (buf []byte) {
	buf = allocateBuffer(1, 0)
	SerializeEWKBHeader(buf[:EWKBHeaderSize], p.SRID, PointID)
	SerializePointData(buf[EWKBHeaderSize:], p.X, p.Y)
	return
}

func writeCount(buf []byte, count uint32) {
	binary.LittleEndian.PutUint32(buf, count)
}

func writePointSlice(buf []byte, points []Point) {
	writeCount(buf, uint32(len(points)))
	buf = buf[CountSize:]
	for _, p := range points {
		SerializePointData(buf, p.X, p.Y)
		buf = buf[PointSize:]
	}
}

func SerializeLineString(l LineString) (buf []byte) {
	buf = allocateBuffer(len(l.Points), 1)
	SerializeEWKBHeader(buf[:EWKBHeaderSize], l.SRID, LineID)
	writePointSlice(buf[EWKBHeaderSize:], l.Points)
	return
}

func writeLineSlice(buf []byte, lines []LineString) {
	writeCount(buf, uint32(len(lines)))
	buf = buf[CountSize:]
	for _, l := range lines {
		writePointSlice(buf, l.Points)
		sz := CountSize + len(l.Points)*PointSize
		buf = buf[sz:]
	}
}

func countPoints(p Polygon) (cnt int) {
	for _, line := range p.Lines {
		cnt += len(line.Points)
	}
	return
}

func SerializePolygon(p Polygon) (buf []byte) {
	buf = allocateBuffer(countPoints(p), len(p.Lines)+1)
	SerializeEWKBHeader(buf[:EWKBHeaderSize], p.SRID, PolyID)
	writeLineSlice(buf[EWKBHeaderSize:], p.Lines)
	return
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
		srid, isBig, geomType, err := DeserializeEWKBHeader(inner)
		if err != nil {
			return nil, err
		}
		// Parse accordingly
		var geom interface{}
		switch geomType {
		case PointID:
			geom, err = DeserializePoint(inner[EWKBHeaderSize:], isBig, srid)
		case LineID:
			geom, err = DeserializeLineString(inner[EWKBHeaderSize:], isBig, srid)
		case PolyID:
			geom, err = DeserializePolygon(inner[EWKBHeaderSize:], isBig, srid)
		case MultiPointID:
			return nil, ErrUnsupportedGISType.New("MultiPoint", hex.EncodeToString(inner))
		case MultiLineID:
			return nil, ErrUnsupportedGISType.New("MultiLineString", hex.EncodeToString(inner))
		case MultiPolyID:
			return nil, ErrUnsupportedGISType.New("MultiPolygon", hex.EncodeToString(inner))
		case GeoCollectionID:
			return nil, ErrUnsupportedGISType.New("GeometryCollection", hex.EncodeToString(inner))
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

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, nil
	}

	var buf []byte
	switch val := v.(type) {
	case Point:
		buf = SerializePoint(val)
	case LineString:
		buf = SerializeLineString(val)
	case Polygon:
		buf = SerializePolygon(val)
	}

	val := appendAndSliceString(dest, fmt.Sprintf("0x%X", buf))

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
