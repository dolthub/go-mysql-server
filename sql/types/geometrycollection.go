// Copyright 2022 Dolthub, Inc.
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

package types

import (
	"math"
	"reflect"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

// GeomCollType represents the GeometryCollection type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-point.html
// The type of the returned value is GeomColl.
type GeomCollType struct {
	SRID        uint32
	DefinedSRID bool
}

// GeomColl is the value type returned from GeomCollType. Implements GeometryValue.
type GeomColl struct {
	SRID  uint32
	Geoms []GeometryValue
}

var _ sql.Type = GeomCollType{}
var _ sql.SpatialColumnType = GeomCollType{}
var _ GeometryValue = GeomColl{}

var (
	ErrNotGeomColl = errors.NewKind("value of type %T is not a point")

	geomcollValueType = reflect.TypeOf(GeomColl{})
)

// Compare implements Type interface.
func (t GeomCollType) Compare(a interface{}, b interface{}) (int, error) {
	return GeometryType{}.Compare(a, b)
}

// Convert implements Type interface.
func (t GeomCollType) Convert(v interface{}) (interface{}, error) {
	// Allow null
	if v == nil {
		return nil, nil
	}
	// Handle conversions
	switch val := v.(type) {
	case []byte:
		// Parse header
		srid, isBig, geomType, err := DeserializeEWKBHeader(val)
		if err != nil {
			return nil, err
		}
		// Throw error if not marked as geometry collection
		if geomType != WKBGeomCollID {
			return nil, sql.ErrInvalidGISData.New("GeomCollType.Convert")
		}
		// Parse data section
		geom, _, err := DeserializeGeomColl(val[EWKBHeaderSize:], isBig, srid)
		if err != nil {
			return nil, err
		}
		return geom, nil
	case string:
		return t.Convert([]byte(val))
	case GeomColl:
		if err := t.MatchSRID(val); err != nil {
			return nil, err
		}
		return val, nil
	default:
		return nil, sql.ErrSpatialTypeConversion.New()
	}
}

// Equals implements the Type interface.
func (t GeomCollType) Equals(otherType sql.Type) bool {
	_, ok := otherType.(GeomCollType)
	return ok
}

// MaxTextResponseByteLength implements the Type interface
func (t GeomCollType) MaxTextResponseByteLength() uint32 {
	return GeometryMaxByteLength
}

// Promote implements the Type interface.
func (t GeomCollType) Promote() sql.Type {
	return t
}

// SQL implements Type interface.
func (t GeomCollType) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, nil
	}

	buf := v.(GeomColl).Serialize()

	return sqltypes.MakeTrusted(sqltypes.Geometry, buf), nil
}

// String implements Type interface.
func (t GeomCollType) String() string {
	return "geometrycollection"
}

// Type implements Type interface.
func (t GeomCollType) Type() query.Type {
	return sqltypes.Geometry
}

// Zero implements Type interface.
func (t GeomCollType) Zero() interface{} {
	return GeomColl{}
}

// ValueType implements Type interface.
func (t GeomCollType) ValueType() reflect.Type {
	return geomcollValueType
}

// GetSpatialTypeSRID implements SpatialColumnType interface.
func (t GeomCollType) GetSpatialTypeSRID() (uint32, bool) {
	return t.SRID, t.DefinedSRID
}

// SetSRID implements SpatialColumnType interface.
func (t GeomCollType) SetSRID(v uint32) sql.Type {
	t.SRID = v
	t.DefinedSRID = true
	return t
}

// MatchSRID implements SpatialColumnType interface
func (t GeomCollType) MatchSRID(v interface{}) error {
	val, ok := v.(GeomColl)
	if !ok {
		return ErrNotGeomColl.New(v)
	}
	if !t.DefinedSRID {
		return nil
	} else if t.SRID == val.SRID {
		return nil
	}
	return sql.ErrNotMatchingSRID.New(val.SRID, t.SRID)
}

// implementsGeometryValue implements GeometryValue interface.
func (g GeomColl) implementsGeometryValue() {}

// GetSRID implements GeometryValue interface.
func (g GeomColl) GetSRID() uint32 {
	return g.SRID
}

// SetSRID implements GeometryValue interface.
func (g GeomColl) SetSRID(srid uint32) GeometryValue {
	geoms := make([]GeometryValue, len(g.Geoms))
	for i, geom := range g.Geoms {
		geoms[i] = geom.SetSRID(srid)
	}
	return GeomColl{
		SRID:  srid,
		Geoms: geoms,
	}
}

// CalculateSize is a helper method to determine how much space to allocate for geometry collections
// TODO: recursion could be better; possible to expand to fit all types
func (g GeomColl) CalculateSize() (numPoints int, numCounts int, numHeaders int) {
	for _, geom := range g.Geoms {
		switch g := geom.(type) {
		case Point:
			numPoints += 1
			numHeaders += 1
		case LineString:
			numPoints += len(g.Points)
			numCounts += 1
			numHeaders += 1
		case Polygon:
			for _, l := range g.Lines {
				numPoints += len(l.Points)
				numCounts += 1
			}
			numCounts += 1
			numHeaders += 1
		case MultiPoint:
			numPoints += len(g.Points)
			numCounts += 1
			numHeaders += len(g.Points) + 1
		case MultiLineString:
			for _, l := range g.Lines {
				numPoints += len(l.Points)
				numCounts += 1
			}
			numCounts += 1
			numHeaders += len(g.Lines) + 1
		case MultiPolygon:
			for _, p := range g.Polygons {
				for _, l := range p.Lines {
					numPoints += len(l.Points)
					numCounts += 1
				}
				numCounts += 1
			}
			numCounts += 1
			numHeaders += len(g.Polygons) + 1
		case GeomColl:
			p, c, h := g.CalculateSize()
			numPoints += p
			numCounts += c + 1
			numHeaders += h + 1
		}
	}
	return
}

// Serialize implements GeometryValue interface.
// TODO: actually count all points to allocate
func (g GeomColl) Serialize() (buf []byte) {
	numPoints, numCounts, numHeaders := g.CalculateSize()
	buf = AllocateGeoTypeBuffer(numPoints, numCounts+1, numHeaders)
	WriteEWKBHeader(buf, g.SRID, WKBGeomCollID)
	g.WriteData(buf[EWKBHeaderSize:])
	return
}

// WriteData implements GeometryValue interface.
func (g GeomColl) WriteData(buf []byte) int {
	WriteCount(buf, uint32(len(g.Geoms)))
	buf = buf[CountSize:]
	count := CountSize
	for _, geom := range g.Geoms {
		var typ uint32
		switch geom.(type) {
		case Point:
			typ = WKBPointID
		case LineString:
			typ = WKBLineID
		case Polygon:
			typ = WKBPolyID
		case MultiPoint:
			typ = WKBMultiPointID
		case MultiLineString:
			typ = WKBMultiLineID
		case MultiPolygon:
			typ = WKBMultiPolyID
		case GeomColl:
			typ = WKBGeomCollID
		}
		WriteWKBHeader(buf, typ)
		buf = buf[WKBHeaderSize:]
		c := geom.WriteData(buf)
		buf = buf[c:]
		count += WKBHeaderSize + c
	}
	return count
}

// Swap implements GeometryValue interface.
func (g GeomColl) Swap() GeometryValue {
	geoms := make([]GeometryValue, len(g.Geoms))
	for i, g := range g.Geoms {
		geoms[i] = g.Swap()
	}
	return GeomColl{
		SRID:  g.SRID,
		Geoms: geoms,
	}
}

// BBox implements GeometryValue interface.
func (g GeomColl) BBox() (float64, float64, float64, float64) {
	minX, minY, maxX, maxY := math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64, -math.MaxFloat64
	for _, g := range g.Geoms {
		gMinX, gMinY, gMaxX, gMaxY := g.BBox()
		minX = math.Min(minX, gMinX)
		minY = math.Min(minY, gMinY)
		maxX = math.Max(maxX, gMaxX)
		maxY = math.Max(maxY, gMaxY)
	}
	return minX, minY, maxX, maxY
}
