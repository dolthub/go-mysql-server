// Copyright 2020-2022 Dolthub, Inc.
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
	"reflect"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"
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

var _ Type = GeomCollType{}
var _ SpatialColumnType = GeomCollType{}
var _ GeometryValue = GeomColl{}

var (
	ErrNotGeomColl = errors.NewKind("value of type %T is not a point")

	geomcollValueType = reflect.TypeOf(GeomColl{})
)

// Compare implements Type interface.
func (t GeomCollType) Compare(a interface{}, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// Expect to receive a GeomColl, throw error otherwise
	_a, ok := a.(GeomColl)
	if !ok {
		return 0, ErrNotGeomColl.New(a)
	}
	_b, ok := b.(GeomColl)
	if !ok {
		return 0, ErrNotGeomColl.New(b)
	}

	// Get shorter length
	var n int
	lenA := len(_a.Geoms)
	lenB := len(_b.Geoms)
	if lenA < lenB {
		n = lenA
	} else {
		n = lenB
	}

	// Compare each point until there's a difference
	for i := 0; i < n; i++ {
		ga := _a.Geoms[i]
		gb := _b.Geoms[i]
		var diff int
		var err error
		switch ga.(type) {
		case Point:
			diff, err = PointType{}.Compare(ga, gb)
		case LineString:
			diff, err = LineStringType{}.Compare(ga, gb)
		case Polygon:
			diff, err = PolygonType{}.Compare(ga, gb)
		case MultiPoint:
			diff, err = MultiPointType{}.Compare(ga, gb)
		case MultiLineString:
			diff, err = MultiLineStringType{}.Compare(ga, gb)
		case MultiPolygon:
			diff, err = MultiPolygonType{}.Compare(ga, gb)
		case GeomColl:
			diff, err = GeomCollType{}.Compare(ga, gb)
		default:
			panic("impossible")
		}
		if err != nil {
			return 0, err
		}
		if diff != 0 {
			return diff, nil
		}
	}

	// Determine based off length
	if lenA > lenB {
		return 1, nil
	}
	if lenA < lenB {
		return -1, nil
	}

	// GeomColls must be the same
	return 0, nil
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
			return nil, ErrInvalidGISData.New("GeomCollType.Convert")
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
		return nil, ErrSpatialTypeConversion.New()
	}
}

// Equals implements the Type interface.
func (t GeomCollType) Equals(otherType Type) bool {
	_, ok := otherType.(GeomCollType)
	return ok
}

// MaxTextResponseByteLength implements the Type interface
func (t GeomCollType) MaxTextResponseByteLength() uint32 {
	return GeometryMaxByteLength
}

// Promote implements the Type interface.
func (t GeomCollType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t GeomCollType) SQL(ctx *Context, dest []byte, v interface{}) (sqltypes.Value, error) {
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
	return "geometry_collection"
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
	return pointValueType
}

// GetSpatialTypeSRID implements SpatialColumnType interface.
func (t GeomCollType) GetSpatialTypeSRID() (uint32, bool) {
	return t.SRID, t.DefinedSRID
}

// SetSRID implements SpatialColumnType interface.
func (t GeomCollType) SetSRID(v uint32) Type {
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
	return ErrNotMatchingSRID.New(val.SRID, t.SRID)
}

// implementsGeometryValue implements GeometryValue interface.
func (p GeomColl) implementsGeometryValue() {}

// GetSRID implements GeometryValue interface.
func (p GeomColl) GetSRID() uint32 {
	return p.SRID
}

// SetSRID implements GeometryValue interface.
func (p GeomColl) SetSRID(srid uint32) GeometryValue {
	return GeomColl{
		SRID: srid,
	}
}

// Serialize implements GeometryValue interface.
func (p GeomColl) Serialize() (buf []byte) {
	buf = allocateBuffer(1, 0, 0)
	WriteEWKBHeader(buf, p.SRID, WKBGeomCollID)
	p.WriteData(buf[EWKBHeaderSize:])
	return
}

// WriteData implements GeometryValue interface.
func (p GeomColl) WriteData(buf []byte) {
	// write header
	// write data according to type
}

// Swap implements GeometryValue interface.
func (p GeomColl) Swap() GeometryValue {
	// TODO: iterate over geoms
	return GeomColl{
		SRID:  p.SRID,
		Geoms: p.Geoms,
	}
}
