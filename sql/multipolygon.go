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

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// MultiPolygonType represents the MULTIPOLYGON type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-multipolygon.html
// The type of the returned value is MultiPolygon.
type MultiPolygonType struct {
	SRID        uint32
	DefinedSRID bool
}

// MultiPolygon is the value type returned from MultiLineStringType. Implements GeometryValue.
type MultiPolygon struct {
	SRID     uint32
	Polygons []Polygon
}

var (
	ErrNotMultiPolygon = errors.NewKind("value of type %T is not a multipolygon")

	multipolygonValueType = reflect.TypeOf(MultiPolygon{})
)

var _ Type = MultiPolygonType{}
var _ SpatialColumnType = MultiPolygonType{}
var _ GeometryValue = MultiPolygon{}

// Compare implements Type interface.
func (t MultiPolygonType) Compare(a interface{}, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// Expect to receive a MultiLineString, throw error otherwise
	_a, ok := a.(MultiLineString)
	if !ok {
		return 0, ErrNotMultiLineString.New(a)
	}
	_b, ok := b.(MultiLineString)
	if !ok {
		return 0, ErrNotMultiLineString.New(b)
	}

	// Get shorter length
	var n int
	lenA := len(_a.Lines)
	lenB := len(_b.Lines)
	if lenA < lenB {
		n = lenA
	} else {
		n = lenB
	}

	// Compare each line until there's a difference
	for i := 0; i < n; i++ {
		diff, err := LineStringType{}.Compare(_a.Lines[i], _b.Lines[i])
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

	// MultiLineString must be the same
	return 0, nil
}

// Convert implements Type interface.
func (t MultiPolygonType) Convert(v interface{}) (interface{}, error) {
	switch buf := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		mline, err := GeometryType{}.Convert(buf)
		if ErrInvalidGISData.Is(err) {
			return nil, ErrInvalidGISData.New("MultiLineString.Convert")
		}
		return mline, err
	case string:
		return t.Convert([]byte(buf))
	case MultiLineString:
		if err := t.MatchSRID(buf); err != nil {
			return nil, err
		}
		return buf, nil
	default:
		return nil, ErrSpatialTypeConversion.New()
	}
}

// Equals implements the Type interface.
func (t MultiPolygonType) Equals(otherType Type) bool {
	_, ok := otherType.(MultiLineStringType)
	return ok
}

// MaxTextResponseByteLength implements the Type interface
func (t MultiPolygonType) MaxTextResponseByteLength() uint32 {
	return GeometryMaxByteLength
}

// Promote implements the Type interface.
func (t MultiPolygonType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t MultiPolygonType) SQL(ctx *Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, nil
	}

	buf := v.(MultiLineString).Serialize()

	return sqltypes.MakeTrusted(sqltypes.Geometry, buf), nil
}

// String implements Type interface.
func (t MultiPolygonType) String() string {
	return "multilinestring"
}

// Type implements Type interface.
func (t MultiPolygonType) Type() query.Type {
	return sqltypes.Geometry
}

// ValueType implements Type interface.
func (t MultiPolygonType) ValueType() reflect.Type {
	return multilinestringValueType
}

// Zero implements Type interface.
func (t MultiPolygonType) Zero() interface{} {
	return MultiLineString{Lines: []LineString{{Points: []Point{{}, {}}}}}
}

// GetSpatialTypeSRID implements SpatialColumnType interface.
func (t MultiPolygonType) GetSpatialTypeSRID() (uint32, bool) {
	return t.SRID, t.DefinedSRID
}

// SetSRID implements SpatialColumnType interface.
func (t MultiPolygonType) SetSRID(v uint32) Type {
	t.SRID = v
	t.DefinedSRID = true
	return t
}

// MatchSRID implements SpatialColumnType interface
func (t MultiPolygonType) MatchSRID(v interface{}) error {
	val, ok := v.(MultiLineString)
	if !ok {
		return ErrNotMultiLineString.New(v)
	}
	if !t.DefinedSRID {
		return nil
	} else if t.SRID == val.SRID {
		return nil
	}
	return ErrNotMatchingSRID.New(val.SRID, t.SRID)
}

// implementsGeometryValue implements GeometryValue interface.
func (p MultiPolygon) implementsGeometryValue() {}

// GetSRID implements GeometryValue interface.
func (p MultiPolygon) GetSRID() uint32 {
	return p.SRID
}

// SetSRID implements GeometryValue interface.
func (p MultiPolygon) SetSRID(srid uint32) GeometryValue {
	polygons := make([]Polygon, len(p.Polygons))
	for i, p := range p.Polygons {
		polygons[i] = p.SetSRID(srid).(Polygon)
	}
	return MultiPolygon{
		SRID:     srid,
		Polygons: polygons,
	}
}

// Serialize implements GeometryValue interface.
func (p MultiPolygon) Serialize() (buf []byte) {
	var numPoints int
	for _, p := range p.Polygons {
		for _, l := range p.Lines {
			numPoints += len(l.Points)
		}
	}
	buf = allocateBuffer(numPoints, len(p.Polygons)+1, len(p.Polygons))
	WriteEWKBHeader(buf, p.SRID, WKBMultiLineID)
	p.WriteData(buf[EWKBHeaderSize:])
	return
}

// WriteData implements GeometryValue interface.
func (p MultiPolygon) WriteData(buf []byte) {
	writeCount(buf, uint32(len(p.Polygons)))
	buf = buf[CountSize:]
	for _, l := range p.Polygons {
		WriteWKBHeader(buf, WKBLineID)
		buf = buf[WKBHeaderSize:]
		l.WriteData(buf)
		buf = buf[CountSize+PointSize*len(l.Polygon):]
	}
}

// Swap implements GeometryValue interface.
func (p MultiPolygon) Swap() GeometryValue {
	lines := make([]Polygon, len(p.Polygons))
	for i, l := range p.Polygons {
		lines[i] = l.Swap().(Polygon)
	}
	return MultiPolygon{
		SRID:     p.SRID,
		Polygons: lines,
	}
}
