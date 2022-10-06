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
	"reflect"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// LineStringType represents the LINESTRING type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-linestring.html
// The type of the returned value is LineString.
type LineStringType struct {
	SRID        uint32
	DefinedSRID bool
}

// LineString is the value type returned from LineStringType. Implements GeometryValue.
type LineString struct {
	SRID   uint32
	Points []Point
}

var _ Type = LineStringType{}
var _ SpatialColumnType = LineStringType{}
var _ GeometryValue = LineString{}

var (
	ErrNotLineString = errors.NewKind("value of type %T is not a linestring")

	lineStringValueType = reflect.TypeOf(LineString{})
)

// Compare implements Type interface.
func (t LineStringType) Compare(a interface{}, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// Expect to receive a LineString, throw error otherwise
	_a, ok := a.(LineString)
	if !ok {
		return 0, ErrNotLineString.New(a)
	}
	_b, ok := b.(LineString)
	if !ok {
		return 0, ErrNotLineString.New(b)
	}

	// Get shorter length
	var n int
	lenA := len(_a.Points)
	lenB := len(_b.Points)
	if lenA < lenB {
		n = lenA
	} else {
		n = lenB
	}

	// Compare each point until there's a difference
	for i := 0; i < n; i++ {
		diff, err := PointType{}.Compare(_a.Points[i], _b.Points[i])
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

	// Lines must be the same
	return 0, nil
}

// Convert implements Type interface.
func (t LineStringType) Convert(v interface{}) (interface{}, error) {
	switch buf := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		line, err := GeometryType{}.Convert(buf)
		if ErrInvalidGISData.Is(err) {
			return nil, ErrInvalidGISData.New("LineStringType.Convert")
		}
		return line, err
	case string:
		return t.Convert([]byte(buf))
	case LineString:
		if err := t.MatchSRID(buf); err != nil {
			return nil, err
		}
		return buf, nil
	default:
		return nil, ErrSpatialTypeConversion.New()
	}
}

// Equals implements the Type interface.
func (t LineStringType) Equals(otherType Type) bool {
	_, ok := otherType.(LineStringType)
	return ok
}

// MaxTextResponseByteLength implements the Type interface
func (t LineStringType) MaxTextResponseByteLength() uint32 {
	return GeometryMaxByteLength
}

// Promote implements the Type interface.
func (t LineStringType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t LineStringType) SQL(ctx *Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, nil
	}

	buf := v.(LineString).Serialize()

	return sqltypes.MakeTrusted(sqltypes.Geometry, buf), nil
}

// String implements Type interface.
func (t LineStringType) String() string {
	return "linestring"
}

// Type implements Type interface.
func (t LineStringType) Type() query.Type {
	return sqltypes.Geometry
}

// ValueType implements Type interface.
func (t LineStringType) ValueType() reflect.Type {
	return lineStringValueType
}

// Zero implements Type interface.
func (t LineStringType) Zero() interface{} {
	return LineString{Points: []Point{{}, {}}}
}

// GetSpatialTypeSRID implements SpatialColumnType interface.
func (t LineStringType) GetSpatialTypeSRID() (uint32, bool) {
	return t.SRID, t.DefinedSRID
}

// SetSRID implements SpatialColumnType interface.
func (t LineStringType) SetSRID(v uint32) Type {
	t.SRID = v
	t.DefinedSRID = true
	return t
}

// MatchSRID implements SpatialColumnType interface
func (t LineStringType) MatchSRID(v interface{}) error {
	val, ok := v.(LineString)
	if !ok {
		return ErrNotLineString.New(v)
	}
	if !t.DefinedSRID {
		return nil
	} else if t.SRID == val.SRID {
		return nil
	}
	return ErrNotMatchingSRID.New(val.SRID, t.SRID)
}

// implementsGeometryValue implements GeometryValue interface.
func (l LineString) implementsGeometryValue() {}

// GetSRID implements GeometryValue interface.
func (l LineString) GetSRID() uint32 {
	return l.SRID
}

// SetSRID implements GeometryValue interface.
func (l LineString) SetSRID(srid uint32) GeometryValue {
	points := make([]Point, len(l.Points))
	for i, p := range l.Points {
		points[i] = p.SetSRID(srid).(Point)
	}
	return LineString{
		SRID:   srid,
		Points: points,
	}
}

// Serialize implements GeometryValue interface.
func (l LineString) Serialize() (buf []byte) {
	buf = allocateBuffer(len(l.Points), 1, 0)
	WriteEWKBHeader(buf, l.SRID, WKBLineID)
	l.WriteData(buf[EWKBHeaderSize:])
	return
}

// WriteData implements GeometryValue interface.
func (l LineString) WriteData(buf []byte) {
	writeCount(buf, uint32(len(l.Points)))
	buf = buf[CountSize:]
	for _, p := range l.Points {
		p.WriteData(buf)
		buf = buf[PointSize:]
	}
}
