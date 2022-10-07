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
	"gopkg.in/src-d/go-errors.v1"
	"reflect"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// MultiLineStringType represents the MUTILINESTRING type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-multilinestring.html
// The type of the returned value is MultiLineString.
type MultiLineStringType struct {
	SRID        uint32
	DefinedSRID bool
}

// MultiLineString is the value type returned from MultiLineStringType. Implements GeometryValue.
type MultiLineString struct {
	SRID  uint32
	Lines []LineString
}

var (
	ErrNotMultiLineString = errors.NewKind("value of type %T is not a multilinestring")

	multilinestringValueType = reflect.TypeOf(MultiLineString{})
)

var _ Type = MultiLineStringType{}
var _ SpatialColumnType = MultiLineStringType{}
var _ GeometryValue = MultiLineString{}

// Compare implements Type interface.
func (t MultiLineStringType) Compare(a interface{}, b interface{}) (int, error) {
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
func (t MultiLineStringType) Convert(v interface{}) (interface{}, error) {
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
func (t MultiLineStringType) Equals(otherType Type) bool {
	_, ok := otherType.(MultiLineStringType)
	return ok
}

// MaxTextResponseByteLength implements the Type interface
func (t MultiLineStringType) MaxTextResponseByteLength() uint32 {
	return GeometryMaxByteLength
}

// Promote implements the Type interface.
func (t MultiLineStringType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t MultiLineStringType) SQL(ctx *Context, dest []byte, v interface{}) (sqltypes.Value, error) {
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
func (t MultiLineStringType) String() string {
	return "multilinestring"
}

// Type implements Type interface.
func (t MultiLineStringType) Type() query.Type {
	return sqltypes.Geometry
}

// ValueType implements Type interface.
func (t MultiLineStringType) ValueType() reflect.Type {
	return multilinestringValueType
}

// Zero implements Type interface.
func (t MultiLineStringType) Zero() interface{} {
	return MultiLineString{Lines: []LineString{{Points: []Point{{}, {}}}}}
}

// GetSpatialTypeSRID implements SpatialColumnType interface.
func (t MultiLineStringType) GetSpatialTypeSRID() (uint32, bool) {
	return t.SRID, t.DefinedSRID
}

// SetSRID implements SpatialColumnType interface.
func (t MultiLineStringType) SetSRID(v uint32) Type {
	t.SRID = v
	t.DefinedSRID = true
	return t
}

// MatchSRID implements SpatialColumnType interface
func (t MultiLineStringType) MatchSRID(v interface{}) error {
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
func (p MultiLineString) implementsGeometryValue() {}

// GetSRID implements GeometryValue interface.
func (p MultiLineString) GetSRID() uint32 {
	return p.SRID
}

// SetSRID implements GeometryValue interface.
func (p MultiLineString) SetSRID(srid uint32) GeometryValue {
	lines := make([]LineString, len(p.Lines))
	for i, l := range p.Lines {
		lines[i] = l.SetSRID(srid).(LineString)
	}
	return MultiLineString{
		SRID:  srid,
		Lines: lines,
	}
}

// Serialize implements GeometryValue interface.
func (p MultiLineString) Serialize() (buf []byte) {
	var numPoints int
	for _, l := range p.Lines {
		numPoints += len(l.Points)
	}
	buf = allocateBuffer(numPoints, len(p.Lines)+1, len(p.Lines))
	WriteEWKBHeader(buf, p.SRID, WKBMLineID)
	p.WriteData(buf[EWKBHeaderSize:])
	return
}

// WriteData implements GeometryValue interface.
func (p MultiLineString) WriteData(buf []byte) {
	writeCount(buf, uint32(len(p.Lines)))
	buf = buf[CountSize:]
	for _, l := range p.Lines {
		WriteWKBHeader(buf, WKBLineID)
		buf = buf[WKBHeaderSize:]
		l.WriteData(buf)
		buf = buf[CountSize+PointSize*len(l.Points):]
	}
}

// Swap implements GeometryValue interface.
func (p MultiLineString) Swap() GeometryValue {
	lines := make([]LineString, len(p.Lines))
	for i, l := range p.Lines {
		lines[i] = l.Swap().(LineString)
	}
	return MultiLineString{
		SRID:  p.SRID,
		Lines: lines,
	}
}
