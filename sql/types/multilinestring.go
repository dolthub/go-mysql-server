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

var _ sql.Type = MultiLineStringType{}
var _ sql.SpatialColumnType = MultiLineStringType{}
var _ GeometryValue = MultiLineString{}

// Compare implements Type interface.
func (t MultiLineStringType) Compare(a interface{}, b interface{}) (int, error) {
	return GeometryType{}.Compare(a, b)
}

// Convert implements Type interface.
func (t MultiLineStringType) Convert(v interface{}) (interface{}, error) {
	switch buf := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		mline, err := GeometryType{}.Convert(buf)
		if sql.ErrInvalidGISData.Is(err) {
			return nil, sql.ErrInvalidGISData.New("MultiLineString.Convert")
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
		return nil, sql.ErrSpatialTypeConversion.New()
	}
}

// Equals implements the Type interface.
func (t MultiLineStringType) Equals(otherType sql.Type) bool {
	_, ok := otherType.(MultiLineStringType)
	return ok
}

// MaxTextResponseByteLength implements the Type interface
func (t MultiLineStringType) MaxTextResponseByteLength() uint32 {
	return GeometryMaxByteLength
}

// Promote implements the Type interface.
func (t MultiLineStringType) Promote() sql.Type {
	return t
}

// SQL implements Type interface.
func (t MultiLineStringType) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
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
	return MultiLineString{Lines: []LineString{LineStringType{}.Zero().(LineString)}}
}

// GetSpatialTypeSRID implements SpatialColumnType interface.
func (t MultiLineStringType) GetSpatialTypeSRID() (uint32, bool) {
	return t.SRID, t.DefinedSRID
}

// SetSRID implements SpatialColumnType interface.
func (t MultiLineStringType) SetSRID(v uint32) sql.Type {
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
	return sql.ErrNotMatchingSRID.New(val.SRID, t.SRID)
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
	buf = AllocateGeoTypeBuffer(numPoints, len(p.Lines)+1, len(p.Lines))
	WriteEWKBHeader(buf, p.SRID, WKBMultiLineID)
	p.WriteData(buf[EWKBHeaderSize:])
	return
}

// WriteData implements GeometryValue interface.
func (p MultiLineString) WriteData(buf []byte) int {
	WriteCount(buf, uint32(len(p.Lines)))
	buf = buf[CountSize:]
	count := CountSize
	for _, l := range p.Lines {
		WriteWKBHeader(buf, WKBLineID)
		buf = buf[WKBHeaderSize:]
		c := l.WriteData(buf)
		buf = buf[c:]
		count += WKBHeaderSize + c
	}
	return count
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

// BBox implements GeometryValue interface.
func (p MultiLineString) BBox() (float64, float64, float64, float64) {
	minX, minY, maxX, maxY := math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64, -math.MaxFloat64
	for _, l := range p.Lines {
		lMinX, lMinY, lMaxX, lMaxY := l.BBox()
		minX = math.Min(minX, lMinX)
		minY = math.Min(minY, lMinY)
		maxX = math.Max(maxX, lMaxX)
		maxY = math.Max(maxY, lMaxY)
	}
	return minX, minY, maxX, maxY
}
