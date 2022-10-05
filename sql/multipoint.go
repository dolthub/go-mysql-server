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

// MultiPointType represents the MULTIPOINT type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-multipoint.html
// The type of the returned value is MultiPoint.
type MultiPointType struct {
	SRID        uint32
	DefinedSRID bool
}

// MultiPoint is the value type returned from MultiPointType. Implements GeometryValue.
type MultiPoint struct {
	SRID   uint32
	Points []Point
}

var _ Type = MultiPointType{}
var _ SpatialColumnType = MultiPointType{}
var _ GeometryValue = MultiPoint{}

var (
	ErrNotMultiPoint = errors.NewKind("value of type %T is not a multipoint")

	multiPointValueType = reflect.TypeOf(MultiPoint{})
)

// Compare implements Type interface.
func (t MultiPointType) Compare(a interface{}, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// Expect to receive a MultiPoint, throw error otherwise
	_a, ok := a.(MultiPoint)
	if !ok {
		return 0, ErrNotMultiPoint.New(a)
	}
	_b, ok := b.(MultiPoint)
	if !ok {
		return 0, ErrNotMultiPoint.New(b)
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

	// MultiPoint must be the same
	return 0, nil
}

// Convert implements Type interface.
func (t MultiPointType) Convert(v interface{}) (interface{}, error) {
	switch buf := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		multipoint, err := GeometryType{}.Convert(buf)
		if err != nil {
			return nil, err
		}
		// TODO: is this even possible?
		if _, ok := multipoint.(MultiPoint); !ok {
			return nil, ErrInvalidGISData.New("MultiPointType.Convert")
		}
		return multipoint, nil
	case string:
		return t.Convert([]byte(buf))
	case MultiPoint:
		if err := t.MatchSRID(buf); err != nil {
			return nil, err
		}
		return buf, nil
	default:
		return nil, ErrSpatialTypeConversion.New()
	}
}

// Equals implements the Type interface.
func (t MultiPointType) Equals(otherType Type) bool {
	_, ok := otherType.(LineStringType)
	return ok
}

// MaxTextResponseByteLength implements the Type interface
func (t MultiPointType) MaxTextResponseByteLength() uint32 {
	return GeometryMaxByteLength
}

// Promote implements the Type interface.
func (t MultiPointType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t MultiPointType) SQL(ctx *Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, nil
	}

	buf := SerializeLineString(v.(LineString))

	return sqltypes.MakeTrusted(sqltypes.Geometry, buf), nil
}

// String implements Type interface.
func (t MultiPointType) String() string {
	return "linestring"
}

// Type implements Type interface.
func (t MultiPointType) Type() query.Type {
	return sqltypes.Geometry
}

// ValueType implements Type interface.
func (t MultiPointType) ValueType() reflect.Type {
	return multiPointValueType
}

// Zero implements Type interface.
func (t MultiPointType) Zero() interface{} {
	return MultiPoint{Points: []Point{{}}}
}

// GetSpatialTypeSRID implements SpatialColumnType interface.
func (t MultiPointType) GetSpatialTypeSRID() (uint32, bool) {
	return t.SRID, t.DefinedSRID
}

// SetSRID implements SpatialColumnType interface.
func (t MultiPointType) SetSRID(v uint32) Type {
	t.SRID = v
	t.DefinedSRID = true
	return t
}

// MatchSRID implements SpatialColumnType interface
func (t MultiPointType) MatchSRID(v interface{}) error {
	val, ok := v.(MultiPoint)
	if !ok {
		return ErrNotMultiPoint.New(v)
	}
	if !t.DefinedSRID {
		return nil
	} else if t.SRID == val.SRID {
		return nil
	}
	return ErrNotMatchingSRID.New(val.SRID, t.SRID)
}

// implementsGeometryValue implements GeometryValue interface.
func (p MultiPoint) implementsGeometryValue() {}

// GetSRID implements GeometryValue interface.
func (p MultiPoint) GetSRID() uint32 {
	return p.SRID
}

// SetSRID implements GeometryValue interface.
func (p MultiPoint) SetSRID(srid uint32) GeometryValue {
	return MultiPoint{
		SRID:   srid,
		Points: p.Points,
	}
}
