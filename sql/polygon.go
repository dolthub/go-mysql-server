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

// PolygonType represents the POLYGON type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-polygon.html
// The type of the returned value is Polygon.
type PolygonType struct {
	SRID        uint32
	DefinedSRID bool
}

// Polygon is the value type returned from PolygonType. Implements GeometryValue.
type Polygon struct {
	SRID  uint32
	Lines []LineString
}

var _ Type = PolygonType{}
var _ SpatialColumnType = PolygonType{}
var _ GeometryValue = Polygon{}

var (
	ErrNotPolygon = errors.NewKind("value of type %T is not a polygon")

	polygonValueType = reflect.TypeOf(Polygon{})
)

// Compare implements Type interface.
func (t PolygonType) Compare(a interface{}, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// Expect to receive a Polygon, throw error otherwise
	_a, ok := a.(Polygon)
	if !ok {
		return 0, ErrNotPolygon.New(a)
	}
	_b, ok := b.(Polygon)
	if !ok {
		return 0, ErrNotPolygon.New(b)
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

	// Polygons must be the same
	return 0, nil
}

// Convert implements Type interface.
func (t PolygonType) Convert(v interface{}) (interface{}, error) {
	// Allow null
	if v == nil {
		return nil, nil
	}
	// Handle conversions
	switch val := v.(type) {
	case []byte:
		// Parse header
		srid, isBig, geomType, err := ParseEWKBHeader(val)
		if err != nil {
			return nil, err
		}
		// Throw error if not marked as linestring
		if geomType != WKBPolyID {
			return nil, err
		}
		// Parse data section
		poly, err := WKBToPoly(val[EWKBHeaderSize:], isBig, srid)
		if err != nil {
			return nil, err
		}
		return poly, nil
	case string:
		return t.Convert([]byte(val))
	case Polygon:
		if err := t.MatchSRID(val); err != nil {
			return nil, err
		}
		return val, nil
	default:
		return nil, ErrSpatialTypeConversion.New()
	}
}

// Equals implements the Type interface.
func (t PolygonType) Equals(otherType Type) bool {
	_, ok := otherType.(PolygonType)
	return ok
}

// Promote implements the Type interface.
func (t PolygonType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t PolygonType) SQL(dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	lv, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, nil
	}

	//TODO: pretty sure this is wrong, lv is not a string type
	val := appendAndSliceString(dest, lv.(string))

	return sqltypes.MakeTrusted(sqltypes.Geometry, val), nil
}

// String implements Type interface.
func (t PolygonType) String() string {
	return "POLYGON"
}

// Type implements Type interface.
func (t PolygonType) Type() query.Type {
	return sqltypes.Geometry
}

// ValueType implements Type interface.
func (t PolygonType) ValueType() reflect.Type {
	return polygonValueType
}

// Zero implements Type interface.
func (t PolygonType) Zero() interface{} {
	return Polygon{Lines: []LineString{{Points: []Point{{}, {}, {}, {}}}}}
}

// GetSpatialTypeSRID implements SpatialColumnType interface.
func (t PolygonType) GetSpatialTypeSRID() (uint32, bool) {
	return t.SRID, t.DefinedSRID
}

// SetSRID implements SpatialColumnType interface.
func (t PolygonType) SetSRID(v uint32) Type {
	t.SRID = v
	t.DefinedSRID = true
	return t
}

// MatchSRID implements SpatialColumnType interface
func (t PolygonType) MatchSRID(v interface{}) error {
	val, ok := v.(Polygon)
	if !ok {
		return ErrNotPolygon.New(v)
	}
	if !t.DefinedSRID {
		return nil
	} else if t.SRID == val.SRID {
		return nil
	}
	return ErrNotMatchingSRID.New(val.SRID, t.SRID)
}

// implementsGeometryValue implements GeometryValue interface.
func (p Polygon) implementsGeometryValue() {}
