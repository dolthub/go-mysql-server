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

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"
)

// PointType represents the POINT type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-point.html
// The type of the returned value is Point.
type PointType struct {
	SRID        uint32
	DefinedSRID bool
}

// Point is the value type returned from PointType. Implements GeometryValue.
type Point struct {
	SRID uint32
	X    float64
	Y    float64
}

var _ Type = PointType{}
var _ SpatialColumnType = PointType{}
var _ GeometryValue = Point{}

var (
	ErrNotPoint = errors.NewKind("value of type %T is not a point")

	pointValueType = reflect.TypeOf(Point{})
)

// Compare implements Type interface.
func (t PointType) Compare(a interface{}, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// Expect to receive a Point, throw error otherwise
	_a, ok := a.(Point)
	if !ok {
		return 0, ErrNotPoint.New(a)
	}
	_b, ok := b.(Point)
	if !ok {
		return 0, ErrNotPoint.New(b)
	}

	// Compare X values
	if _a.X > _b.X {
		return 1, nil
	}
	if _a.X < _b.X {
		return -1, nil
	}

	// Compare Y values
	if _a.Y > _b.Y {
		return 1, nil
	}
	if _a.Y < _b.Y {
		return -1, nil
	}

	// Points must be the same
	return 0, nil
}

// Convert implements Type interface.
func (t PointType) Convert(v interface{}) (interface{}, error) {
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
		// Throw error if not marked as point
		if geomType != WKBPointID {
			return nil, ErrInvalidGISData.New("PointType.Convert")
		}
		// Parse data section
		point, err := WKBToPoint(val[EWKBHeaderSize:], isBig, srid)
		if err != nil {
			return nil, err
		}
		return point, nil
	case string:
		return t.Convert([]byte(val))
	case Point:
		if err := t.MatchSRID(val); err != nil {
			return nil, err
		}
		return val, nil
	default:
		return nil, ErrSpatialTypeConversion.New()
	}
}

// Equals implements the Type interface.
func (t PointType) Equals(otherType Type) bool {
	_, ok := otherType.(PointType)
	return ok
}

// Promote implements the Type interface.
func (t PointType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t PointType) SQL(dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	pv, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, nil
	}

	//TODO: pretty sure this is wrong, pv is not a string type
	val := appendAndSliceString(dest, pv.(string))

	return sqltypes.MakeTrusted(sqltypes.Geometry, val), nil
}

// String implements Type interface.
func (t PointType) String() string {
	return "POINT"
}

// Type implements Type interface.
func (t PointType) Type() query.Type {
	return sqltypes.Geometry
}

// Zero implements Type interface.
func (t PointType) Zero() interface{} {
	return Point{X: 0.0, Y: 0.0}
}

// ValueType implements Type interface.
func (t PointType) ValueType() reflect.Type {
	return pointValueType
}

// GetSpatialTypeSRID implements SpatialColumnType interface.
func (t PointType) GetSpatialTypeSRID() (uint32, bool) {
	return t.SRID, t.DefinedSRID
}

// SetSRID implements SpatialColumnType interface.
func (t PointType) SetSRID(v uint32) Type {
	t.SRID = v
	t.DefinedSRID = true
	return t
}

// MatchSRID implements SpatialColumnType interface
func (t PointType) MatchSRID(v interface{}) error {
	val, ok := v.(Point)
	if !ok {
		return ErrNotPoint.New(v)
	}
	if !t.DefinedSRID {
		return nil
	} else if t.SRID == val.SRID {
		return nil
	}
	return ErrNotMatchingSRID.New(val.SRID, t.SRID)
}

// implementsGeometryValue implements GeometryValue interface.
func (p Point) implementsGeometryValue() {}
