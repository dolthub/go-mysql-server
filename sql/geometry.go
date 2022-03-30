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
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"
)

// Represents the Geometry type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-geometry.html
type Geometry struct {
	Inner interface{} // Will be Point, Linestring, or Polygon
}

type GeometryType struct {
	InnerType Type // Will be PointType, LinestringType, or PolygonType
}

var _ Type = GeometryType{}

var ErrNotGeometry = errors.NewKind("Value of type %T is not a geometry")

// Compare implements Type interface.
func (t GeometryType) Compare(a any, b any) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// If b is a geometry type
	if bb, ok := b.(Geometry); ok {
		return t.Compare(a, bb.Inner)
	}

	// Expected to receive a geometry type
	switch this := a.(type) {
	case Point:
		return PointType{}.Compare(this, b)
	case Linestring:
		return LinestringType{}.Compare(this, b)
	case Polygon:
		return PolygonType{}.Compare(this, b)
	case Geometry:
		return t.Compare(this.Inner, b)
	default:
		return 0, ErrNotGeometry.New(a)
	}
}

// Convert implements Type interface.
func (t GeometryType) Convert(v interface{}) (interface{}, error) {
	// Must be a one of the Spatial Types
	switch this := v.(type) {
	case Point:
		return Geometry{Inner: this}, nil
	case Linestring:
		return Geometry{Inner: this}, nil
	case Polygon:
		return Geometry{Inner: this}, nil
	case Geometry:
		return this, nil
	default:
		return nil, ErrNotPoint.New(v) // TODO: change to be geometry error
	}
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

	pv, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, nil
	}

	val := appendAndSlice(dest, []byte(pv.(string)))

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

// Zero implements Type interface.
func (t GeometryType) Zero() interface{} {
	// TODO: it doesn't make sense for geometry to have a zero type
	return nil
}
