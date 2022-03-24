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
)

// Represents the Geometry type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-geometry.html
type Geometry interface {
	Point | Linestring | Polygon
}

type GeometryType[T PointType | LinestringType | PolygonType] struct {
	InnerType T
}

// Compare implements Type interface.
func (t GeometryType[T]) Compare(a any, b any) (int, error) {
	// TODO
	return 0, nil
}

// Convert implements Type interface.
func (t GeometryType[T]) Convert(v interface{}) (interface{}, error) {
	// TODO
	return v, nil
}

// Promote implements the Type interface.
func (t GeometryType[T]) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t GeometryType[T]) SQL(dest []byte, v interface{}) (sqltypes.Value, error) {
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
func (t GeometryType[T]) String() string {
	return "GEOMETRY"
}

// Type implements Type interface.
func (t GeometryType[T]) Type() query.Type {
	return sqltypes.Geometry
}

// Zero implements Type interface.
func (t GeometryType[T]) Zero() interface{} {
	// TODO: it doesn't make sense for geometry to have a zero type
	return nil
}
