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
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// Represents the Point type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-point.html
type Polygon struct {
	SRID  uint32
	Lines []Linestring
}

type PolygonType struct{}

var _ Type = PolygonType{}

var ErrNotPolygon = errors.NewKind("value of type %T is not a polygon")

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
		diff, err := LinestringType{}.Compare(_a.Lines[i], _b.Lines[i])
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
	// Must be a Polygon, fail otherwise
	if v, ok := v.(Polygon); ok {
		return v, nil
	}

	return nil, ErrNotPolygon.New(v)
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

	return sqltypes.MakeTrusted(sqltypes.Geometry, []byte(lv.(string))), nil
}

// String implements Type interface.
func (t PolygonType) String() string {
	return "POLYGON"
}

// Type implements Type interface.
func (t PolygonType) Type() query.Type {
	return sqltypes.Geometry
}

// Zero implements Type interface.
func (t PolygonType) Zero() interface{} {
	return Polygon{Lines: []Linestring{{Points: []Point{{}, {}, {}, {}}}}}
}
