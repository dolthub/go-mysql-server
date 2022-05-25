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

// Represents the Linestring type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-linestring.html
type Linestring struct {
	SRID   uint32
	Points []Point
}

type LinestringType struct {
	SRID        uint32
	DefinedSRID bool
}

var _ Type = LinestringType{}
var _ SpatialColumnType = LinestringType{}

var ErrNotLinestring = errors.NewKind("value of type %T is not a linestring")

// Compare implements Type interface.
func (t LinestringType) Compare(a interface{}, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// Expect to receive a Linestring, throw error otherwise
	_a, ok := a.(Linestring)
	if !ok {
		return 0, ErrNotLinestring.New(a)
	}
	_b, ok := b.(Linestring)
	if !ok {
		return 0, ErrNotLinestring.New(b)
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
func (t LinestringType) Convert(v interface{}) (interface{}, error) {
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
		if geomType != WKBLineID {
			return nil, err
		}
		// Parse data section
		line, err := WKBToLine(val[EWKBHeaderSize:], isBig, srid)
		if err != nil {
			return nil, err
		}
		return line, nil
	case string:
		return t.Convert([]byte(val))
	case Linestring:
		if err := t.MatchSRID(val); err != nil {
			return nil, err
		}
		return val, nil
	default:
		return nil, ErrSpatialTypeConversion.New()
	}
}

// Equals implements the Type interface.
func (t LinestringType) Equals(otherType Type) bool {
	_, ok := otherType.(LinestringType)
	return ok
}

// Promote implements the Type interface.
func (t LinestringType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t LinestringType) SQL(dest []byte, v interface{}) (sqltypes.Value, error) {
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
func (t LinestringType) String() string {
	return "LINESTRING"
}

// Type implements Type interface.
func (t LinestringType) Type() query.Type {
	return sqltypes.Geometry
}

// Zero implements Type interface.
func (t LinestringType) Zero() interface{} {
	return Linestring{Points: []Point{{}, {}}}
}

// GetSpatialTypeSRID implements SpatialColumnType interface.
func (t LinestringType) GetSpatialTypeSRID() (uint32, bool) {
	return t.SRID, t.DefinedSRID
}

// SetSRID implements SpatialColumnType interface.
func (t LinestringType) SetSRID(v uint32) Type {
	t.SRID = v
	t.DefinedSRID = true
	return t
}

// MatchSRID implements SpatialColumnType interface
func (t LinestringType) MatchSRID(v interface{}) error {
	val, ok := v.(Linestring)
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
