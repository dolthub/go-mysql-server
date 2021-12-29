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
	"errors"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// Represents the Linestring type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-linestring.html
type Linestring struct {
	Points []Point
}

type LinestringType struct{}

var _ Type = LinestringType{}

// Compare implements Type interface.
func (t LinestringType) Compare(a interface{}, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// Expect to receive a Linestring, throw error otherwise
	_a, ok := a.(Linestring)
	if !ok {
		return 0, errors.New("received a non-Linestring type") // TODO: turn this into a const error
	}
	_b, ok := b.(Linestring)
	if !ok {
		return 0, errors.New("received a non-Linestring type")
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
	// Must be a Linestring, fail otherwise
	if v, ok := v.(Linestring); ok {
		return v, nil
	}

	return nil, errors.New("can't convert to Linestring")
}

// Promote implements the Type interface.
func (t LinestringType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t LinestringType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	pv, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, nil
	}

	return sqltypes.MakeTrusted(sqltypes.Geometry, []byte(pv.(string))), nil
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
	return nil
}
