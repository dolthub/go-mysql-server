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
	"fmt"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// Represents the Point type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-point.html
type PointType interface {
	Type
}

type PointValue struct {
	X float64
	Y float64
}

var Point PointType = PointValue{}

// Compare implements Type interface.
func (t PointValue) Compare(a interface{}, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// Cast to pointValue
	_a, err := t.convertToPointValue(a)
	if err != nil {
		return 0, err
	}
	_b, err := t.convertToPointValue(b)
	if err != nil {
		return 0, err
	}

	// Compare X values
	if _a.X > _b.X {
		return 1, nil
	}
	if _a.X < _b.X {
		return -1, nil
	}
	return 0, nil
}

func (t PointValue) convertToPointValue(v interface{}) (PointValue, error) {
	switch v := v.(type) {
	case PointValue:
		return v, nil
	default:
		return PointValue{}, errors.New("can't convert to pointValue")
	}
}

// Convert implements Type interface.
func (t PointValue) Convert(v interface{}) (interface{}, error) {
	// Convert to string
	switch v := v.(type) {
	case PointValue:
		return fmt.Sprintf("(%f,%f)", v.X, v.Y), nil
	case string:
		return v, nil
	default:
		return nil, errors.New("Cannot convert to PointValue")
	}
}

// Promote implements the Type interface.
func (t PointValue) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t PointValue) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	pv, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, nil
	}

	return sqltypes.MakeTrusted(sqltypes.Geometry, []byte(pv.(string))), nil
}

// ToString implements Type interface.
func (t PointValue) ToString() (string, error) {
	return fmt.Sprintf("POINT(%f, %f)", t.X, t.Y), nil
}

// String implements Type interface.
func (t PointValue) String() string {
	return "POINT"
}

// Type implements Type interface.
func (t PointValue) Type() query.Type {
	return sqltypes.Geometry
}

// Zero implements Type interface.
func (t PointValue) Zero() interface{} {
	return nil
}