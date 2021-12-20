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
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// Represents the Linestring type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-point.html
type LinestringType interface {
	Type
}

type LinestringValue struct {
	Points []PointValue
}

var Linestring LinestringType = LinestringValue{}

// Compare implements Type interface.
func (t LinestringValue) Compare(a interface{}, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// TODO: how to compare?
	return 0, nil
}

func convertLinestringToString(v LinestringValue) (string, error) {
	// Initialize array to accumulate arguments
	var parts []string
	for _, p := range v.Points {
		s, err := p.Convert(p) // TODO: this can't be right
		if err != nil {
			return "", err
		}
		parts = append(parts, s.(string))
	}
	return "linestring(" + strings.Join(parts, ",") + ")", nil
}

// Convert implements Type interface.
func (t LinestringValue) Convert(v interface{}) (interface{}, error) {
	switch v := v.(type) {
	// Already a LinestringValue do nothing
	case LinestringValue:
		// TODO: this is what comes from displaying table
		return v, nil
	// TODO: this is used for insert?
	// Decode string to linestring
	case string:
		val := v[len("linestring")+1 : len(v)-1]
		pStrings := strings.Split(val, "),")
		var points []PointValue
		for i, p := range pStrings {
			if i != len(pStrings)-1 {
				p = p + ")"
			}
			tmp := PointValue{}
			pv, err := tmp.Convert(p)
			if err != nil {
				return nil, err
			}

			points = append(points, pv.(PointValue))
		}
		return LinestringValue{points}, nil
	default:
		return nil, errors.New("Cannot convert to LinestringValue")
	}
}

// Promote implements the Type interface.
func (t LinestringValue) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t LinestringValue) SQL(v interface{}) (sqltypes.Value, error) {
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
func (t LinestringValue) ToString() (string, error) {
	// TODO: this is what comes from LineString constructor
	return convertLinestringToString(t)
}

// String implements Type interface.
func (t LinestringValue) String() string {
	// TODO: this is what prints on describe table
	return "LINESTRING"
}

// Type implements Type interface.
func (t LinestringValue) Type() query.Type {
	return sqltypes.Geometry
}

// Zero implements Type interface.
func (t LinestringValue) Zero() interface{} {
	return nil
}
