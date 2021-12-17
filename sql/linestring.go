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

// Represents the Point type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-point.html
type LineStringType interface {
	Type
}

type LineStringValue struct {
	Points []PointValue //TODO: PointType or PointValue?
}

var LineString LineStringType = LineStringValue{}

// Compare implements Type interface.
func (t LineStringValue) Compare(a interface{}, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// TODO: how to compare?
	return 0, nil
}

// Convert implements Type interface.
func (t LineStringValue) Convert(v interface{}) (interface{}, error) {
	// Initialize array to accumulate arguments
	var parts []string

	// Convert each point into a string
	switch v := v.(type) {
	case LineStringValue:
		// TODO: this is what comes from displaying table
		for _, p := range v.Points {
			s, err := p.Convert(p) // TODO: this can't be right
			if err != nil {
				return nil, errors.New("cannot convert to linestringvalue")
			}
			parts = append(parts, s.(string))
		}
		return strings.Join(parts, ","), nil
	default:
		return nil, errors.New("Cannot convert to PointValue")
	}
}

// Promote implements the Type interface.
func (t LineStringValue) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t LineStringValue) SQL(v interface{}) (sqltypes.Value, error) {
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
func (t LineStringValue) ToString() (string, error) {
	// TODO: this is what comes from LineString constructor
	// TODO: use string
	var parts []string
	for _, p := range t.Points {
		s, err := p.Convert(p) // TODO: this can't be right
		if err != nil {
			return "", errors.New("cannot convert to linestringvalue")
		}
		parts = append(parts, s.(string))
	}
	return strings.Join(parts, ","), nil
}

// String implements Type interface.
func (t LineStringValue) String() string {
	// TODO: this is what prints on describe table
	return "LINESTRING"
}

// Type implements Type interface.
func (t LineStringValue) Type() query.Type {
	return sqltypes.Geometry
}

// Zero implements Type interface.
func (t LineStringValue) Zero() interface{} {
	return nil
}
