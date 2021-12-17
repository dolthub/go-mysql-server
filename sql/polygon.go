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
type PolygonType interface {
	Type
}

type PolygonValue struct {
	Lines []LineStringValue
}

var Polygon PolygonType = PolygonValue{}

// Compare implements Type interface.
func (t PolygonValue) Compare(a interface{}, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// TODO: how to compare?
	return 0, nil
}

func convertToString(v PolygonValue) (string, error) {
	// Initialize array to accumulate arguments
	var parts []string
	for _, p := range v.Lines {
		s, err := p.Convert(p) // TODO: this can't be right
		if err != nil {
			return "", errors.New("cannot convert to linestringvalue")
		}
		parts = append(parts, s.(string))
	}
	return strings.Join(parts, ","), nil
}

// Convert implements Type interface.
func (t PolygonValue) Convert(v interface{}) (interface{}, error) {
	// TODO: this is what comes from displaying table
	if val, ok := v.(PolygonValue); ok {
		return convertToString(val)
	}
	return nil, errors.New("Cannot convert to PolygonValue")
}

// Promote implements the Type interface.
func (t PolygonValue) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t PolygonValue) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	lv, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, nil
	}

	return sqltypes.MakeTrusted(sqltypes.Geometry, []byte(lv.(string))), nil
}

// ToString implements Type interface.
func (t PolygonValue) ToString() (string, error) {
	// TODO: this is what comes from Polygon constructor
	// TODO: use helper func
	return convertToString(t)
}

// String implements Type interface.
func (t PolygonValue) String() string {
	// TODO: this is what prints on describe table
	return "POLYGON"
}

// Type implements Type interface.
func (t PolygonValue) Type() query.Type {
	return sqltypes.Geometry
}

// Zero implements Type interface.
func (t PolygonValue) Zero() interface{} {
	return nil
}
