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
	Lines []LinestringValue
}

var Polygon PolygonType = PolygonValue{}

// Compare implements Type interface.
func (t PolygonValue) Compare(a interface{}, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// Cast to linestring
	_a, err := t.convertToPolygonValue(a)
	if err != nil {
		return 0, err
	}
	_b, err := t.convertToPolygonValue(b)
	if err != nil {
		return 0, err
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
		diff, err := LinestringValue{}.Compare(_a.Lines[i], _b.Lines[i])
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

	return 0, nil
}

func (t PolygonValue) convertToPolygonValue(v interface{}) (PolygonValue, error) {
	switch v := v.(type) {
	case PolygonValue:
		return v, nil
	case string:
		// TODO: janky parsing
		// get everything between parentheses
		v = v[len("polygon(") : len(v)-1]
		lineStrs := strings.Split(v, ",linestring")
		// convert into PointValues and append to array
		var lines []LinestringValue
		for i, s := range lineStrs {
			// Add back delimiter, except for first one
			if i != 0 {
				s = "linestring" + s
			}
			res, err := LinestringValue{}.convertToLinestringValue(s)
			if err != nil {
				return PolygonValue{}, err
			}
			lines = append(lines, res)
		}
		return PolygonValue{Lines: lines}, nil
	default:
		return PolygonValue{}, errors.New("can't convert to PolygonValue")
	}
}

func convertPolygonToString(v PolygonValue) (string, error) {
	// Initialize array to accumulate arguments
	var parts []string
	for _, l := range v.Lines {
		s, err := l.Convert(l)
		if err != nil {
			return "", err
		}
		parts = append(parts, s.(string))
	}
	return "polygon(" + strings.Join(parts, ",") + ")", nil
}

// Convert implements Type interface.
func (t PolygonValue) Convert(v interface{}) (interface{}, error) {
	// Convert each line into a string and join
	switch v := v.(type) {
	case PolygonValue:
		return convertPolygonToString(v)
	case string:
		return v, nil
	default:
		return nil, errors.New("Cannot convert to PolygonValue")
	}

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
	return convertPolygonToString(t)
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
