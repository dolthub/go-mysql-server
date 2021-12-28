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

// Represents the MultiPoint type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-multipoint.html
type MultiPointType interface {
	Type
}

type MultiPointValue struct {
	Points []PointValue
}

var MultiPoint MultiPointType = MultiPointValue{}

// Compare implements Type interface.
func (t MultiPointValue) Compare(a interface{}, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// Cast to MultiPoint
	_a, err := t.convertToMultiPointValue(a)
	if err != nil {
		return 0, err
	}
	_b, err := t.convertToMultiPointValue(b)
	if err != nil {
		return 0, err
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
		diff, err := PointValue{}.Compare(_a.Points[i], _b.Points[i])
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

func (t MultiPointValue) convertToMultiPointValue(v interface{}) (MultiPointValue, error) {
	switch v := v.(type) {
	case MultiPointValue:
		return v, nil
	case string:
		// TODO: janky parsing
		// get everything between parentheses
		v = v[len("multipoint(") : len(v)-1]
		pointStrs := strings.Split(v, ",point")
		// convert into PointValues and append to array
		var points []PointValue
		for i, s := range pointStrs {
			// Add back delimiter, except for first one
			if i != 0 {
				s = "point" + s
			}
			res, err := PointValue{}.convertToPointValue(s)
			if err != nil {
				return MultiPointValue{}, err
			}
			points = append(points, res)
		}
		return MultiPointValue{Points: points}, nil
	default:
		return MultiPointValue{}, errors.New("can't convert to MultiPointValue")
	}
}

func convertToMultiPointValue(v MultiPointValue) (string, error) {
	// Initialize array to accumulate arguments
	var parts []string
	for _, p := range v.Points {
		s, err := p.Convert(p)
		if err != nil {
			return "", err
		}
		parts = append(parts, s.(string))
	}
	return "multipoint(" + strings.Join(parts, ",") + ")", nil
}

// Convert implements Type interface.
func (t MultiPointValue) Convert(v interface{}) (interface{}, error) {
	// Convert to string
	switch v := v.(type) {
	case MultiPointValue:
		// TODO: this is what comes from displaying table
		return convertToMultiPointValue(v)
	// TODO: this is used for insert?
	case string:
		return v, nil
	default:
		return nil, errors.New("Cannot convert to MultiPointValue")
	}
}

// Promote implements the Type interface.
func (t MultiPointValue) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t MultiPointValue) SQL(v interface{}) (sqltypes.Value, error) {
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
func (t MultiPointValue) ToString() (string, error) {
	return convertToMultiPointValue(t)
}

// String implements Type interface.
func (t MultiPointValue) String() string {
	return "MULTIPOINT"
}

// Type implements Type interface.
func (t MultiPointValue) Type() query.Type {
	return sqltypes.Geometry
}

// Zero implements Type interface.
func (t MultiPointValue) Zero() interface{} {
	return nil
}
