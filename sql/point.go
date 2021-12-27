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
	"strconv"
	"strings"

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

//type pointTypeImpl struct{}

var Point PointType = &PointValue{}

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

	// Compare Y values
	if _a.Y > _b.Y {
		return 1, nil
	}
	if _a.Y < _b.Y {
		return -1, nil
	}

	// Points must be the same
	return 0, nil
}

func (t PointValue) convertToPointValue(v interface{}) (PointValue, error) {
	// TODO: this is what is called running UPDATE
	switch v := v.(type) {
	case PointValue:
		return v, nil
	case string:
		// TODO: janky parsing
		// get everything between parentheses
		v = v[6 : len(v)-1]
		s := strings.Split(v, ",")
		x, err := strconv.ParseFloat(s[0], 64)
		if err != nil {
			return PointValue{}, err
		}
		y, err := strconv.ParseFloat(s[1], 64)
		if err != nil {
			return PointValue{}, err
		}
		return PointValue{X: x, Y: y}, nil
	default:
		return PointValue{}, errors.New("can't convert to PointValue")
	}
}

// Convert implements Type interface.
func (t PointValue) Convert(v interface{}) (interface{}, error) {
	// Convert to string
	switch v := v.(type) {
	case PointValue:
		// TODO: this is what comes from displaying table
		return fmt.Sprintf("point(%f,%f)", v.X, v.Y), nil
	case string:
		// TODO: figure out why this statement also runs
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
	// TODO: this is what comes from point constructor
	return fmt.Sprintf("POINT(%f,%f)", t.X, t.Y), nil
}

// String implements Type interface.
func (t PointValue) String() string {
	// TODO: this is what prints on describe table
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
