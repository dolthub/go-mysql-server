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
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

var Point PointType = pointType{}

// Represents the Point type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-point.html
type PointType interface {
	GeometryType
}

type pointType struct {}

// Compare implements Type interface.
func (t pointType) Compare(a interface{}, b interface{}) (int, error) {
	var err error
	if a, err = t.Convert(a); err != nil {
		return 0, err
	}
	if b, err = t.Convert(b); err != nil {
		return 0, err
	}
	return a.(PointValue).Compare(NewEmptyContext(), b.(PointValue))
}

// Convert implements Type interface.
func (t pointType) Convert(v interface{}) (interface{}, error) {
	return v.(PointValue), nil
}

// MustConvert implements the Type interface.
func (t pointType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Promote implements the Type interface.
func (t pointType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t pointType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	pv, ok := v.(PointValue)
	if !ok {
		return sqltypes.NULL, nil
	}

	s, err := pv.ToString(NewEmptyContext())
	if err != nil {
		return sqltypes.NULL, err
	}

	return sqltypes.MakeTrusted(sqltypes.Geometry, []byte(s)), nil
}

// String implements Type interface.
func (t pointType) String() string {
	return "POINT"
}

// Type implements Type interface.
func (t pointType) Type() query.Type {
	return sqltypes.Geometry
}

// Zero implements Type interface.
func (t pointType) Zero() interface{} {
	return nil
}

// SRID implements GeometryType interface.
func (t pointType) SRID() uint32 {
	return 0
}

// Coordinates implements GeometryType interface.
func (t pointType) Coordinates() []float64 {
	return []float64{0}
}

// Interior implements GeometryType interface.
func (t pointType) Interior() []GeometryType {
	return nil
}

// Boundary implements GeometryType interface.
func (t pointType) Boundary() []GeometryType {
	return nil
}

// Exterior implements GeometryType interface.
func (t pointType) Exterior() []GeometryType {
	return nil
}

// MBR implements GeometryType interface.
func (t pointType) MBR() (float64, float64, float64, float64, float64, float64, float64, float64) {
	return 0,1,2,3,4,5,6,7
}

// IsSimple implements GeometryType interface.
func (t pointType) IsSimple() bool {
	return false
}

// IsClosed implements GeometryType interface.
func (t pointType) IsClosed() bool {
	return false
}

// IsEmpty implements GeometryType interface.
func (t pointType) IsEmpty() bool{
	return false
}

// Dimension implements GeometryType interface.
func (t pointType) Dimension() int {
	return 0
}
