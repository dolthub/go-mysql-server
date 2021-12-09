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

var (
	Geometry GeometryType = geometryType{}
)

// Represents the GEOMETRY type.
// https://dev.mysql.com/doc/refman/8.0/en/gis-class-geometry.html
type GeometryType interface {
	Type
	SRID() uint32
	Coordinates() []float64
	Interior() []GeometryType
	Boundary() []GeometryType
	Exterior() []GeometryType
	// MINX MINY, MAXX MINY, MAXX MAXY, MINX MAXY, MINX MINY
	MBR() (float64, float64, float64, float64, float64, float64, float64, float64)
	IsSimple() bool
	IsClosed() bool
	IsEmpty() bool
	Dimension() int // TODO: only valid return values are -1, 0, 1, 2
}

type geometryType struct {}

// Compare implements Type interface.
func (t geometryType) Compare(a interface{}, b interface{}) (int, error) {
	return 0, errors.New("Geometry Compare not implemented yet")
}

// Convert implements Type interface.
func (t geometryType) Convert(v interface{}) (interface{}, error) {
	return nil, errors.New("Geometry Convert not implemented yet")
}

// MustConvert implements the Type interface.
func (t geometryType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Promote implements the Type interface.
func (t geometryType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t geometryType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.MakeTrusted(sqltypes.Geometry, nil), nil
}

// String implements Type interface.
func (t geometryType) String() string {
	return "GEOMETRY"
}

// Type implements Type interface.
func (t geometryType) Type() query.Type {
	return sqltypes.Geometry
}

// Zero implements Type interface.
func (t geometryType) Zero() interface{} {
	return nil
}

// SRID implements GeometryType interface.
func (t geometryType) SRID() uint32 {
	return 0
}

// Coordinates implements GeometryType interface.
func (t geometryType) Coordinates() []float64 {
	return []float64{0}
}

// Interior implements GeometryType interface.
func (t geometryType) Interior() []GeometryType {
	return nil
}

// Boundary implements GeometryType interface.
func (t geometryType) Boundary() []GeometryType {
	return nil
}

// Exterior implements GeometryType interface.
func (t geometryType) Exterior() []GeometryType {
	return nil
}

// MBR implements GeometryType interface.
func (t geometryType) MBR() (float64, float64, float64, float64, float64, float64, float64, float64) {
	return 0,1,2,3,4,5,6,7
}

// IsSimple implements GeometryType interface.
func (t geometryType) IsSimple() bool {
	return false
}

// IsClosed implements GeometryType interface.
func (t geometryType) IsClosed() bool {
	return false
}

// IsEmpty implements GeometryType interface.
func (t geometryType) IsEmpty() bool{
	return false
}

// Dimension implements GeometryType interface.
func (t geometryType) Dimension() int {
	return -1
}
