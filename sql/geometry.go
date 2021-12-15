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

/*import (
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"
)

var ErrConvertingToGeometry = errors.NewKind("value %v is not valid Geometry")

var Geometry GeometryType = geometryType{}

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

type geometryType struct{}

// Compare implements Type interface.
func (t geometryType) Compare(a interface{}, b interface{}) (int, error) {
	var err error
	if a, err = t.Convert(a); err != nil {
		return 0, err
	}
	if b, err = t.Convert(b); err != nil {
		return 0, err
	}
	// todo: making a context here is expensive
	return a.(GeometryValue).Compare(NewEmptyContext(), b.(GeometryValue))
}

// Convert implements Type interface.
func (t geometryType) Convert(v interface{}) (doc interface{}, err error) {
	switch v := v.(type) {
	case GeometryValue: // TODO: should be impossible
		return v, nil
	case PointValue:
		return GeometryObject{Val: v}, nil
	default:
		// TODO: write custom marshal function
	}
	return GeometryObject{Val: doc}, nil
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

	gs, ok := v.(GeometryValue)
	if !ok {
		return sqltypes.NULL, nil
	}

	// todo: making a context here is expensive
	s, err := gs.ToString(NewEmptyContext())
	if err != nil {
		return sqltypes.NULL, err
	}

	return sqltypes.MakeTrusted(sqltypes.Geometry, []byte(s)), nil
}

// String implements Type interface.
func (t geometryType) String() string {
	return "Geometry"
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
}*/
