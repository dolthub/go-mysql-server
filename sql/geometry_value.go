// Copyright 2021 Dolthub, Inc.
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

// GeometryValue is an integrator specific implementation of a Geometry field value.
type GeometryValue interface {
	// Unmarshall converts a GeometryValue to a GeometryObject
	Unmarshall(ctx *Context) (val GeometryObject, err error)
	// Compare compares two GeometryValues. It maintains the same return value
	// semantics as Type.Compare()
	Compare(ctx *Context, v GeometryValue) (cmp int, err error)
	// ToString marshalls a GeometryValue to a valid Geometry-encoded string.
	ToString(ctx *Context) (string, error)
}

type GeometryObject struct {
	Val interface{}
}

var _ GeometryValue = GeometryObject{}

func (g GeometryObject) Unmarshall(ctx *Context) (GeometryObject, error) {
	return g, nil
}

func compareGeometry(a, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// TODO: type switch for type of geometry

	// TODO: implement this
	return 0, nil
}

func (g GeometryObject) Compare(ctx *Context, v GeometryValue) (int, error) {
	other, err := v.Unmarshall(ctx)
	if err != nil {
		return 0, err
	}
	return compareGeometry(g.Val, other.Val)
}

func (g GeometryObject) ToString(ctx *Context) (string, error) {
	return "need to implement a recursive method like json.Marshall", nil
}
