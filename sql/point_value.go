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

import (
	"fmt"
)

// PointValue is an integrator specific implementation of a Point field value.
type PointValue interface {
	// Unmarshall converts a PointValue to a PointObject
	Unmarshall(ctx *Context) (val PointObject, err error)
	// Compare compares two PointValues. It maintains the same return value
	// semantics as Type.Compare()
	Compare(ctx *Context, v PointValue) (cmp int, err error)
	// ToString marshalls a PointValue to a valid string.
	ToString(ctx *Context) (string, error)
}

type PointObject struct {
	X float64
	Y float64
}

var _ PointValue = PointObject{}

func (p PointObject) Unmarshall(ctx *Context) (PointObject, error) {
	return p, nil
}

func comparePoint(a, b interface{}) (int, error) {
	// Compare nulls
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	// Cast to PointObjects
	_a := a.(PointObject)
	_b := b.(PointObject)

	// Compare X values
	if _a.X > _b.X {
		return 1, nil
	}
	if _a.X < _b.X {
		return -1, nil
	}
	return 0, nil
}

func (p PointObject) Compare(ctx *Context, v PointValue) (int, error) {
	other, err := v.Unmarshall(ctx)
	if err != nil {
		return 0, err
	}
	return comparePoint(p, other)
}

func (p PointObject) ToString(ctx *Context) (string, error) {
	return fmt.Sprintf("(%f, %f)", p.X, p.Y), nil
}