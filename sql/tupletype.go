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
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

type TupleType []Type

// CreateTuple returns a new tuple type with the given element types.
func CreateTuple(types ...Type) Type {
	return TupleType(types)
}

func (t TupleType) Compare(a, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	a, err := t.Convert(a)
	if err != nil {
		return 0, err
	}

	b, err = t.Convert(b)
	if err != nil {
		return 0, err
	}

	left := a.([]interface{})
	right := b.([]interface{})
	for i := range left {
		cmp, err := t[i].Compare(left[i], right[i])
		if err != nil {
			return 0, err
		}

		if cmp != 0 {
			return cmp, nil
		}
	}

	return 0, nil
}

func (t TupleType) Convert(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	if vals, ok := v.([]interface{}); ok {
		if len(vals) != len(t) {
			return nil, ErrInvalidColumnNumber.New(len(t), len(vals))
		}

		var result = make([]interface{}, len(t))
		for i, typ := range t {
			var err error
			result[i], err = typ.Convert(vals[i])
			if err != nil {
				return nil, err
			}
		}

		return result, nil
	}
	return nil, ErrNotTuple.New(v)
}

func (t TupleType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Equals implements the Type interface.
func (t TupleType) Equals(otherType Type) bool {
	if ot, ok := otherType.(TupleType); ok && len(t) == len(ot) {
		for i, tupType := range t {
			if !tupType.Equals(ot[i]) {
				return false
			}
		}
		return true
	}
	return false
}

func (t TupleType) Promote() Type {
	return t
}

func (t TupleType) SQL([]byte, interface{}) (sqltypes.Value, error) {
	return sqltypes.Value{}, fmt.Errorf("unable to convert tuple type to SQL")
}

func (t TupleType) String() string {
	var elems = make([]string, len(t))
	for i, el := range t {
		elems[i] = el.String()
	}
	return fmt.Sprintf("TUPLE(%s)", strings.Join(elems, ", "))
}

func (t TupleType) Type() query.Type {
	return sqltypes.Expression
}

func (t TupleType) Zero() interface{} {
	zeroes := make([]interface{}, len(t))
	for i, tt := range t {
		zeroes[i] = tt.Zero()
	}
	return zeroes
}
