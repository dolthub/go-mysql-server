// Copyright 2022 Dolthub, Inc.
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

package types

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/sql"
)

var tupleValueType = reflect.TypeOf((*[]interface{})(nil)).Elem()

type TupleType []sql.Type

var _ sql.Type = TupleType{nil}
var _ sql.CollationCoercible = TupleType{nil}

// CreateTuple returns a new tuple type with the given element types.
func CreateTuple(types ...sql.Type) sql.Type {
	return TupleType(types)
}

// Compare compares two tuples element-by-element. A definite non-equal
// element pair short-circuits. If every non-NULL pair matches but at least
// one element is NULL, returns sql.ErrNilOperand (SQL NULL for = / != / IN).
func (t TupleType) Compare(ctx context.Context, a interface{}, b interface{}) (int, error) {
	if hasNulls, res := CompareNulls(a, b); hasNulls {
		return res, nil
	}

	left, ok := a.([]interface{})
	if !ok {
		return 0, sql.ErrNotTuple.New(a)
	}
	right, ok := b.([]interface{})
	if !ok {
		return 0, sql.ErrNotTuple.New(b)
	}
	if len(left) != len(t) {
		return 0, sql.ErrInvalidColumnNumber.New(len(t), len(left))
	}
	if len(right) != len(t) {
		return 0, sql.ErrInvalidColumnNumber.New(len(t), len(right))
	}

	var sawNull bool
	for i := range left {
		if left[i] == nil || right[i] == nil {
			// Skip convert/compare: a NULL literal's type can make Convert of
			// the non-NULL side fail with ErrValueNotNil.
			sawNull = true
			continue
		}

		lv, _, err := t[i].Convert(ctx, left[i])
		if err != nil {
			return 0, err
		}
		rv, _, err := t[i].Convert(ctx, right[i])
		if err != nil {
			return 0, err
		}

		cmp, err := t[i].Compare(ctx, lv, rv)
		if err != nil {
			return 0, err
		}

		if cmp != 0 {
			return cmp, nil
		}
	}

	if sawNull {
		return 0, sql.ErrNilOperand.New()
	}

	return 0, nil
}

func (t TupleType) Convert(ctx context.Context, v interface{}) (interface{}, sql.ConvertInRange, error) {
	if v == nil {
		return nil, sql.InRange, nil
	}
	if vals, ok := v.([]interface{}); ok {
		if len(vals) != len(t) {
			return nil, sql.InRange, sql.ErrInvalidColumnNumber.New(len(t), len(vals))
		}

		var result = make([]interface{}, len(t))
		for i, typ := range t {
			var err error
			result[i], _, err = typ.Convert(ctx, vals[i])
			if err != nil && !sql.ErrTruncatedIncorrect.Is(err) {
				return nil, sql.InRange, err
			}
		}

		return result, sql.InRange, nil
	}
	return nil, sql.InRange, sql.ErrNotTuple.New(v)
}

// Equals implements the Type interface.
func (t TupleType) Equals(otherType sql.Type) bool {
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

// MaxTextResponseByteLength implements the Type interface
func (t TupleType) MaxTextResponseByteLength(*sql.Context) uint32 {
	// TupleTypes are never actually sent over the wire directly
	return 0
}

func (t TupleType) Promote() sql.Type {
	return t
}

func (t TupleType) SQL(*sql.Context, []byte, interface{}) (sqltypes.Value, error) {
	return sqltypes.Value{}, fmt.Errorf("unable to convert tuple type to SQL")
}

func (t TupleType) String() string {
	var elems = make([]string, len(t))
	for i, el := range t {
		elems[i] = el.String()
	}
	return fmt.Sprintf("tuple(%s)", strings.Join(elems, ", "))
}

func (t TupleType) Type() query.Type {
	return sqltypes.Expression
}

// ValueType implements Type interface.
func (t TupleType) ValueType() reflect.Type {
	return tupleValueType
}

func (t TupleType) Zero() interface{} {
	zeroes := make([]interface{}, len(t))
	for i, tt := range t {
		zeroes[i] = tt.Zero()
	}
	return zeroes
}

// CollationCoercibility implements sql.CollationCoercible interface.
func (TupleType) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}
