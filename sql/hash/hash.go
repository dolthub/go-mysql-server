// Copyright 2025 Dolthub, Inc.
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

package hash

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/apd/v3"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var digestPool = sync.Pool{
	New: func() any {
		return xxhash.New()
	},
}

// Hashable is implemented by values that can have more than one representation that compares as equal, such as
// intervals in which "1 day" equals "24 hours". HashOf and HashOfSimple hash these values with WriteHashKey rather than
// with their printed form, so that values that compare as equal hash the same. Slices whose elements are Hashable are
// hashed element by element.
type Hashable interface {
	// WriteHashKey writes a canonical representation of the value to |w|. Values that compare as equal must write
	// identical bytes.
	WriteHashKey(ctx *sql.Context, w io.Writer) error
}

var hashableType = reflect.TypeOf((*Hashable)(nil)).Elem()

// ExprsToSchema converts a list of sql.Expression to a sql.Schema.
// This is used for functions that use HashOf, but don't already have a schema.
// The generated schema ONLY contains the types of the expressions without any column names or any other info.
func ExprsToSchema(ctx *sql.Context, exprs ...sql.Expression) sql.Schema {
	var sch sql.Schema
	for _, expr := range exprs {
		sch = append(sch, &sql.Column{Type: expr.Type(ctx)})
	}
	return sch
}

// HashOf returns a hash of the given row to be used as key in a cache.
func HashOf(ctx *sql.Context, sch sql.Schema, row sql.Row) (uint64, error) {
	hash := digestPool.Get().(*xxhash.Digest)
	hash.Reset()
	defer digestPool.Put(hash)
	for i, v := range row {
		if i > 0 {
			// separate each value in the row with a nil byte
			if _, err := hash.Write([]byte{0}); err != nil {
				return 0, err
			}
		}

		v, err := sql.UnwrapAny(ctx, v)
		if err != nil {
			return 0, fmt.Errorf("error unwrapping value: %w", err)
		}

		if v == nil {
			if _, err := hash.WriteString("<nil>"); err != nil {
				return 0, err
			}
			continue
		}

		if len(row) > 1 {
			var length int
			switch v := v.(type) {
			case string:
				length = len(v)
			case []byte:
				length = len(v)
			}
			if length > 0 {
				var lenBuf [8]byte
				binary.LittleEndian.PutUint64(lenBuf[:], uint64(length))
				if _, err := hash.Write(lenBuf[:]); err != nil {
					return 0, err
				}
			}
		}

		// TODO: we may not always have the type information available, so we check schema length.
		//  Then, defer to original behavior
		if i < len(sch) {
			switch typ := sch[i].Type.(type) {
			case types.StringType:
				var strVal string
				strVal, _, err = types.ConvertToString(ctx, v, typ, nil)
				if err != nil {
					return 0, err
				}
				err = typ.Collation().WriteWeightString(hash, strVal)
				if err != nil {
					return 0, err
				}
				continue
			}
		}
		switch v := v.(type) {
		case int:
			_, err = hash.WriteString(strconv.FormatInt(int64(v), 10))
		case int8:
			_, err = hash.WriteString(strconv.FormatInt(int64(v), 10))
		case int16:
			_, err = hash.WriteString(strconv.FormatInt(int64(v), 10))
		case int32:
			_, err = hash.WriteString(strconv.FormatInt(int64(v), 10))
		case int64:
			_, err = hash.WriteString(strconv.FormatInt(v, 10))
		case uint:
			_, err = hash.WriteString(strconv.FormatUint(uint64(v), 10))
		case uint8:
			_, err = hash.WriteString(strconv.FormatUint(uint64(v), 10))
		case uint16:
			_, err = hash.WriteString(strconv.FormatUint(uint64(v), 10))
		case uint32:
			_, err = hash.WriteString(strconv.FormatUint(uint64(v), 10))
		case uint64:
			_, err = hash.WriteString(strconv.FormatUint(v, 10))
		case float32:
			str := strconv.FormatFloat(float64(v), 'f', -1, 32)
			if str == "-0" {
				str = "0"
			}
			_, err = hash.WriteString(str)
		case float64:
			str := strconv.FormatFloat(v, 'f', -1, 64)
			if str == "-0" {
				str = "0"
			}
			_, err = hash.WriteString(str)
		case *apd.Decimal:
			_, err = hash.WriteString(decimalHashString(v))
		case string:
			_, err = hash.WriteString(v)
		case []byte:
			_, err = hash.Write(v)
		case time.Time:
			err = writeTimeHash(hash, v)
		case Hashable:
			err = v.WriteHashKey(ctx, hash)
		case []any:
			err = writeSliceHash(ctx, hash, v)
		default:
			if elems, ok := hashableSliceElements(v); ok {
				err = writeSliceHash(ctx, hash, elems)
			} else {
				_, err = hash.WriteString(fmt.Sprintf("%v", v))
			}
		}
		if err != nil {
			return 0, err
		}
	}
	return hash.Sum64(), nil
}

// HashOfSimple returns a hash for a single interface value
func HashOfSimple(ctx *sql.Context, i any, t sql.Type) (uint64, sql.ConvertInRange, error) {
	if i == nil {
		return 0, sql.InRange, nil
	}

	var str string
	var inRange sql.ConvertInRange
	if types.IsTextOnly(t) {
		// Collated strings that are equivalent may have different runes, so we must make them hash to the same value
		coll := t.(sql.StringType).Collation()
		if s, ok := i.(string); ok {
			str = s
		} else {
			converted, _, err := types.ConvertOrTruncate(ctx, i, t)
			if err != nil {
				return 0, sql.InRange, err
			}
			str, _, err = sql.Unwrap[string](ctx, converted)
			if err != nil {
				return 0, sql.InRange, err
			}
		}
		if types.IsChar(t) {
			str = strings.TrimRight(str, " ")
		}
		h, err := coll.HashToUint(str)
		if err != nil {
			return 0, sql.InRange, err
		}
		return h, inRange, nil
	}

	hash := digestPool.Get().(*xxhash.Digest)
	hash.Reset()
	defer digestPool.Put(hash)

	if types.IsTuple(t) {
		tup := i.([]any)
		tupType := t.(types.TupleType)
		hashes := make([]uint64, len(tup))
		for idx, v := range tup {
			// TODO: handle out of range conversions here?
			h, _, err := HashOfSimple(ctx, v, tupType[idx])
			if err != nil {
				return 0, sql.InRange, err
			}
			hashes[idx] = h
		}
		str = fmt.Sprintf("%v", hashes) // TODO: avoid Sprintf for performance
		_, err := hash.WriteString(str)
		if err != nil {
			return 0, sql.InRange, err
		}
		return hash.Sum64(), inRange, nil
	}

	var val any
	var err error
	val, inRange, err = types.ConvertOrTruncate(ctx, i, t.Promote())
	if err != nil {
		return 0, sql.InRange, err
	}

	// TODO: use strconv.Append...() and a byte buffer
	switch v := val.(type) {
	case time.Time:
		if err = writeTimeHash(hash, v); err != nil {
			return 0, sql.InRange, err
		}
		return hash.Sum64(), inRange, nil
	case Hashable:
		if err = v.WriteHashKey(ctx, hash); err != nil {
			return 0, sql.InRange, err
		}
		return hash.Sum64(), inRange, nil
	case []any:
		if err = writeSliceHash(ctx, hash, v); err != nil {
			return 0, sql.InRange, err
		}
		return hash.Sum64(), inRange, nil
	case int:
		str = strconv.FormatInt(int64(v), 10)
	case int8:
		str = strconv.FormatInt(int64(v), 10)
	case int16:
		str = strconv.FormatInt(int64(v), 10)
	case int32:
		str = strconv.FormatInt(int64(v), 10)
	case int64:
		str = strconv.FormatInt(v, 10)
	case uint:
		str = strconv.FormatUint(uint64(v), 10)
	case uint8:
		str = strconv.FormatUint(uint64(v), 10)
	case uint16:
		str = strconv.FormatUint(uint64(v), 10)
	case uint32:
		str = strconv.FormatUint(uint64(v), 10)
	case uint64:
		str = strconv.FormatUint(v, 10)
	case float32:
		str = strconv.FormatFloat(float64(v), 'f', -1, 32)
		if str == "-0" {
			str = "0"
		}
	case float64:
		str = strconv.FormatFloat(v, 'f', -1, 64)
		if str == "-0" {
			str = "0"
		}
	case *apd.Decimal:
		str = decimalHashString(v)
	default:
		str = fmt.Sprintf("%v", v) // TODO: avoid Sprintf for performance
	}

	_, err = hash.WriteString(str)
	if err != nil {
		return 0, sql.InRange, err
	}
	return hash.Sum64(), inRange, nil
}

// decimalHashString returns a normalized decimal string used for hashing.
// Trailing 0s and trailing decimal points are removed. Negative 0 is normalized to positive 0.
func decimalHashString(v *apd.Decimal) string {
	res := new(apd.Decimal)
	res, _ = res.Reduce(v)
	return res.Text('f')
}

// HashesCanonically returns whether HashOf hashes |v| the same as every value that compares as equal to it, without
// needing to know its type. This holds for Hashable values, decimals, times, slices of Hashable values, and slices of
// such values and other primitives.
// Callers that otherwise hash a value's serialized form, which can differ for equal values, should prefer HashOf for
// these values.
func HashesCanonically(v any) bool {
	switch v := v.(type) {
	case Hashable, *apd.Decimal, time.Time:
		return true
	case []any:
		for _, elem := range v {
			switch elem.(type) {
			case nil, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string, []byte:
				continue
			}

			if !HashesCanonically(elem) {
				return false
			}
		}
		return true
	default:
		_, ok := hashableSliceElements(v)
		return ok
	}
}

// writeTimeHash writes the instant of |t| to |w|, so that times that are equal hash the same regardless of their
// location.
func writeTimeHash(w io.Writer, t time.Time) error {
	var buf [12]byte
	binary.LittleEndian.PutUint64(buf[:8], uint64(t.Unix()))
	binary.LittleEndian.PutUint32(buf[8:], uint32(t.Nanosecond()))
	_, err := w.Write(buf[:])
	return err
}

// hashableSliceElements returns the elements of |v| when it is a slice of a type that implements Hashable, such as a
// slice of an integrator's own value type.
func hashableSliceElements(v any) ([]any, bool) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice || !rv.Type().Elem().Implements(hashableType) {
		return nil, false
	}

	elems := make([]any, rv.Len())
	for i := range elems {
		elems[i] = rv.Index(i).Interface()
	}
	return elems, true
}

// writeSliceHash writes the hash of each element of |vals| to |w|, so that slices whose elements compare as equal hash
// the same even when the elements have different printed forms.
func writeSliceHash(ctx *sql.Context, w io.Writer, vals []any) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(len(vals)))
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}

	for _, v := range vals {
		h, err := HashOf(ctx, nil, sql.Row{v})
		if err != nil {
			return err
		}

		binary.LittleEndian.PutUint64(buf[:], h)
		if _, err = w.Write(buf[:]); err != nil {
			return err
		}
	}
	return nil
}
