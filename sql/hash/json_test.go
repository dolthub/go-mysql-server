// Copyright 2026 Dolthub, Inc.
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
	"context"
	"errors"
	"math"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TestJSONHashNumericEquality exercises both hashing paths against JSON's numeric comparison contract.
func TestJSONHashNumericEquality(t *testing.T) {
	decimal, _, err := apd.NewFromString("42.0")
	require.NoError(t, err)
	tests := []struct {
		name        string
		left, right any
		equal       bool
	}{
		{"integer and double", int64(42), float64(42), true},
		{"signed and unsigned", int64(42), uint64(42), true},
		{"negative zero", int64(0), math.Copysign(0, -1), true},
		{"integer widths", int8(-42), int32(-42), true},
		{"unsigned widths", uint8(42), uint32(42), true},
		{"native widths", int(42), uint(42), true},
		{"float32 exact promotion", float32(0.1), float64(float32(0.1)), true},
		{"float32 precision", float32(0.1), float64(0.1), false},
		{"decimal comparison", decimal, float64(42), true},
		{"large exact double", uint64(1) << 53, float64(1 << 53), true},
		{"large distinct integers", uint64(1) << 53, uint64(1)<<53 + 1, false},
		{"large rounded double", uint64(1)<<53 + 1, float64(1 << 53), false},
		{"minimum signed", int64(math.MinInt64), float64(math.MinInt64), true},
		{"maximum signed", int64(math.MaxInt64), uint64(math.MaxInt64), true},
		{"maximum signed rounded double", int64(math.MaxInt64), float64(math.MaxInt64), false},
		{"unsigned exact double", uint64(1) << 63, float64(1 << 63), true},
		{"maximum unsigned", uint64(math.MaxUint64), float64(math.MaxUint64), false},
		{"distinct types", "42", int64(42), false},
		{"null and text", nil, "null", false},
		{"boolean and text", true, "true", false},
		{"array boundaries", []any{int64(1), int64(23)}, []any{int64(12), int64(3)}, false},
		{"nested equality", map[string]any{"a": []any{int64(42), math.Copysign(0, -1)}}, map[string]any{"a": []any{float64(42), int64(0)}}, true},
		{"object key boundaries", map[string]any{"a": "bc"}, map[string]any{"ab": "c"}, false},
		{"object order", map[string]any{"a": int64(42), "b": true}, map[string]any{"b": true, "a": float64(42)}, true},
	}
	ctx := sql.NewEmptyContext()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmp, err := types.CompareJSON(ctx, tt.left, tt.right)
			require.NoError(t, err)
			require.Equal(t, tt.equal, cmp == 0)
			left := types.JSONDocument{Val: tt.left}
			right := types.JSONDocument{Val: tt.right}
			lh, err := HashOf(ctx, nil, sql.NewRow(left))
			require.NoError(t, err)
			rh, err := HashOf(ctx, nil, sql.NewRow(right))
			require.NoError(t, err)
			require.Equal(t, tt.equal, lh == rh)
			lsh, _, err := HashOfSimple(ctx, left, types.JSON)
			require.NoError(t, err)
			rsh, _, err := HashOfSimple(ctx, right, types.JSON)
			require.NoError(t, err)
			require.Equal(t, lh, lsh)
			require.Equal(t, rh, rsh)
		})
	}
}

// TestJSONHashWrappers verifies that storage wrappers use decoded JSON values and propagate decoding errors.
func TestJSONHashWrappers(t *testing.T) {
	ctx := sql.NewEmptyContext()
	expected, err := HashOf(ctx, nil, sql.NewRow(types.MustJSON(`{"a":[42,0]}`)))
	require.NoError(t, err)
	values := []any{
		types.MustJSON(`{"a":[42.0,-0.0]}`),
		types.NewLazyJSONDocument([]byte(`{"a":[42.0,-0.0]}`)),
		jsonAnyWrapper{value: types.NewLazyJSONDocument([]byte(`{"a":[42.0,-0.0]}`))},
	}
	for _, value := range values {
		rowHash, err := HashOf(ctx, nil, sql.NewRow(value))
		require.NoError(t, err)
		require.Equal(t, expected, rowHash)
		simpleHash, _, err := HashOfSimple(ctx, value, types.JSON)
		require.NoError(t, err)
		require.Equal(t, expected, simpleHash)
	}
	for _, value := range []any{
		types.NewLazyJSONDocument([]byte(`{`)),
		jsonAnyWrapper{err: errors.New("cannot load JSON")},
	} {
		_, err := HashOf(ctx, nil, sql.NewRow(value))
		require.Error(t, err)
		_, _, err = HashOfSimple(ctx, value, types.JSON)
		require.Error(t, err)
	}
}

// jsonAnyWrapper models a storage wrapper whose decoded value is a JSON document.
type jsonAnyWrapper struct {
	value any
	err   error
}

// UnwrapAny implements sql.AnyWrapper.
func (w jsonAnyWrapper) UnwrapAny(context.Context) (any, error) { return w.value, w.err }

// IsExactLength implements sql.AnyWrapper.
func (w jsonAnyWrapper) IsExactLength() bool { return false }

// MaxByteLength implements sql.AnyWrapper.
func (w jsonAnyWrapper) MaxByteLength() int64 { return 0 }

// Compare implements sql.AnyWrapper.
func (w jsonAnyWrapper) Compare(context.Context, any) (int, bool, error) { return 0, false, nil }

// Hash implements sql.AnyWrapper.
func (w jsonAnyWrapper) Hash() any { return nil }

// BenchmarkJSONHash measures hashing without constructing a second JSON object graph.
func BenchmarkJSONHash(b *testing.B) {
	ctx := sql.NewEmptyContext()
	elements := make([]any, 1000)
	for i := range elements {
		elements[i] = map[string]any{"n": int64(i), "f": float64(i), "s": "value"}
	}
	row := sql.NewRow(types.JSONDocument{Val: elements})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := HashOf(ctx, nil, row)
		if err != nil {
			b.Fatal(err)
		}
	}
}
