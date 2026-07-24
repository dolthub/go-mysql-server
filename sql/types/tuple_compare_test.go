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

package types

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

// Exercises the shipped CompareTupleValues helper (nicktobey #3640 design):
// shared element walk for type-level Compare and expression equality.
func TestCompareTupleValues_EqualityNilSemantics(t *testing.T) {
	ctx := context.Background()
	elem := TupleType{Int64, Int64}

	// (1,2)=(NULL,2) → cmp 0, hasNil true (indeterminate for expression layer)
	cmp, hasNil, err := CompareTupleValues(ctx, []interface{}{int64(1), int64(2)}, []interface{}{nil, int64(2)}, elem, true)
	require.NoError(t, err)
	require.Equal(t, 0, cmp)
	require.True(t, hasNil)

	// (1,2)=(NULL,3) → definite mismatch dominates nil
	cmp, hasNil, err = CompareTupleValues(ctx, []interface{}{int64(1), int64(2)}, []interface{}{nil, int64(3)}, elem, true)
	require.NoError(t, err)
	require.NotEqual(t, 0, cmp)
	require.True(t, hasNil)

	// (NULL,2)=(1,2) — no Convert-through-Null crash; indeterminate
	cmp, hasNil, err = CompareTupleValues(ctx, []interface{}{nil, int64(2)}, []interface{}{int64(1), int64(2)}, elem, true)
	require.NoError(t, err)
	require.Equal(t, 0, cmp)
	require.True(t, hasNil)

	// (1,2)=(1,2) equal, no nil
	cmp, hasNil, err = CompareTupleValues(ctx, []interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(2)}, elem, true)
	require.NoError(t, err)
	require.Equal(t, 0, cmp)
	require.False(t, hasNil)

	// (1,2)=(1,3) unequal, no nil
	cmp, hasNil, err = CompareTupleValues(ctx, []interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(3)}, elem, true)
	require.NoError(t, err)
	require.NotEqual(t, 0, cmp)
	require.False(t, hasNil)
}

func TestCompareTupleValues_TypePathNoNilOwnership(t *testing.T) {
	ctx := context.Background()
	elem := TupleType{Int64, Int64}

	// earlyReturnOnNil=false: type-level path does not report hasNil for equality
	cmp, hasNil, err := CompareTupleValues(ctx, []interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(2)}, elem, false)
	require.NoError(t, err)
	require.Equal(t, 0, cmp)
	require.False(t, hasNil)

	// TupleType.Compare uses the shared helper with earlyReturnOnNil=false
	tt := TupleType{Int64, Int64}
	cmp, err = tt.Compare(ctx, []interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(2)})
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	cmp, err = tt.Compare(ctx, []interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(9)})
	require.NoError(t, err)
	require.NotEqual(t, 0, cmp)
}

// Length mismatches must error — not false-equal (short left) or panic (long left).
func TestCompareTupleValues_LengthMismatch(t *testing.T) {
	ctx := context.Background()
	elem := TupleType{Int64, Int64}

	// Shorter left vs 2-element type: must not return cmp=0,err=nil
	cmp, hasNil, err := CompareTupleValues(ctx, []interface{}{int64(1)}, []interface{}{int64(1), int64(2)}, elem, true)
	require.Error(t, err)
	require.True(t, sql.ErrInvalidColumnNumber.Is(err))
	require.Equal(t, 0, cmp)
	require.False(t, hasNil)

	// Longer left: must error, not index OOB panic
	cmp, hasNil, err = CompareTupleValues(ctx, []interface{}{int64(1), int64(2), int64(3)}, []interface{}{int64(1), int64(2)}, elem, true)
	require.Error(t, err)
	require.True(t, sql.ErrInvalidColumnNumber.Is(err))
	require.Equal(t, 0, cmp)
	require.False(t, hasNil)

	// Shorter right
	_, _, err = CompareTupleValues(ctx, []interface{}{int64(1), int64(2)}, []interface{}{int64(1)}, elem, false)
	require.Error(t, err)
	require.True(t, sql.ErrInvalidColumnNumber.Is(err))

	// TupleType.Compare path
	tt := TupleType{Int64, Int64}
	_, err = tt.Compare(ctx, []interface{}{int64(1)}, []interface{}{int64(1), int64(2)})
	require.Error(t, err)
	require.True(t, sql.ErrInvalidColumnNumber.Is(err))

	_, err = tt.Compare(ctx, []interface{}{int64(1), int64(2), int64(3)}, []interface{}{int64(1), int64(2)})
	require.Error(t, err)
	require.True(t, sql.ErrInvalidColumnNumber.Is(err))
}
