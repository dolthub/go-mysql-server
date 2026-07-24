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

package expression

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Drives comparison.Compare for tuple operands through the real entry point
// (newComparison + Compare), proving expression-owned ErrNilOperand semantics.
func TestComparison_TupleNullEquality(t *testing.T) {
	ctx := sql.NewEmptyContext()

	eq := NewEquals(
		NewLiteral([]interface{}{int64(1), int64(2)}, types.CreateTuple(types.Int64, types.Int64)),
		NewLiteral([]interface{}{nil, int64(2)}, types.CreateTuple(types.Int64, types.Int64)),
	)
	v, err := eq.Eval(ctx, nil)
	require.NoError(t, err)
	require.Nil(t, v) // SQL NULL

	eqFalse := NewEquals(
		NewLiteral([]interface{}{int64(1), int64(2)}, types.CreateTuple(types.Int64, types.Int64)),
		NewLiteral([]interface{}{nil, int64(3)}, types.CreateTuple(types.Int64, types.Int64)),
	)
	v, err = eqFalse.Eval(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, false, v)

	// Operand swap crash regression: (NULL,2)=(1,2)
	eqSwap := NewEquals(
		NewLiteral([]interface{}{nil, int64(2)}, types.CreateTuple(types.Null, types.Int64)),
		NewLiteral([]interface{}{int64(1), int64(2)}, types.CreateTuple(types.Int64, types.Int64)),
	)
	v, err = eqSwap.Eval(ctx, nil)
	require.NoError(t, err)
	require.Nil(t, v)

	eqTrue := NewEquals(
		NewLiteral([]interface{}{int64(1), int64(2)}, types.CreateTuple(types.Int64, types.Int64)),
		NewLiteral([]interface{}{int64(1), int64(2)}, types.CreateTuple(types.Int64, types.Int64)),
	)
	v, err = eqTrue.Eval(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, true, v)
}
