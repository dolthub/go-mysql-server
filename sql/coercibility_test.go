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

package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type dummyCoercibleExpr struct {
	Expression
	collation    CollationID
	coercibility byte
}

func (d dummyCoercibleExpr) CollationCoercibility(ctx *Context) (CollationID, byte) {
	return d.collation, d.coercibility
}

func TestResolveCoercibility(t *testing.T) {
	ctx := NewEmptyContext()

	t.Run("empty expressions", func(t *testing.T) {
		col, coer := ResolveCoercibilityExpressions(ctx, 0)
		require.Equal(t, Collation_binary, col)
		require.Equal(t, CoercibilityIgnorable, coer)
	})

	t.Run("single expression", func(t *testing.T) {
		e := dummyCoercibleExpr{collation: Collation_latin1_swedish_ci, coercibility: CoercibilityCoercible}
		col, coer := ResolveCoercibilityExpressions(ctx, 0, e)
		require.Equal(t, Collation_latin1_swedish_ci, col)
		require.Equal(t, CoercibilityCoercible, coer)
	})

	t.Run("multiple expressions", func(t *testing.T) {
		e1 := dummyCoercibleExpr{collation: Collation_latin1_swedish_ci, coercibility: CoercibilityCoercible}
		e2 := dummyCoercibleExpr{collation: Collation_utf8mb4_0900_ai_ci, coercibility: CoercibilityNumeric}
		col, coer := ResolveCoercibilityExpressions(ctx, 0, e1, e2)
		require.Equal(t, Collation_latin1_swedish_ci, col)
		require.Equal(t, CoercibilityCoercible, coer)
	})

	t.Run("binary precedence at equal coercibility", func(t *testing.T) {
		col, coer := ResolveCoercibility(Collation_binary, CoercibilityImplicit, Collation_utf8mb4_0900_ai_ci, CoercibilityImplicit)
		require.Equal(t, Collation_binary, col)
		require.Equal(t, CoercibilityImplicit, coer)

		col2, coer2 := ResolveCoercibility(Collation_utf8mb4_0900_ai_ci, CoercibilityImplicit, Collation_binary, CoercibilityImplicit)
		require.Equal(t, Collation_binary, col2)
		require.Equal(t, CoercibilityImplicit, coer2)
	})

	t.Run("superset conversion at equal coercibility", func(t *testing.T) {
		col, coer := ResolveCoercibility(Collation_latin1_swedish_ci, CoercibilityImplicit, Collation_utf8mb4_0900_ai_ci, CoercibilityImplicit)
		require.Equal(t, Collation_utf8mb4_0900_ai_ci, col)
		require.Equal(t, CoercibilityImplicit, coer)

		col2, coer2 := ResolveCoercibility(Collation_utf8mb4_0900_ai_ci, CoercibilityImplicit, Collation_latin1_swedish_ci, CoercibilityImplicit)
		require.Equal(t, Collation_utf8mb4_0900_ai_ci, col2)
		require.Equal(t, CoercibilityImplicit, coer2)
	})

	t.Run("explicit conflict returns none", func(t *testing.T) {
		col, coer := ResolveCoercibility(Collation_latin1_swedish_ci, CoercibilityExplicit, Collation_utf8mb4_0900_ai_ci, CoercibilityExplicit)
		require.Equal(t, Collation_binary, col)
		require.Equal(t, CoercibilityNone, coer)
	})

	t.Run("same charset binary beats non-binary", func(t *testing.T) {
		col, coer := ResolveCoercibility(Collation_utf8mb4_bin, CoercibilityImplicit, Collation_utf8mb4_0900_ai_ci, CoercibilityImplicit)
		require.Equal(t, Collation_utf8mb4_bin, col)
		require.Equal(t, CoercibilityImplicit, coer)

		col2, coer2 := ResolveCoercibility(Collation_utf8mb4_0900_ai_ci, CoercibilityImplicit, Collation_utf8mb4_bin, CoercibilityImplicit)
		require.Equal(t, Collation_utf8mb4_bin, col2)
		require.Equal(t, CoercibilityImplicit, coer2)
	})

	t.Run("same charset two different binary collations conflict", func(t *testing.T) {
		col, coer := ResolveCoercibility(Collation_utf8mb4_bin, CoercibilityImplicit, Collation_utf8mb4_0900_bin, CoercibilityImplicit)
		require.Equal(t, Collation_utf8mb4_bin, col)
		require.Equal(t, CoercibilityNone, coer)
	})

	t.Run("same charset two different non-binary collations conflict", func(t *testing.T) {
		col, coer := ResolveCoercibility(Collation_utf8mb4_0900_ai_ci, CoercibilityImplicit, Collation_utf8mb4_general_ci, CoercibilityImplicit)
		require.Equal(t, Collation_utf8mb4_bin, col)
		require.Equal(t, CoercibilityNone, coer)
	})

	t.Run("collation IsBinary", func(t *testing.T) {
		require.True(t, Collation_binary.IsBinary())
		require.True(t, Collation_utf8mb4_bin.IsBinary())
		require.True(t, Collation_utf8mb4_0900_bin.IsBinary())
		require.True(t, Collation_latin1_bin.IsBinary())
		require.False(t, Collation_utf8mb4_0900_ai_ci.IsBinary())
		require.False(t, Collation_latin1_swedish_ci.IsBinary())
	})

	t.Run("all numeric expressions convert to connection collation", func(t *testing.T) {
		e1 := dummyCoercibleExpr{collation: Collation_binary, coercibility: CoercibilityNumeric}
		e2 := dummyCoercibleExpr{collation: Collation_binary, coercibility: CoercibilityNumeric}
		col, coer := ResolveCoercibilityExpressions(ctx, CoercibilityAllowNumericConv, e1, e2)
		require.Equal(t, Collation_Default, col)
		require.Equal(t, CoercibilityCoercible, coer)
	})

	t.Run("single numeric expression converts to connection collation", func(t *testing.T) {
		e1 := dummyCoercibleExpr{collation: Collation_binary, coercibility: CoercibilityNumeric}
		col, coer := ResolveCoercibilityExpressions(ctx, CoercibilityAllowNumericConv, e1)
		require.Equal(t, Collation_Default, col)
		require.Equal(t, CoercibilityCoercible, coer)
	})

	t.Run("mixed numeric and string expression retains string collation", func(t *testing.T) {
		eNum := dummyCoercibleExpr{collation: Collation_binary, coercibility: CoercibilityNumeric}
		eStr := dummyCoercibleExpr{collation: Collation_utf8mb4_0900_ai_ci, coercibility: CoercibilityCoercible}
		col, coer := ResolveCoercibilityExpressions(ctx, CoercibilityAllowNumericConv, eNum, eStr)
		require.Equal(t, Collation_utf8mb4_0900_ai_ci, col)
		require.Equal(t, CoercibilityCoercible, coer)
	})

	t.Run("charset superset conversion", func(t *testing.T) {
		col, coer := ResolveCoercibility(Collation_utf8mb4_0900_bin, CoercibilityImplicit, Collation_utf8mb3_general_ci, CoercibilitySysConst)
		require.Equal(t, Collation_utf8mb4_0900_bin, col)
		require.Equal(t, CoercibilityImplicit, coer)

		col, coer = ResolveCoercibility(Collation_utf8mb3_general_ci, CoercibilityImplicit, Collation_utf8mb4_0900_ai_ci, CoercibilityImplicit)
		require.Equal(t, Collation_utf8mb4_0900_ai_ci, col)
		require.Equal(t, CoercibilityImplicit, coer)

		col, coer = ResolveCoercibility(Collation_utf8mb4_0900_ai_ci, CoercibilityImplicit, Collation_latin1_swedish_ci, CoercibilityImplicit)
		require.Equal(t, Collation_utf8mb4_0900_ai_ci, col)
		require.Equal(t, CoercibilityImplicit, coer)
	})

	t.Run("CoercibilityFlags Has", func(t *testing.T) {
		flags := CoercibilityAllowNumericConv
		require.True(t, flags.Has(CoercibilityAllowNumericConv))
		require.False(t, CoercibilityFlags(0).Has(CoercibilityAllowNumericConv))
	})
}
