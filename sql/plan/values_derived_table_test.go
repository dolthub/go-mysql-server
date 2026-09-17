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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// TestValueDerivedTableExtendedTypeSchema verifies that extended types choose their common type through the engine hook.
func TestValueDerivedTableExtendedTypeSchema(t *testing.T) {
	ctx := sql.NewEmptyContext()
	unknownType := sql.FakeExtendedType{Name: "unknown", ZeroVal: ""}
	intType := sql.FakeExtendedType{Name: "int4", ZeroVal: int32(0)}

	previousHook := sql.GetCommonExtendedType
	t.Cleanup(func() {
		sql.GetCommonExtendedType = previousHook
	})
	var hookCalled bool
	sql.GetCommonExtendedType = func(_ *sql.Context, sourceType, targetType sql.ExtendedType) sql.ExtendedType {
		hookCalled = true
		require.Equal(t, unknownType, sourceType)
		require.Equal(t, intType, targetType)
		return intType
	}

	rows := [][]sql.Expression{
		{expression.NewLiteral(nil, unknownType)},
		{expression.NewLiteral(int32(2), intType)},
	}
	schema := NewValueDerivedTable(ctx, NewValues(rows), "v").Schema(ctx)

	require.True(t, hookCalled)
	require.Len(t, schema, 1)
	require.Equal(t, intType, schema[0].Type)
	require.True(t, schema[0].Nullable)
}
