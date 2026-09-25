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

	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestConvertUsingMalformedString(t *testing.T) {
	invalid := NewLiteral([]byte{0x61, 0xFF, 0x62}, types.LongBlob)

	t.Run("malformed utf8mb4 evaluates to NULL with a warning", func(t *testing.T) {
		ctx := sql.NewEmptyContext()
		result, err := NewConvertUsing(invalid, sql.CharacterSet_utf8mb4).Eval(ctx, nil)
		require.NoError(t, err)
		require.Nil(t, result)
		require.Equal(t, 1, len(ctx.Warnings()))
		require.Equal(t, mysql.ERInvalidCharacterString, ctx.Warnings()[0].Code)
		require.Contains(t, ctx.Warnings()[0].Message, "invalid string for character set")
	})

	t.Run("valid utf8mb4 is converted unchanged", func(t *testing.T) {
		ctx := sql.NewEmptyContext()
		valid := NewLiteral([]byte{0x61, 0xC3, 0xA9, 0x62}, types.LongBlob)
		result, err := NewConvertUsing(valid, sql.CharacterSet_utf8mb4).Eval(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, "aéb", result)
		require.Empty(t, ctx.Warnings())
	})

	t.Run("a binary target accepts malformed utf8mb4", func(t *testing.T) {
		ctx := sql.NewEmptyContext()
		result, err := NewConvertUsing(invalid, sql.CharacterSet_binary).Eval(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, "a\xffb", result)
		require.Empty(t, ctx.Warnings())
	})
}
