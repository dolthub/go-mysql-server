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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestCharSetHandler(t *testing.T) {
	t.Run("binary charset handler", func(t *testing.T) {
		h := NewCharSetHandler(sql.Collation_binary)

		l, err := h.NumChars("é")
		require.NoError(t, err)
		require.Equal(t, 2, l)

		pos, err := h.CharPos("é", 1)
		require.NoError(t, err)
		require.Equal(t, 1, pos)

		posEnd, err := h.CharPos("é", 5)
		require.NoError(t, err)
		require.Equal(t, 2, posEnd)
	})

	t.Run("utf8mb4 charset handler", func(t *testing.T) {
		h := NewCharSetHandler(sql.Collation_utf8mb4_0900_ai_ci)

		l, err := h.NumChars("é")
		require.NoError(t, err)
		require.Equal(t, 1, l)

		l4, err := h.NumChars("hé👍")
		require.NoError(t, err)
		require.Equal(t, 3, l4)

		pos, err := h.CharPos("éàü", 2)
		require.NoError(t, err)
		require.Equal(t, len("éà"), pos)

		posEmoji, err := h.CharPos("hé👍", 2)
		require.NoError(t, err)
		require.Equal(t, len("hé"), posEmoji)

		posEnd, err := h.CharPos("hé👍", 10)
		require.NoError(t, err)
		require.Equal(t, len("hé👍"), posEnd)
	})
}
