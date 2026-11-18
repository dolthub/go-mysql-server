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

package json

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TestJSONExtractPathErrorIdentity preserves MySQL error metadata for objects and JSON null.
func TestJSONExtractPathErrorIdentity(t *testing.T) {
	ctx := sql.NewEmptyContext()
	path := expression.NewLiteral(`$.a\/b`, types.Text)
	for _, test := range []struct {
		name  string
		value interface{}
	}{
		{name: "object", value: types.JSONDocument{Val: types.JsonObject{"a/b": 5}}},
		{name: "JSON null", value: types.JSONDocument{Val: nil}},
	} {
		t.Run(test.name, func(t *testing.T) {
			extract, err := NewJSONExtract(ctx, expression.NewLiteral(test.value, types.JSON), path)
			require.NoError(t, err)
			_, err = extract.Eval(ctx, nil)
			require.True(t, sql.ErrInvalidJSONPath.Is(err))
			sqlErr := sql.CastSQLError(err)
			require.Equal(t, 3143, sqlErr.Num)
			require.Equal(t, "42000", sqlErr.State)
		})
	}
	t.Run("SQL null", func(t *testing.T) {
		extract, err := NewJSONExtract(ctx, expression.NewLiteral(nil, types.JSON), path)
		require.NoError(t, err)
		value, err := extract.Eval(ctx, nil)
		require.NoError(t, err)
		require.Nil(t, value)
	})
}
