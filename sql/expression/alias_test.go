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

func TestAliasWithChildrenPreservesMetadata(t *testing.T) {
	ctx := sql.NewEmptyContext()
	child := NewLiteral(int64(1), types.Int64)
	replacement := NewLiteral("one", types.Text)
	for _, unreferencable := range []bool{false, true} {
		alias := NewAlias(ctx, "elem", child).WithId(42).(*Alias)
		if unreferencable {
			alias = alias.AsUnreferencable()
		}
		rewritten, err := alias.WithChildren(ctx, replacement)
		require.NoError(t, err)
		updated := rewritten.(*Alias)
		require.Equal(t, sql.ColumnId(42), updated.Id())
		require.Equal(t, unreferencable, updated.Unreferencable())
		require.Equal(t, "elem", updated.Name())
		require.Same(t, replacement, updated.Child)
		require.Same(t, child, alias.Child)
	}
}
