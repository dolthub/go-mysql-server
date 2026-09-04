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

	"github.com/dolthub/go-mysql-server/internal/exprtest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestLastInsertIdString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	withoutArg, err := NewLastInsertId(ctx)
	require.NoError(t, err)
	require.Equal(t, "last_insert_id()", withoutArg.String())
	exprtest.AssertStringRoundTrip(t, withoutArg.String())

	withArg, err := NewLastInsertId(ctx, expression.NewLiteral(42, types.Int64))
	require.NoError(t, err)
	require.Equal(t, "last_insert_id(42)", withArg.String())
	exprtest.AssertStringRoundTrip(t, withArg.String())
}
