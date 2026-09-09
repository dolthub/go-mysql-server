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

package window

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/internal/exprtest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestNTileString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	buckets := expression.NewLiteral(2, types.Int64)
	expr := NewNTile(ctx, buckets)
	expr = expr.(sql.WindowAdaptableExpression).WithWindow(ctx, sql.NewWindowDefinition(nil, nil, nil, "", ""))
	require.Equal(t, "ntile(2) over ()", expr.String())
	parsed := exprtest.RequireFunction(t, exprtest.ParseExpression(t, expr))
	require.Equal(t, strings.ToLower(expr.(sql.FunctionExpression).FunctionName()), parsed.Name.Lowered())
	require.Len(t, parsed.Exprs, 1)
	exprtest.AssertExpressionValue(t, exprtest.RequireFunctionArgument(t, parsed, 0), buckets)
	require.NotNil(t, parsed.Over)
	require.Empty(t, parsed.Over.PartitionBy)
	require.Empty(t, parsed.Over.OrderBy)
	require.Nil(t, parsed.Over.Frame)
}
