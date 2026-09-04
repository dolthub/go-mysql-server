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

	"github.com/dolthub/go-mysql-server/internal/exprtest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJsonLengthString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	expr, err := NewJsonLength(
		ctx,
		expression.NewGetField(0, types.JSON, "doc", false),
		expression.NewLiteral("$.items", types.Text),
	)
	require.NoError(t, err)
	require.Equal(t, "json_length(doc, '$.items')", expr.String())
	exprtest.AssertFunctionRoundTrip(t, expr.(sql.FunctionExpression))
}
