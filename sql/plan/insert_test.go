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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TestInsertExpressionsDoesNotMutatePlan verifies that enumerating an insert's expressions does not mutate the plan.
// The analyzer stores duplicate-update, predicate, and RETURNING expressions in slices backed by the same array.
// Expressions must therefore build an independent result instead of appending checks into that shared storage,
// which would overwrite the predicate or RETURNING expressions before execution.
func TestInsertExpressionsDoesNotMutatePlan(t *testing.T) {
	update := expression.NewSetField(
		expression.NewGetField(0, types.Int64, "value", false),
		expression.NewLiteral(int64(1), types.Int64),
	)
	predicate := expression.NewLiteral(true, types.Boolean)
	returning := expression.NewGetField(0, types.Int64, "value", false)
	check := expression.NewLiteral(true, types.Boolean)
	analyzedExpressions := []sql.Expression{update, predicate, returning}

	insert := &InsertInto{
		OnDupExprs: NewUpdateExprs(analyzedExpressions[:1], 1),
		checks:     sql.CheckConstraints{{Expr: check}},
		OnDupWhere: analyzedExpressions[1],
		Returning:  analyzedExpressions[2:],
	}

	require.Equal(t, []sql.Expression{update, check, predicate, returning}, insert.Expressions())
	require.Same(t, returning, insert.Returning[0])
}
