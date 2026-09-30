// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iters

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// countingExpr wraps an expression and counts Eval calls. Sort key expressions must be evaluated at most once
// per row: re-evaluating a non-deterministic expression (e.g. ORDER BY RAND()) on every comparison biases
// which row a top-1 scan returns.
type countingExpr struct {
	sql.Expression
	count *int
}

func (c *countingExpr) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	*c.count++
	return c.Expression.Eval(ctx, row)
}

func TestTopRowIterEvaluatesKeysOncePerRow(t *testing.T) {
	ctx := sql.NewEmptyContext()
	const numRows = 100

	var evalCount int
	sortConditions := sql.SortConditions{
		{
			Expr:         &countingExpr{expression.NewGetField(0, types.Int32, "col1", false), &evalCount},
			Order:        sql.Ascending,
			NullOrdering: sql.NullsFirst,
		},
	}

	rows := make([]sql.Row, numRows)
	for i := range rows {
		rows[i] = sql.NewRow(int32((i * 7) % numRows))
	}

	iter := NewTopRowIter(sortConditions, false, sql.RowsToRowIter(rows...))
	row, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, int32(0), row[0])
	require.Equal(t, numRows, evalCount)
}
