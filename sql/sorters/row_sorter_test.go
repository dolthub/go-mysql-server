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

package sorters

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// countingExpr wraps an expression and counts Eval calls. Sort key expressions must be evaluated at most once
// per row: re-evaluating a non-deterministic expression (e.g. ORDER BY RAND()) on every comparison biases the
// resulting order.
type countingExpr struct {
	sql.Expression
	count *int
}

func (c *countingExpr) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	*c.count++
	return c.Expression.Eval(ctx, row)
}

func testRows(n int) []sql.Row {
	rows := make([]sql.Row, n)
	for i := range rows {
		rows[i] = sql.NewRow(int32((i * 7) % n))
	}
	return rows
}

func countingSortConditions(count *int) sql.SortConditions {
	return sql.SortConditions{
		{
			Expr:         &countingExpr{expression.NewGetField(0, types.Int32, "col1", false), count},
			Order:        sql.Ascending,
			NullOrdering: sql.NullsFirst,
		},
	}
}

func TestRowSorterEvaluatesKeysOncePerRow(t *testing.T) {
	ctx := sql.NewEmptyContext()
	const numRows = 100

	var evalCount int
	rows := testRows(numRows)
	sorter := NewRowSorterWithRows(ctx, countingSortConditions(&evalCount), rows)
	sort.Stable(sorter)
	require.NoError(t, sorter.GetError())

	for i, row := range rows {
		require.Equal(t, int32(i), row[0])
	}
	require.LessOrEqual(t, evalCount, numRows)
}

func TestGetTopNRowsEvaluatesKeysOncePerRow(t *testing.T) {
	ctx := sql.NewEmptyContext()
	const numRows = 100

	var evalCount int
	iter := sql.RowsToRowIter(testRows(numRows)...)
	topRows, rowCount, err := GetTopNRows(ctx, iter, countingSortConditions(&evalCount), 5)
	require.NoError(t, err)
	require.Equal(t, int64(numRows), rowCount)

	require.Len(t, topRows, 5)
	for i, row := range topRows {
		require.Equal(t, int32(i), row[0])
	}
	require.LessOrEqual(t, evalCount, numRows)
}
