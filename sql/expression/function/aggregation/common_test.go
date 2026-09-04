// Copyright 2020-2021 Dolthub, Inc.
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

package aggregation

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func eval(t *testing.T, e sql.Expression, row sql.Row) interface{} {
	t.Helper()

	ctx := sql.NewEmptyContext()
	v, err := e.Eval(ctx, row)
	require.NoError(t, err)
	return v
}

func evalBuffer(t *testing.T, buf sql.AggregationBuffer) interface{} {
	t.Helper()

	ctx := sql.NewEmptyContext()
	v, err := buf.Eval(ctx)
	require.NoError(t, err)
	return v
}

func aggregate(t *testing.T, agg sql.Aggregation, rows ...sql.Row) interface{} {
	t.Helper()

	ctx := sql.NewEmptyContext()
	buf, _ := agg.NewBuffer(ctx)
	for _, row := range rows {
		require.NoError(t, buf.Update(ctx, row))
	}
	return evalBuffer(t, buf)
}

func TestGeneratedUnaryAggregateDescribe(t *testing.T) {
	ctx := sql.NewEmptyContext()
	hashIn, err := expression.NewHashInTuple(
		ctx,
		expression.NewGetField(0, types.Int64, "x", false),
		expression.NewTuple(expression.NewLiteral(1, types.Int64)),
	)
	require.NoError(t, err)

	aggregates := []sql.Expression{
		NewAnyValue(hashIn),
		NewAvg(hashIn),
		NewBitAnd(hashIn),
		NewBitOr(hashIn),
		NewBitXor(hashIn),
		NewCount(hashIn),
		NewFirst(hashIn),
		NewJsonArray(hashIn),
		NewLast(hashIn),
		NewMax(hashIn),
		NewMin(hashIn),
		NewSum(hashIn),
		NewStdDevPop(hashIn),
		NewStdDevSamp(hashIn),
		NewVarPop(hashIn),
		NewVarSamp(hashIn),
	}
	for _, aggregate := range aggregates {
		require.Contains(t, sql.Describe(ctx, aggregate, sql.DescribeOptions{Estimates: true}), "(x HASH IN (1))")
		require.Equal(t, sql.DebugString(ctx, aggregate), sql.Describe(ctx, aggregate, sql.DescribeOptions{Debug: true}))
	}

	windowed := NewAvg(expression.NewLiteral(1, types.Int64)).WithWindow(ctx, &sql.WindowDefinition{
		PartitionBy: []sql.Expression{hashIn},
	})
	require.Equal(t,
		"AVG(1) over ( partition by (x HASH IN (1)))",
		sql.Describe(ctx, windowed, sql.DescribeOptions{Estimates: true}),
	)
}
