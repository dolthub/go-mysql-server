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

package rowexec

import (
	mathrand "math/rand"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/iters"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *BaseBuilder) buildRandomSample(
	ctx *sql.Context,
	n *plan.RandomSample,
	row sql.Row,
) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.RandomSample")

	limit, err := iters.GetInt64Value(ctx, n.Limit)
	if err != nil || limit <= 0 {
		span.End()
		return sql.RowsToRowIter(), nil
	}

	count, err := n.Index.Count(ctx)
	if err != nil || count == 0 {
		span.End()
		return sql.RowsToRowIter(), nil
	}

	maxLimit := min(maxMemorySampleLimit(ctx), n.Index.MaxOrdinalSampleLimit(ctx, count))
	if limit > maxLimit {
		span.End()
		childIter, err := b.buildNodeExec(ctx, n.TableNode, row)
		if err != nil {
			return nil, err
		}
		randExpr, err := function.NewRand(ctx)
		if err != nil {
			return nil, err
		}
		sortConds := []sql.SortCondition{{
			Expr:  randExpr,
			Order: sql.Ascending,
		}}
		if limit == 1 {
			return iters.NewTopRowIter(sortConds, false, childIter), nil
		}
		return iters.NewTopRowsIter(sortConds, limit, false, childIter), nil
	}

	rng := mathrand.New(mathrand.NewSource(mathrand.Int63()))
	selected := make(map[uint64]struct{}, limit)
	ordinals := make([]uint64, 0, limit)

	for uint64(len(selected)) < uint64(limit) {
		r := uint64(rng.Int63n(int64(count)))
		if _, exists := selected[r]; !exists {
			selected[r] = struct{}{}
			ordinals = append(ordinals, r)
		}
	}

	lookup := sql.NewOrdinalIndexLookup(n.Index, ordinals...)
	indexedTable := n.Table.IndexedAccess(ctx, lookup)

	partIter, err := indexedTable.LookupPartitions(ctx, lookup)
	if err != nil {
		span.End()
		return nil, err
	}

	tableIter := sql.NewTableRowIter(ctx, indexedTable, partIter)
	return sql.NewSpanIter(span, tableIter), nil
}

// maxMemorySampleLimit calculates the maximum sample size that fits
// within MySQL's [sort_buffer_size] in |ctx|.
//
// Each selection map entry consumes ~24 bytes in the Go runtime.
//
// [sort_buffer_size]: https://dev.mysql.com/doc/refman/8.4/en/server-system-variables.html#sysvar_sort_buffer_size
func maxMemorySampleLimit(ctx *sql.Context) int64 {
	const defaultLimit = 1024
	if ctx == nil || ctx.Session == nil {
		return defaultLimit
	}
	val, err := ctx.GetSessionVariable(ctx, "sort_buffer_size")
	if err != nil || val == nil {
		return defaultLimit
	}
	bufSize, _, err := types.Int64.Convert(ctx, val)
	if err != nil {
		return defaultLimit
	}
	bytes, ok := bufSize.(int64)
	if !ok || bytes <= 0 {
		return defaultLimit
	}
	return max(1, bytes/24)
}
