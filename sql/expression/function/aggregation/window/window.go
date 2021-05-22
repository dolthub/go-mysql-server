// Copyright 2021 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func windowResolved(window *sql.Window) bool {
	if window == nil {
		return true
	}
	return expression.ExpressionsResolved(append(window.OrderBy.ToExpressions(), window.PartitionBy...)...)
}

func partitionsToSortFields(partitionExprs []sql.Expression) sql.SortFields {
	sfs := make(sql.SortFields, len(partitionExprs))
	for i, expr := range partitionExprs {
		sfs[i] = sql.SortField{
			Column: expr,
			Order:  sql.Ascending,
		}
	}
	return sfs
}

func evalExprs(ctx *sql.Context, exprs []sql.Expression, row sql.Row) (sql.Row, error) {
	result := make(sql.Row, len(exprs))
	for i, expr := range exprs {
		var err error
		result[i], err = expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// isNewPartition compares the order by columns between two rows, returning true when the last row is null
// or when next row is in a different partition than the last
func isNewPartition(ctx *sql.Context, partitionBy []sql.Expression, last sql.Row, row sql.Row) (bool, error) {
	if len(last) == 0 {
		return true, nil
	}

	if len(partitionBy) == 0 {
		return false, nil
	}

	lastExp, err := evalExprs(ctx, partitionBy, last)
	if err != nil {
		return false, err
	}

	thisExp, err := evalExprs(ctx, partitionBy, row)
	if err != nil {
		return false, err
	}

	for i := range lastExp {
		if lastExp[i] != thisExp[i] {
			return true, nil
		}
	}

	return false, nil
}

// isNewOrderValue compares the order by columns between two rows, returning true when the last row is null or
// when the next row's orderBy columns are unique
func isNewOrderValue(ctx *sql.Context, orderByExprs []sql.Expression, last sql.Row, row sql.Row) (bool, error) {
	if len(last) == 0 {
		return true, nil
	}

	lastExp, err := evalExprs(ctx, orderByExprs, last)
	if err != nil {
		return false, err
	}

	thisExp, err := evalExprs(ctx, orderByExprs, row)
	if err != nil {
		return false, err
	}

	for i := range lastExp {
		if lastExp[i] != thisExp[i] {
			return true, nil
		}
	}

	return false, nil
}
