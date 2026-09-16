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

package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestWindowExpressionId(t *testing.T) {
	tCol := expression.NewGetFieldWithTable(0, 1, types.Int64, "", "t", "a.b", false)
	tDotACol := expression.NewGetFieldWithTable(1, 2, types.Int64, "", "t.a", "b", false)
	require.Equal(t, tCol.String(), tDotACol.String())

	window := sql.NewWindowDefinition(nil, nil, nil, "", "")
	first := aggregation.NewSum(tCol).WithWindow(nil, window).WithId(10).(sql.WindowAdaptableExpression)
	identical := aggregation.NewSum(tCol).WithWindow(nil, window).WithId(20).(sql.WindowAdaptableExpression)
	colliding := aggregation.NewSum(tDotACol).WithWindow(nil, window)
	require.Equal(t, sql.WindowExpressionId(first), sql.WindowExpressionId(identical))
	require.NotEqual(t, sql.WindowExpressionId(first), sql.WindowExpressionId(colliding))

	partitionedFirst := aggregation.NewSum(tCol).WithWindow(nil,
		sql.NewWindowDefinition([]sql.Expression{tCol}, nil, nil, "", ""))
	partitionedColliding := aggregation.NewSum(tCol).WithWindow(nil,
		sql.NewWindowDefinition([]sql.Expression{tDotACol}, nil, nil, "", ""))
	require.NotEqual(t, sql.WindowExpressionId(partitionedFirst), sql.WindowExpressionId(partitionedColliding))
	firstPartitionId, err := partitionedFirst.Window().PartitionId()
	require.NoError(t, err)
	collidingPartitionId, err := partitionedColliding.Window().PartitionId()
	require.NoError(t, err)
	require.NotEqual(t, firstPartitionId, collidingPartitionId)

	ascending := aggregation.NewSum(tCol).WithWindow(nil, sql.NewWindowDefinition(nil, sql.SortConditions{{
		Expr: tCol, Order: sql.Ascending, NullOrdering: sql.NullsFirst,
	}}, nil, "", ""))
	descending := aggregation.NewSum(tCol).WithWindow(nil, sql.NewWindowDefinition(nil, sql.SortConditions{{
		Expr: tCol, Order: sql.Descending, NullOrdering: sql.NullsFirst,
	}}, nil, "", ""))
	require.NotEqual(t, sql.WindowExpressionId(ascending), sql.WindowExpressionId(descending))
	nullsLast := aggregation.NewSum(tCol).WithWindow(nil, sql.NewWindowDefinition(nil, sql.SortConditions{{
		Expr: tCol, Order: sql.Ascending, NullOrdering: sql.NullsLast,
	}}, nil, "", ""))
	require.NotEqual(t, sql.WindowExpressionId(ascending), sql.WindowExpressionId(nullsLast))
}
