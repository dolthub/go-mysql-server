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

package analyzer

import (
	"math"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

func TestTopNPaginationBounds(t *testing.T) {
	lit := func(n int64) sql.Expression { return expression.NewLiteral(n, types.Int64) }
	for _, tt := range []struct {
		name          string
		limit, offset sql.Expression
	}{
		{"ordinary", lit(2), lit(1)},
		{"exact boundary", lit(math.MaxInt64 - 1), lit(1)},
		{"zero offset", lit(math.MaxInt64), lit(0)},
		{"overflow", lit(math.MaxInt64), lit(1)},
		{"finite overflow", lit(math.MaxInt64 - 1), lit(2)},
		{"huge offset", lit(1), lit(math.MaxInt64)},
		{"both huge", lit(math.MaxInt64), lit(math.MaxInt64)},
		{"deferred limit", expression.NewBindVar("lim"), lit(1)},
		{"deferred offset", lit(1), expression.NewBindVar("off")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, project := range []bool{false, true} {
				for _, calcFoundRows := range []bool{false, true} {
					var child sql.Node = plan.NewSort(nil, plan.NewUnresolvedTable("t", ""))
					if project {
						child = plan.NewProject(sql.NewEmptyContext(), nil, child)
					}
					input := plan.NewLimit(tt.limit, plan.NewOffset(tt.offset, child)).WithCalcFoundRows(calcFoundRows)
					result, same, err := insertTopNNodes(sql.NewEmptyContext(), nil, input, nil, nil, nil)
					require.NoError(t, err)

					require.Equal(t, transform.NewTree, same)
					if project {
						result = result.(*plan.Limit).Child.(*plan.Project).Child
					}
					top := result.(*plan.Offset).Child.(*plan.TopN)
					sum := top.Limit.(*expression.RowCountSum)
					require.Same(t, tt.limit, sum.LeftChild)
					require.Same(t, tt.offset, sum.RightChild)
					require.Equal(t, calcFoundRows, top.CalcFoundRows)
				}
			}
		})
	}
}
