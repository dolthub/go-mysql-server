// Copyright 2023 Dolthub, Inc.
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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/memo"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestBiasedCoster(t *testing.T) {
	db := memory.NewDatabase("mydb")
	db.EnablePrimaryKeyIndexes()
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	xy := memory.NewFilteredTable(db, "xy", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "x", Type: types.Int64, Source: "xy"},
		{Name: "y", Type: types.Int64, Source: "xy"},
	}, 0), nil)

	ab := memory.NewFilteredTable(db, "ab", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: types.Int64, Source: "ab"},
		{Name: "b", Type: types.Int64, Source: "ab"},
	}, 0), nil)

	xy.EnablePrimaryKeyIndexes()
	ab.EnablePrimaryKeyIndexes()

	ab.Insert(ctx, sql.Row{int64(0), int64(0)})
	ab.Insert(ctx, sql.Row{int64(1), int64(1)})
	ab.Insert(ctx, sql.Row{int64(2), int64(2)})
	xy.Insert(ctx, sql.Row{int64(0), int64(0)})
	xy.Insert(ctx, sql.Row{int64(1), int64(1)})

	db.AddTable("xy", xy)
	db.AddTable("ab", ab)

	a := NewDefault(sql.NewDatabaseProvider(db))

	n := plan.NewInnerJoin(
		plan.NewResolvedTable(xy, db, nil),
		plan.NewResolvedTable(ab, db, nil),
		expression.NewEquals(
			expression.NewGetFieldWithTable(0, types.Int64, "db", "xy", "x", false),
			expression.NewGetFieldWithTable(0, types.Int64, "db", "ab", "a", false),
		),
	)

	tests := []struct {
		name string
		c    func() memo.Coster
		exp  plan.JoinType
	}{
		{
			name: "inner",
			c:    memo.NewInnerBiasedCoster,
			exp:  plan.JoinTypeInner,
		},
		{
			name: "lookup",
			c:    memo.NewLookupBiasedCoster,
			exp:  plan.JoinTypeLookup,
		},
		{
			name: "hash",
			c:    memo.NewHashBiasedCoster,
			exp:  plan.JoinTypeHash,
		},
		{
			name: "merge",
			c:    memo.NewMergeBiasedCoster,
			exp:  plan.JoinTypeMerge,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s biased coster", tt.name), func(t *testing.T) {
			a.Coster = tt.c()
			cmp, err := replanJoin(ctx, n, a, nil)
			require.NoError(t, err)
			types := collectJoinTypes(cmp)[0]
			require.Equal(t, tt.exp, types)
		})
	}
}

func collectJoinTypes(n sql.Node) []plan.JoinType {
	var types []plan.JoinType
	transform.Inspect(n, func(n sql.Node) bool {
		if n == nil {
			return true
		}
		j, ok := n.(*plan.JoinNode)
		if ok {
			types = append(types, j.Op)
		}

		if ex, ok := n.(sql.Expressioner); ok {
			for _, e := range ex.Expressions() {
				transform.InspectExpr(e, func(e sql.Expression) bool {
					sq, ok := e.(*plan.Subquery)
					if !ok {
						return false
					}
					types = append(types, collectJoinTypes(sq.Query)...)
					return false
				})
			}
		}
		return true
	})
	return types
}
