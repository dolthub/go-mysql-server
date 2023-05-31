package analyzer

import (
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/memo"
	"github.com/dolthub/go-mysql-server/sql/types"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestHashJoins(t *testing.T) {
	tests := []struct {
		name string
		plan sql.Node
		memo string
	}{
		{
			name: "hash join 1",
			plan: plan.NewInnerJoin(
				plan.NewInnerJoin(
					plan.NewInnerJoin(
						tableNode("ab"),
						tableNode("xy"),
						newEq("ab.a = xy.x"),
					),
					tableNode("pq"),
					newEq("xy.x = pq.p"),
				),
				tableNode("uv"),
				newEq("pq.q=uv.u"),
			),
			memo: `memo:
├── G1: (tableScan: ab)
├── G2: (tableScan: xy)
├── G3: (hashJoin 1 2) (hashJoin 2 1) (innerJoin 2 1) (innerJoin 1 2)
├── G4: (tableScan: pq)
├── G5: (hashJoin 3 4) (hashJoin 1 8) (hashJoin 8 1) (hashJoin 4 3) (innerJoin 4 3) (innerJoin 8 1) (innerJoin 1 8) (innerJoin 3 4)
├── G6: (tableScan: uv)
├── G7: (hashJoin 5 6) (hashJoin 1 10) (hashJoin 10 1) (hashJoin 3 9) (hashJoin 9 3) (hashJoin 6 5) (innerJoin 6 5) (innerJoin 9 3) (innerJoin 3 9) (innerJoin 10 1) (innerJoin 1 10) (innerJoin 5 6)
├── G8: (hashJoin 2 4) (hashJoin 4 2) (innerJoin 4 2) (innerJoin 2 4)
├── G9: (hashJoin 4 6) (hashJoin 6 4) (innerJoin 6 4) (innerJoin 4 6)
└── G10: (hashJoin 2 9) (hashJoin 9 2) (hashJoin 8 6) (hashJoin 6 8) (innerJoin 6 8) (innerJoin 8 6) (innerJoin 9 2) (innerJoin 2 9)
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := memo.NewMemo(nil, nil, nil, 0, memo.NewDefaultCoster(), memo.NewDefaultCarder())
			j := memo.NewJoinOrderBuilder(m)
			j.ReorderJoin(tt.plan)
			addHashJoins(m)
			require.Equal(t, tt.memo, m.String())
		})
	}
}

var childSchema = sql.NewPrimaryKeySchema(sql.Schema{
	{Name: "i", Type: types.Int64, Nullable: true},
	{Name: "s", Type: types.Text, Nullable: true},
})

func tableNode(name string) sql.Node {
	t := memory.NewTable(name, childSchema, nil)
	return plan.NewResolvedTable(t, nil, nil)
}

func newEq(eq string) sql.Expression {
	vars := strings.Split(eq, "=")
	if len(vars) > 2 {
		panic("invalid equal expression")
	}
	left := strings.Split(vars[0], ".")
	right := strings.Split(vars[1], ".")
	return expression.NewEquals(
		expression.NewGetFieldWithTable(0, types.Int64, left[0], left[1], false),
		expression.NewGetFieldWithTable(0, types.Int64, right[0], right[1], false),
	)
}
