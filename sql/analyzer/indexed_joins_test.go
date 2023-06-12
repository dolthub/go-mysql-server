package analyzer

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/memo"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
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
						ab(),
						xy(),
						newEq("ab.a=xy.x"),
					),
					pq(),
					newEq("xy.x=pq.p"),
				),
				uv(),
				newEq("pq.q=uv.u"),
			),
			memo: `memo:
├── G1: (tablescan: ab)
├── G2: (tablescan: xy)
├── G3: (colref: 'ab.a')
├── G4: (colref: 'xy.x')
├── G5: (equal 3 4)
├── G6: (hashjoin 1 2) (hashjoin 2 1) (innerjoin 2 1) (innerjoin 1 2)
├── G7: (tablescan: pq)
├── G8: (colref: 'pq.p')
├── G9: (equal 4 8)
├── G10: (hashjoin 6 7) (hashjoin 1 16) (hashjoin 16 1) (hashjoin 7 6) (innerjoin 7 6) (innerjoin 16 1) (innerjoin 1 16) (innerjoin 6 7)
├── G11: (tablescan: uv)
├── G12: (colref: 'pq.q')
├── G13: (colref: 'uv.u')
├── G14: (equal 12 13)
├── G15: (hashjoin 10 11) (hashjoin 1 18) (hashjoin 18 1) (hashjoin 6 17) (hashjoin 17 6) (hashjoin 11 10) (innerjoin 11 10) (innerjoin 17 6) (innerjoin 6 17) (innerjoin 18 1) (innerjoin 1 18) (innerjoin 10 11)
├── G16: (hashjoin 2 7) (hashjoin 7 2) (innerjoin 7 2) (innerjoin 2 7)
├── G17: (hashjoin 7 11) (hashjoin 11 7) (innerjoin 11 7) (innerjoin 7 11)
└── G18: (hashjoin 2 17) (hashjoin 17 2) (hashjoin 16 11) (hashjoin 11 16) (innerjoin 11 16) (innerjoin 16 11) (innerjoin 17 2) (innerjoin 2 17)
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

func uv() sql.Node {
	t := memory.NewTable("uv", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "u", Type: types.Int64, Nullable: true},
		{Name: "v", Type: types.Text, Nullable: true},
	}, 0), nil)
	return plan.NewResolvedTable(t, nil, nil)
}

func xy() sql.Node {
	t := memory.NewTable("xy", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "x", Type: types.Int64, Nullable: true},
		{Name: "y", Type: types.Text, Nullable: true},
	}, 0), nil)
	return plan.NewResolvedTable(t, nil, nil)
}

func ab() sql.Node {
	t := memory.NewTable("ab", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: types.Int64, Nullable: true},
		{Name: "b", Type: types.Text, Nullable: true},
	}, 0), nil)
	return plan.NewResolvedTable(t, nil, nil)
}

func pq() sql.Node {
	t := memory.NewTable("pq", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "p", Type: types.Int64, Nullable: true},
		{Name: "q", Type: types.Text, Nullable: true},
	}, 0), nil)
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
