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
	db := memory.NewDatabase("db")

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
						ab(db),
						xy(db),
						newEq("ab.a=xy.x"),
					),
					pq(db),
					newEq("xy.x=pq.p"),
				),
				uv(db),
				newEq("pq.q=uv.u"),
			),
			memo: `memo:
├── G1: (tablescan: ab)
├── G2: (tablescan: xy)
├── G3: (hashjoin 1 2) (hashjoin 2 1) (innerjoin 2 1) (innerjoin 1 2)
├── G4: (tablescan: pq)
├── G5: (hashjoin 3 4) (hashjoin 1 9) (hashjoin 9 1) (hashjoin 2 8) (hashjoin 8 2) (hashjoin 4 3) (innerjoin 4 3) (innerjoin 8 2) (innerjoin 2 8) (innerjoin 9 1) (innerjoin 1 9) (innerjoin 3 4)
├── G6: (tablescan: uv)
├── G7: (hashjoin 5 6) (hashjoin 1 12) (hashjoin 12 1) (hashjoin 2 11) (hashjoin 11 2) (hashjoin 3 10) (hashjoin 10 3) (hashjoin 6 5) (innerjoin 6 5) (innerjoin 10 3) (innerjoin 3 10) (innerjoin 11 2) (innerjoin 2 11) (innerjoin 12 1) (innerjoin 1 12) (innerjoin 5 6)
├── G8: (hashjoin 1 4) (hashjoin 4 1) (innerjoin 4 1) (innerjoin 1 4)
├── G9: (hashjoin 2 4) (hashjoin 4 2) (innerjoin 4 2) (innerjoin 2 4)
├── G10: (hashjoin 4 6) (hashjoin 6 4) (innerjoin 6 4) (innerjoin 4 6)
├── G11: (hashjoin 1 10) (hashjoin 10 1) (hashjoin 8 6) (hashjoin 6 8) (innerjoin 6 8) (innerjoin 8 6) (innerjoin 10 1) (innerjoin 1 10)
└── G12: (hashjoin 2 10) (hashjoin 10 2) (hashjoin 9 6) (hashjoin 6 9) (innerjoin 6 9) (innerjoin 9 6) (innerjoin 10 2) (innerjoin 2 10)
`,
		},
	}

	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := memo.NewMemo(ctx, newTestCatalog(db), nil, 0, memo.NewDefaultCoster())
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

func uv(db *memory.Database) sql.Node {
	t := memory.NewTable(db, "uv", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "u", Type: types.Int64, Nullable: true},
		{Name: "v", Type: types.Text, Nullable: true},
	}, 0), nil)
	return plan.NewResolvedTable(t, db, nil).WithId(4).WithColumns(sql.NewColSet(7, 8))
}

func xy(db *memory.Database) sql.Node {
	t := memory.NewTable(db, "xy", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "x", Type: types.Int64, Nullable: true},
		{Name: "y", Type: types.Text, Nullable: true},
	}, 0), nil)
	return plan.NewResolvedTable(t, db, nil).WithId(1).WithColumns(sql.NewColSet(1, 2))
}

func ab(db *memory.Database) sql.Node {
	t := memory.NewTable(db, "ab", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: types.Int64, Nullable: true},
		{Name: "b", Type: types.Text, Nullable: true},
	}, 0), nil)
	return plan.NewResolvedTable(t, db, nil).WithId(2).WithColumns(sql.NewColSet(3, 4))
}

func pq(db *memory.Database) sql.Node {
	t := memory.NewTable(db, "pq", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "p", Type: types.Int64, Nullable: true},
		{Name: "q", Type: types.Text, Nullable: true},
	}, 0), nil)
	return plan.NewResolvedTable(t, db, nil).WithId(3).WithColumns(sql.NewColSet(5, 6))
}

func newEq(eq string) sql.Expression {
	vars := strings.Split(eq, "=")
	if len(vars) > 2 {
		panic("invalid equal expression")
	}
	left := strings.Split(vars[0], ".")
	right := strings.Split(vars[1], ".")
	return expression.NewEquals(
		expression.NewGetFieldWithTable(colId(left[1]), tabId(left[0]), types.Int64, "db", left[0], left[1], false),
		expression.NewGetFieldWithTable(colId(right[1]), tabId(right[0]), types.Int64, "db", right[0], right[1], false),
	)
}

func colId(n string) int {
	switch n {
	case "x":
		return 1
	case "y":
		return 2
	case "a":
		return 3
	case "b":
		return 4
	case "p":
		return 5
	case "q":
		return 6
	case "u":
		return 7
	case "v":
		return 8
	default:
		panic("unknown col")
	}
}

func tabId(n string) int {
	switch n {
	case "xy":
		return 1
	case "ab":
		return 2
	case "pq":
		return 3
	case "uv":
		return 4
	default:
		panic("unknown table")
	}
}
