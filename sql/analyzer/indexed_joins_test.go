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
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	tests := []struct {
		name string
		plan sql.Node
		memo string
	}{
		{
			name: "hash join 1",
			plan: plan.NewInnerJoin(
				ctx,
				plan.NewInnerJoin(
					ctx,
					plan.NewInnerJoin(
						ctx,
						ab(ctx, db),
						xy(ctx, db),
						newEq("ab.a=xy.x"),
					),
					pq(ctx, db),
					newEq("xy.x=pq.p"),
				),
				uv(ctx, db),
				newEq("pq.q=uv.u"),
			),
			memo: `memo:
├── G1: (tablescan: ab)
├── G2: (tablescan: xy)
├── G3: (hashjoin 1[ab] 2[xy]) (hashjoin 2[xy] 1[ab]) (innerjoin 2[xy] 1[ab]) (innerjoin 1[ab] 2[xy])
├── G4: (tablescan: pq)
├── G5: (hashjoin 3 4[pq]) (hashjoin 1[ab] 9) (hashjoin 9 1[ab]) (hashjoin 2[xy] 8) (hashjoin 8 2[xy]) (hashjoin 4[pq] 3) (innerjoin 4[pq] 3) (innerjoin 8 2[xy]) (innerjoin 2[xy] 8) (innerjoin 9 1[ab]) (innerjoin 1[ab] 9) (innerjoin 3 4[pq])
├── G6: (tablescan: uv)
├── G7: (hashjoin 5 6[uv]) (hashjoin 1[ab] 12) (hashjoin 12 1[ab]) (hashjoin 2[xy] 11) (hashjoin 11 2[xy]) (hashjoin 3 10) (hashjoin 10 3) (hashjoin 6[uv] 5) (innerjoin 6[uv] 5) (innerjoin 10 3) (innerjoin 3 10) (innerjoin 11 2[xy]) (innerjoin 2[xy] 11) (innerjoin 12 1[ab]) (innerjoin 1[ab] 12) (innerjoin 5 6[uv])
├── G8: (hashjoin 1[ab] 4[pq]) (hashjoin 4[pq] 1[ab]) (innerjoin 4[pq] 1[ab]) (innerjoin 1[ab] 4[pq])
├── G9: (hashjoin 2[xy] 4[pq]) (hashjoin 4[pq] 2[xy]) (innerjoin 4[pq] 2[xy]) (innerjoin 2[xy] 4[pq])
├── G10: (hashjoin 4[pq] 6[uv]) (hashjoin 6[uv] 4[pq]) (innerjoin 6[uv] 4[pq]) (innerjoin 4[pq] 6[uv])
├── G11: (hashjoin 1[ab] 10) (hashjoin 10 1[ab]) (hashjoin 8 6[uv]) (hashjoin 6[uv] 8) (innerjoin 6[uv] 8) (innerjoin 8 6[uv]) (innerjoin 10 1[ab]) (innerjoin 1[ab] 10)
└── G12: (hashjoin 2[xy] 10) (hashjoin 10 2[xy]) (hashjoin 9 6[uv]) (hashjoin 6[uv] 9) (innerjoin 6[uv] 9) (innerjoin 9 6[uv]) (innerjoin 10 2[xy]) (innerjoin 2[xy] 10)
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := memo.NewMemo(ctx, newTestCatalog(db), nil, memo.NewDefaultCoster(), nil)
			j := memo.NewJoinOrderBuilder(m)
			j.ReorderJoin(ctx, tt.plan)
			addHashJoins(ctx, m)
			require.Equal(t, tt.memo, m.String())
		})
	}
}

func uv(ctx *sql.Context, db *memory.Database) sql.Node {
	t := memory.NewTable(ctx, db, "uv", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "u", Type: types.Int64, Nullable: true, Source: "uv"},
		{Name: "v", Type: types.Text, Nullable: true, Source: "uv"},
	}, 0), nil)
	return plan.NewResolvedTable(t, db, nil).WithId(4).WithColumns(sql.NewColSet(7, 8))
}

func xy(ctx *sql.Context, db *memory.Database) sql.Node {
	t := memory.NewTable(ctx, db, "xy", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "x", Type: types.Int64, Nullable: true, Source: "xy"},
		{Name: "y", Type: types.Text, Nullable: true, Source: "xy"},
	}, 0), nil)
	return plan.NewResolvedTable(t, db, nil).WithId(1).WithColumns(sql.NewColSet(1, 2))
}

func ab(ctx *sql.Context, db *memory.Database) sql.Node {
	t := memory.NewTable(ctx, db, "ab", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: types.Int64, Nullable: true, Source: "ab"},
		{Name: "b", Type: types.Text, Nullable: true, Source: "ab"},
	}, 0), nil)
	return plan.NewResolvedTable(t, db, nil).WithId(2).WithColumns(sql.NewColSet(3, 4))
}

func pq(ctx *sql.Context, db *memory.Database) sql.Node {
	t := memory.NewTable(ctx, db, "pq", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "p", Type: types.Int64, Nullable: true, Source: "pq"},
		{Name: "q", Type: types.Text, Nullable: true, Source: "pq"},
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

// TestIndexedJoinBindVarEqualityFilter tests analysis of a join query that contains a
// bind var on an indexed column.
//
// Some engine embedders (i.e. Doltgres, which must answer a Postgres wire-protocol
// Describe message with accurate result columns before the client has sent bind
// values) run a full analysis pass on a prepared query's plan while it still holds
// unbound *expression.BindVar placeholders in place of literals.
func TestIndexedJoinBindVarEqualityFilter(t *testing.T) {
	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	t1Fk := expression.NewGetFieldWithTable(21, 10, types.Int64, "db", "t1", "fk", true)
	t2Pk := expression.NewGetFieldWithTable(23, 11, types.Int64, "db", "t2", "pk", false)

	joinFilter := expression.NewEquals(t1Fk, t2Pk)
	bindVarFilter := expression.NewEquals(t1Fk, expression.NewBindVar("v1"))

	left := plan.NewFilter(ctx, bindVarFilter, mergeJoinT1(ctx, db))
	joined := plan.NewInnerJoin(ctx, left, mergeJoinT2(ctx, db), joinFilter)

	m := memo.NewMemo(ctx, memory.NewStatsProv(), nil, memo.NewDefaultCoster(), nil)
	j := memo.NewJoinOrderBuilder(m)
	j.ReorderJoin(ctx, joined)

	require.NotPanics(t, func() {
		require.NoError(t, addMergeJoins(ctx, m))
	})
}

// mergeJoinT1 returns a table with a primary key on pk and a secondary index on fk.
func mergeJoinT1(ctx *sql.Context, db *memory.Database) sql.Node {
	tbl := memory.NewTable(ctx, db, "t1", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "pk", Type: types.Int64, Nullable: false, Source: "t1", PrimaryKey: true},
		{Name: "fk", Type: types.Int64, Nullable: true, Source: "t1"},
		{Name: "val", Type: types.Int64, Nullable: true, Source: "t1"},
	}), nil)
	if err := tbl.CreateIndex(ctx, sql.IndexDef{
		Name:    "t1_fk_idx",
		Columns: []sql.IndexColumn{{Name: "fk"}},
	}); err != nil {
		panic(err)
	}
	db.AddTable("t1", tbl)
	return plan.NewResolvedTable(tbl, db, nil).WithId(10).WithColumns(sql.NewColSet(20, 21, 22))
}

// mergeJoinT2 returns a table with a primary key on pk.
func mergeJoinT2(ctx *sql.Context, db *memory.Database) sql.Node {
	tbl := memory.NewTable(ctx, db, "t2", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "pk", Type: types.Int64, Nullable: false, Source: "t2", PrimaryKey: true},
		{Name: "val", Type: types.Int64, Nullable: true, Source: "t2"},
	}), nil)
	db.AddTable("t2", tbl)
	return plan.NewResolvedTable(tbl, db, nil).WithId(11).WithColumns(sql.NewColSet(23, 24))
}
