package analyzer

import (
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
			m := NewMemo(nil, nil, nil, NewDefaultCoster(), NewDefaultCarder())
			j := newJoinOrderBuilder(m)
			j.reorderJoin(tt.plan)
			addHashJoins(m)
			require.Equal(t, tt.memo, j.m.String())
		})
	}
}
