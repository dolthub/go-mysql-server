package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestHintIndicatedDeps(t *testing.T) {
	p := plan.NewInnerJoin(
		plan.NewInnerJoin(
			plan.NewInnerJoin(
				tableNode("ab"),
				tableNode("xy"),
				newEq("ab.i = xy.i"),
			),
			tableNode("pq"),
			newEq("xy.i = pq.i"),
		),
		tableNode("uv"),
		newEq("pq.i=uv.i"),
	)

	tests := []struct {
		name    string
		hint    []string
		plan    sql.Node
		exp     map[GroupId]vertexSet
		invalid bool
	}{
		{
			name: "valid1",
			hint: []string{"ab", "xy", "pq", "uv"},
			plan: p,
			exp: map[GroupId]vertexSet{
				1:  testVertexSet(0),          // ab
				2:  testVertexSet(1),          // xy
				3:  testVertexSet(0, 1),       // ab x xy
				4:  testVertexSet(2),          // pq
				5:  testVertexSet(0, 1, 2),    // ab x xy x pq
				6:  testVertexSet(3),          // uv
				7:  testVertexSet(0, 1, 2, 3), // ab x xy x pq x uv
				8:  testVertexSet(1, 2),       // xy x pq
				9:  testVertexSet(2, 3),       // pq x uv
				10: testVertexSet(1, 2, 3),    // xy x pq x uv
			},
		},
		{
			name: "valid2",
			hint: []string{"pq", "xy", "ab", "uv"},
			plan: p,
			exp: map[GroupId]vertexSet{
				1:  testVertexSet(2),          // ab
				2:  testVertexSet(1),          // xy
				3:  testVertexSet(2, 1),       // ab x xy
				4:  testVertexSet(0),          // pq
				5:  testVertexSet(2, 1, 0),    // ab x xy x pq
				6:  testVertexSet(3),          // uv
				7:  testVertexSet(2, 1, 0, 3), // ab x xy x pq x uv
				8:  testVertexSet(1, 0),       // xy x pq
				9:  testVertexSet(0, 3),       // pq x uv
				10: testVertexSet(1, 0, 3),    // xy x pq x uv
			},
		},
		{
			name:    "invalid1",
			hint:    []string{"ab", "xy"},
			plan:    p,
			invalid: true,
		},
		{
			name:    "invalid2",
			hint:    []string{"rs", "pq", "ab", "uv"},
			plan:    p,
			invalid: true,
		},
		{
			name:    "invalid3",
			hint:    []string{},
			plan:    p,
			invalid: true,
		},
		{
			name:    "invalid4",
			hint:    []string{"ab", "xy", "rs"},
			plan:    p,
			invalid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := newJoinOrderBuilder(NewMemo(nil, nil, nil, NewDefaultCoster(), NewDefaultCarder()))
			j.reorderJoin(tt.plan)
			j.m.WithJoinOrder(JoinOrderHint{tables: tt.hint})
			if tt.invalid {
				require.Equal(t, j.m.orderHint, (*joinOrderDeps)(nil))
			} else {
				require.Equal(t, tt.exp, j.m.orderHint.groups)
			}
		})
	}
}

func testVertexSet(i ...int) vertexSet {
	s := vertexSet(0)
	for _, i := range i {
		s = s.add(uint64(i))
	}
	return s
}
