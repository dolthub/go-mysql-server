package memo

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestHintParsing(t *testing.T) {
	tests := []struct {
		comment string
		hints   []Hint
	}{
		{
			comment: "/*+ join_order(a,b) */",
			hints:   []Hint{{Typ: HintTypeJoinOrder, Args: []string{"a", "b"}}},
		},
		{
			comment: "/*+ JOIN_ORDER(a,b) */",
			hints:   []Hint{{Typ: HintTypeJoinOrder, Args: []string{"a", "b"}}},
		},
		{
			comment: "/*+ join_order(a, b) */",
			hints:   []Hint{{Typ: HintTypeJoinOrder, Args: []string{"a", "b"}}},
		},
		{
			comment: "/*+ join_order(a,    b) */",
			hints:   []Hint{{Typ: HintTypeJoinOrder, Args: []string{"a", "b"}}},
		},
		{
			comment: "/*+JOIN_ORDER(a,b)*/",
			hints:   []Hint{{Typ: HintTypeJoinOrder, Args: []string{"a", "b"}}},
		},
		{
			comment: "/* join_order(a,b) */",
			hints:   []Hint{},
		},
		{
			comment: "/*+ join_order(a_1, b_2) */",
			hints:   []Hint{{Typ: HintTypeJoinOrder, Args: []string{"a_1", "b_2"}}},
		},
		{
			comment: "/*+ join_order(a1, b2) */",
			hints:   []Hint{{Typ: HintTypeJoinOrder, Args: []string{"a1", "b2"}}},
		},
		{
			comment: "/*+ join_order( a1, b2 ) */",
			hints:   []Hint{{Typ: HintTypeJoinOrder, Args: []string{"a1", "b2"}}},
		},
		{
			comment: "/*+ join_order(( a1, b2 )) */",
			hints:   []Hint{},
		},
		{
			comment: "/*+ NO_ICP */",
			hints:   []Hint{{Typ: HintTypeNoIndexConditionPushDown}},
		},
		{
			comment: "/*+ JOIN_FIXED_ORDER */",
			hints:   []Hint{{Typ: HintTypeJoinFixedOrder}},
		},
		{
			comment: "/*+ JOIN_FIXED_ORDER(a) */",
			hints:   []Hint{},
		},
		{
			comment: "/*+ MERGE_JOIN(a,b) */",
			hints:   []Hint{{Typ: HintTypeMergeJoin, Args: []string{"a", "b"}}},
		},
		{
			comment: "/*+ MERGE_JOIN(a,b,c) */",
			hints:   []Hint{},
		},
		{
			comment: "/*+ lookup_join(a,b) */",
			hints:   []Hint{{Typ: HintTypeLookupJoin, Args: []string{"a", "b"}}},
		},
		{
			comment: "/*+ hash_join(a,b) */",
			hints:   []Hint{{Typ: HintTypeHashJoin, Args: []string{"a", "b"}}},
		},
		{
			comment: "/*+ semi_join(a,b) */",
			hints:   []Hint{{Typ: HintTypeSemiJoin, Args: []string{"a", "b"}}},
		},
		{
			comment: "/*+ inner_join(a,b) */",
			hints:   []Hint{{Typ: HintTypeInnerJoin, Args: []string{"a", "b"}}},
		},
		{
			comment: "/*+ anti_join(a,b) */",
			hints:   []Hint{{Typ: HintTypeAntiJoin, Args: []string{"a", "b"}}},
		},
		{
			comment: "/*+ hash_join(a,b) merge_join(b,c) lookup_join(a,d) */",
			hints: []Hint{
				{Typ: HintTypeHashJoin, Args: []string{"a", "b"}},
				{Typ: HintTypeMergeJoin, Args: []string{"b", "c"}},
				{Typ: HintTypeLookupJoin, Args: []string{"a", "d"}},
			},
		},
		{
			comment: "/*+ max_execution_time merge_join(b,c) join_fixed_order */",
			hints: []Hint{
				{Typ: HintTypeMergeJoin, Args: []string{"b", "c"}},
				{Typ: HintTypeJoinFixedOrder},
			},
		},
		{
			comment: "/*+ JOIN_ORDER(a,b,c) LOOKUP_JOIN(a,b) MERGE_JOIN(b,c) NO_ICP ()KF)E)SFKK)SE)F_SE_F)E)S)KEFK */",
			hints: []Hint{
				{Typ: HintTypeJoinOrder, Args: []string{"a", "b", "c"}},
				{Typ: HintTypeLookupJoin, Args: []string{"a", "b"}},
				{Typ: HintTypeMergeJoin, Args: []string{"b", "c"}},
				{Typ: HintTypeNoIndexConditionPushDown},
			},
		},
		{
			comment: "/*+ NOT_A_REAL_HINT JOIN_ORDER(a,b,c) ()KF)E)SFKK) SE)F_SE_F)E)S)KEFK LOOKUP_JOIN(a,b) JOIN_ORDER() MERGE_JOIN(b,c) NO_ICP */",
			hints: []Hint{
				{Typ: HintTypeJoinOrder, Args: []string{"a", "b", "c"}},
				{Typ: HintTypeLookupJoin, Args: []string{"a", "b"}},
				{Typ: HintTypeMergeJoin, Args: []string{"b", "c"}},
				{Typ: HintTypeNoIndexConditionPushDown},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.comment, func(t *testing.T) {
			res := parseJoinHints(tt.comment)
			require.ElementsMatch(t, tt.hints, res)
		})
	}
}

func TestOrderHintBuilding(t *testing.T) {
	p := plan.NewInnerJoin(
		plan.NewInnerJoin(
			plan.NewInnerJoin(
				tableNode("ab"),
				tableNode("xy"),
				newEq("ab.x=xy.x"),
			),
			tableNode("pq"),
			newEq("xy.x=pq.x"),
		),
		tableNode("uv"),
		newEq("pq.x=uv.x"),
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
				6:  testVertexSet(0, 1),       // ab x xy
				7:  testVertexSet(2),          // pq
				10: testVertexSet(0, 1, 2),    // ab x xy x pq
				11: testVertexSet(3),          // uv
				14: testVertexSet(0, 1, 2, 3), // ab x xy x pq x uv
				15: testVertexSet(1, 2),       // xy x pq
				16: testVertexSet(2, 3),       // pq x uv
				17: testVertexSet(1, 2, 3),    // xy x pq x uv
			},
		},
		{
			name: "valid2",
			hint: []string{"pq", "xy", "ab", "uv"},
			plan: p,
			exp: map[GroupId]vertexSet{
				1:  testVertexSet(2),          // ab
				2:  testVertexSet(1),          // xy
				6:  testVertexSet(2, 1),       // ab x xy
				7:  testVertexSet(0),          // pq
				10: testVertexSet(2, 1, 0),    // ab x xy x pq
				11: testVertexSet(3),          // uv
				14: testVertexSet(2, 1, 0, 3), // ab x xy x pq x uv
				15: testVertexSet(1, 0),       // xy x pq
				16: testVertexSet(0, 3),       // pq x uv
				17: testVertexSet(1, 0, 3),    // xy x pq x uv
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
			j := NewJoinOrderBuilder(NewMemo(nil, nil, nil, 0, NewDefaultCoster(), NewDefaultCarder()))
			j.ReorderJoin(tt.plan)
			j.m.WithJoinOrder(tt.hint)
			if tt.invalid {
				require.Equal(t, j.m.hints.order, (*joinOrderHint)(nil))
			} else {
				fmt.Println(j.m.String())
				require.Equal(t, tt.exp, j.m.hints.order.groups)
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
