// Copyright 2022 Dolthub, Inc.
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

package memo

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestJoinOrderBuilder(t *testing.T) {
	tests := []struct {
		in    sql.Node
		name  string
		plans string
	}{
		{
			name: "inner joins",
			in: plan.NewInnerJoin(
				plan.NewInnerJoin(
					plan.NewInnerJoin(
						tableNode("a"),
						tableNode("b"),
						newEq("a.x = b.x"),
					),
					tableNode("c"),
					newEq("b.x = c.x"),
				),
				tableNode("d"),
				newEq("c.x = d.x"),
			),
			plans: `memo:
├── G1: (tablescan: a)
├── G2: (tablescan: b)
├── G3: (colref: 'a.x')
├── G4: (colref: 'b.x')
├── G5: (equal 3 4)
├── G6: (innerjoin 2 1) (innerjoin 1 2)
├── G7: (tablescan: c)
├── G8: (colref: 'c.x')
├── G9: (equal 4 8)
├── G10: (innerjoin 7 6) (innerjoin 15 1) (innerjoin 1 15) (innerjoin 6 7)
├── G11: (tablescan: d)
├── G12: (colref: 'd.x')
├── G13: (equal 8 12)
├── G14: (innerjoin 11 10) (innerjoin 16 6) (innerjoin 6 16) (innerjoin 17 1) (innerjoin 1 17) (innerjoin 10 11)
├── G15: (innerjoin 7 2) (innerjoin 2 7)
├── G16: (innerjoin 11 7) (innerjoin 7 11)
└── G17: (innerjoin 11 15) (innerjoin 15 11) (innerjoin 16 2) (innerjoin 2 16)
`,
		},
		{
			name: "non-inner joins",
			in: plan.NewInnerJoin(
				plan.NewInnerJoin(
					plan.NewLeftOuterJoin(
						tableNode("a"),
						tableNode("b"),
						newEq("a.x = b.x"),
					),
					plan.NewLeftOuterJoin(
						plan.NewFullOuterJoin(
							tableNode("c"),
							tableNode("d"),
							newEq("c.x = d.x"),
						),
						tableNode("e"),
						newEq("c.x = e.x"),
					),
					newEq("a.x = e.x"),
				),
				plan.NewInnerJoin(
					tableNode("f"),
					tableNode("g"),
					newEq("f.x = g.x"),
				),
				newEq("e.x = g.x"),
			),
			plans: `memo:
├── G1: (tablescan: a)
├── G2: (tablescan: b)
├── G3: (colref: 'a.x')
├── G4: (colref: 'b.x')
├── G5: (equal 3 4)
├── G6: (leftjoin 1 2)
├── G7: (tablescan: c)
├── G8: (tablescan: d)
├── G9: (colref: 'c.x')
├── G10: (colref: 'd.x')
├── G11: (equal 9 10)
├── G12: (fullouterjoin 7 8)
├── G13: (tablescan: e)
├── G14: (colref: 'e.x')
├── G15: (equal 9 14)
├── G16: (leftjoin 12 13)
├── G17: (equal 3 14)
├── G18: (innerjoin 16 6) (leftjoin 27 2) (innerjoin 6 16)
├── G19: (tablescan: f)
├── G20: (tablescan: g)
├── G21: (colref: 'f.x')
├── G22: (colref: 'g.x')
├── G23: (equal 21 22)
├── G24: (innerjoin 20 19) (innerjoin 19 20)
├── G25: (equal 14 22)
├── G26: (innerjoin 30 19) (innerjoin 19 30) (innerjoin 24 18) (innerjoin 31 6) (innerjoin 6 31) (leftjoin 32 2) (innerjoin 18 24)
├── G27: (innerjoin 16 1) (innerjoin 1 16)
├── G28: (innerjoin 20 16) (innerjoin 16 20)
├── G29: (innerjoin 20 27) (innerjoin 27 20) (innerjoin 28 1) (innerjoin 1 28)
├── G30: (innerjoin 20 18) (innerjoin 18 20) (innerjoin 28 6) (innerjoin 6 28) (leftjoin 29 2)
├── G31: (innerjoin 28 19) (innerjoin 19 28) (innerjoin 24 16) (innerjoin 16 24)
└── G32: (innerjoin 29 19) (innerjoin 19 29) (innerjoin 24 27) (innerjoin 27 24) (innerjoin 31 1) (innerjoin 1 31)
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := NewJoinOrderBuilder(NewMemo(nil, nil, nil, 0, NewDefaultCoster(), NewDefaultCarder()))
			j.ReorderJoin(tt.in)
			require.Equal(t, tt.plans, j.m.String())
		})
	}
}

func TestJoinOrderBuilder_populateSubgraph(t *testing.T) {
	tests := []struct {
		name     string
		join     sql.Node
		expEdges []edge
	}{
		{
			name: "cross join",
			join: plan.NewCrossJoin(
				tableNode("a"),
				plan.NewInnerJoin(
					tableNode("b"),
					plan.NewLeftOuterJoin(
						tableNode("c"),
						tableNode("d"),
						newEq("c.x=d.x"),
					),
					newEq("b.y=d.y"),
				),
			),
			expEdges: []edge{
				newEdge2(plan.JoinTypeLeftOuter, "0011", "0011", "0010", "0001", nil,
					&Equal{
						scalarBase: &scalarBase{},
						Left:       newColRef(3, 7, "c.x"),
						Right:      newColRef(4, 10, "d.x"),
					}, ""), // C x D
				newEdge2(plan.JoinTypeInner, "0101", "0111", "0100", "0011", nil,
					&Equal{
						scalarBase: &scalarBase{},
						Left:       newColRef(2, 5, "b.y"),
						Right:      newColRef(4, 11, "d.y"),
					},
					""), // B x (CD)
				newEdge2(plan.JoinTypeCross, "0000", "1111", "1000", "0111", nil, nil, ""), // A x (BCD)
			},
		},
		{
			name: "right deep left join",
			join: plan.NewInnerJoin(
				tableNode("a"),
				plan.NewInnerJoin(
					tableNode("b"),
					plan.NewLeftOuterJoin(
						tableNode("c"),
						tableNode("d"),
						newEq("c.x=d.x"),
					),
					newEq("b.y=d.y"),
				),
				newEq("a.z=b.z"),
			),
			expEdges: []edge{
				newEdge2(plan.JoinTypeLeftOuter, "0011", "0011", "0010", "0001", nil,
					&Equal{
						Left:  newColRef(3, 7, "c.x"),
						Right: newColRef(4, 10, "d.x"),
					}, ""), // C x D
				newEdge2(plan.JoinTypeInner, "0101", "0111", "0100", "0011", nil,
					&Equal{
						Left:  newColRef(2, 5, "b.y"),
						Right: newColRef(4, 11, "d.y"),
					},
					""), // B x (CD)
				newEdge2(plan.JoinTypeInner, "1100", "1100", "1000", "0111", []conflictRule{{from: newVertexSet("0001"), to: newVertexSet("0010")}},
					&Equal{
						Left:  newColRef(1, 3, "a.z"),
						Right: newColRef(2, 6, "b.z"),
					},
					""), // A x (BCD)
			},
		},
		{
			name: "bushy left joins",
			join: plan.NewLeftOuterJoin(
				plan.NewLeftOuterJoin(
					tableNode("a"),
					tableNode("b"),
					newEq("a.x=b.x"),
				),
				plan.NewLeftOuterJoin(
					tableNode("c"),
					tableNode("d"),
					newEq("c.x=d.x"),
				),
				newEq("b.y=c.y"),
			),
			expEdges: []edge{
				newEdge2(plan.JoinTypeLeftOuter, "1100", "1100", "1000", "0100", nil,
					&Equal{
						Left:  newColRef(1, 1, "a.x"),
						Right: newColRef(2, 4, "b.x"),
					}, ""), // A x B
				newEdge2(plan.JoinTypeLeftOuter, "0011", "0011", "0010", "0001", nil,
					&Equal{
						Left:  newColRef(7, 7, "c.x"),  // offset by filters
						Right: newColRef(8, 10, "d.x"), // offset by filters
					},
					""), // C x D
				newEdge2(plan.JoinTypeLeftOuter, "0110", "1111", "1100", "0011", nil,
					&Equal{
						Left:  newColRef(2, 5, "b.y"),
						Right: newColRef(7, 8, "c.y"),
					},
					""), // (AB) x (CD)
			},
		},
		{
			// SELECT *
			// FROM (SELECT * FROM A CROSS JOIN B)
			// LEFT JOIN C
			// ON B.x = C.x
			name: "degenerate inner join",
			join: plan.NewLeftOuterJoin(
				plan.NewCrossJoin(
					tableNode("a"),
					tableNode("b"),
				),
				tableNode("c"),
				newEq("b.x=c.x"),
			),
			expEdges: []edge{
				newEdge2(plan.JoinTypeCross, "000", "110", "100", "010", nil, nil, ""), // A X B
				newEdge2(plan.JoinTypeLeftOuter, "011", "111", "110", "001", nil,
					&Equal{
						Left:  newColRef(2, 4, "b.x"),
						Right: newColRef(4, 7, "c.x"),
					},
					""), // (AB) x C
			},
		},
		{
			// SELECT *
			// FROM (SELECT * FROM A INNER JOIN B ON True)
			// FULL JOIN (SELECT * FROM C INNER JOIN D ON True)
			// ON A.x = C.x
			name: "degenerate inner join",
			join: plan.NewFullOuterJoin(
				plan.NewInnerJoin(
					tableNode("a"),
					tableNode("b"),
					expression.NewLiteral(true, types.Boolean),
				),
				plan.NewInnerJoin(
					tableNode("c"),
					tableNode("d"),
					expression.NewLiteral(true, types.Boolean),
				),
				newEq("a.x=c.x"),
			),
			expEdges: []edge{
				newEdge2(plan.JoinTypeInner, "0000", "1100", "1000", "0100", nil, &Literal{Val: true, Typ: types.Boolean}, ""), // A x B
				newEdge2(plan.JoinTypeInner, "0000", "0011", "0010", "0001", nil, &Literal{Val: true, Typ: types.Boolean}, ""), // C x D
				newEdge2(plan.JoinTypeFullOuter, "1010", "1111", "1100", "0011", nil,
					&Equal{
						Left:  newColRef(1, 1, "a.x"),
						Right: newColRef(5, 7, "c.x"),
					},
					""), // (AB) x (CD)
			},
		},
		{
			// SELECT * FROM A
			// WHERE EXISTS
			// (
			//   SELECT * FROM B
			//   LEFT JOIN C ON B.x = C.x
			//   WHERE A.y = B.y
			// )
			// note: left join is the right child
			name: "semi join",
			join: plan.NewSemiJoin(
				plan.NewLeftOuterJoin(
					tableNode("b"),
					tableNode("c"),
					newEq("b.x=c.x"),
				),
				tableNode("a"),
				newEq("a.y=b.y"),
			),
			expEdges: []edge{
				newEdge2(plan.JoinTypeLeftOuter, "110", "110", "100", "010", nil,
					&Equal{
						Left:  newColRef(1, 1, "b.x"),
						Right: newColRef(2, 4, "c.x"),
					},
					""), // B x C
				newEdge2(plan.JoinTypeSemi, "101", "101", "110", "001", nil,
					&Equal{
						Left:  newColRef(7, 8, "a.y"),
						Right: newColRef(1, 2, "b.y"),
					},
					""), // A x (BC)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewJoinOrderBuilder(NewMemo(nil, nil, nil, 0, NewDefaultCoster(), NewDefaultCarder()))
			b.populateSubgraph(tt.join)
			edgesEq(t, tt.expEdges, b.edges)
		})
	}
}

func newColRef(table GroupId, col sql.ColumnId, gf string) *ExprGroup {
	parts := strings.Split(gf, ".")
	return &ExprGroup{
		Scalar: &ColRef{
			scalarBase: &scalarBase{},
			Table:      table,
			Col:        col,
			Gf:         expression.NewGetFieldWithTable(0, types.Int64, parts[0], parts[1], false),
		},
	}
}

func newEq(eq string) sql.Expression {
	vars := strings.Split(strings.Replace(eq, " ", "", -1), "=")
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

func TestAssociativeTransforms(t *testing.T) {
	// Sourced from Figure 3
	// each test has a reversible pair test which is a product of its transform
	validTests := []struct {
		name      string
		eA        *edge
		eB        *edge
		transform assocTransform
		rev       bool
	}{
		{
			name:      "assoc(a,b)",
			eA:        newEdge(plan.JoinTypeInner, "110", "010", "100"),
			eB:        newEdge(plan.JoinTypeInner, "101", "110", "001"),
			transform: assoc,
		},
		{
			name:      "assoc(b,a)",
			eA:        newEdge(plan.JoinTypeInner, "010", "101", "010"),
			eB:        newEdge(plan.JoinTypeInner, "101", "001", "100"),
			transform: assoc,
			rev:       true,
		},
		{
			name:      "r-asscom(a,b)",
			eA:        newEdge(plan.JoinTypeInner, "110", "010", "100"),
			eB:        newEdge(plan.JoinTypeInner, "101", "001", "110"),
			transform: rightAsscom,
		},
		{
			name:      "r-asscom(b,a)",
			eA:        newEdge(plan.JoinTypeInner, "110", "010", "101"),
			eB:        newEdge(plan.JoinTypeInner, "101", "001", "100"),
			transform: rightAsscom,
			rev:       true,
		},
		{
			name:      "l-asscom(a,b)",
			eA:        newEdge(plan.JoinTypeInner, "110", "100", "010"),
			eB:        newEdge(plan.JoinTypeInner, "101", "110", "001"),
			transform: leftAsscom,
		},
		{
			name:      "l-asscom(b,a)",
			eA:        newEdge(plan.JoinTypeInner, "110", "101", "010"),
			eB:        newEdge(plan.JoinTypeInner, "101", "100", "001"),
			transform: leftAsscom,
			rev:       true,
		},
		{
			name:      "assoc(a,b)",
			eA:        newEdge(plan.JoinTypeInner, "110", "010", "100"),
			eB:        newEdge(plan.JoinTypeLeftOuter, "101", "110", "001"),
			transform: assoc,
		},
		// l-asscom is OK with everything but full outerjoin w/ null rejecting A(e1).
		// Refer to rule table.
		{
			name:      "l-asscom(a,b)",
			eA:        newEdge(plan.JoinTypeLeftOuter, "110", "100", "010"),
			eB:        newEdge(plan.JoinTypeInner, "101", "110", "001"),
			transform: leftAsscom,
		},
		{
			name:      "l-asscom(b,a)",
			eA:        newEdge(plan.JoinTypeLeftOuter, "110", "101", "010"),
			eB:        newEdge(plan.JoinTypeLeftOuter, "101", "100", "001"),
			transform: leftAsscom,
			rev:       true,
		},
		// TODO special case operators
	}

	for _, tt := range validTests {
		t.Run(fmt.Sprintf("OK %s", tt.name), func(t *testing.T) {
			var res bool
			if tt.rev {
				res = tt.transform(tt.eB, tt.eA)
			} else {
				res = tt.transform(tt.eA, tt.eB)
			}
			require.True(t, res)
		})
	}

	invalidTests := []struct {
		name      string
		eA        *edge
		eB        *edge
		transform assocTransform
		rev       bool
	}{
		// most transforms are invalid, these are also from Figure 3
		{
			name:      "assoc(a,b)",
			eA:        newEdge(plan.JoinTypeInner, "110", "010", "100"),
			eB:        newEdge(plan.JoinTypeInner, "101", "001", "100"),
			transform: assoc,
		},
		{
			name:      "r-asscom(a,b)",
			eA:        newEdge(plan.JoinTypeInner, "110", "010", "100"),
			eB:        newEdge(plan.JoinTypeInner, "101", "100", "010"),
			transform: rightAsscom,
		},
		{
			name:      "l-asscom(a,b)",
			eA:        newEdge(plan.JoinTypeInner, "110", "010", "100"),
			eB:        newEdge(plan.JoinTypeInner, "101", "001", "100"),
			transform: leftAsscom,
		},
		// these are correct transforms with cross or inner joins, but invalid
		// with other operators
		{
			name:      "assoc(a,b)",
			eA:        newEdge(plan.JoinTypeLeftOuter, "110", "010", "100"),
			eB:        newEdge(plan.JoinTypeInner, "101", "110", "001"),
			transform: assoc,
		},
		{
			// this one depends on rejecting nulls on A(e2)
			name:      "left join assoc(b,a)",
			eA:        newEdge(plan.JoinTypeLeftOuter, "010", "101", "010"),
			eB:        newEdge(plan.JoinTypeLeftOuter, "101", "001", "100"),
			transform: assoc,
			rev:       true,
		},
		{
			name:      "left join r-asscom(a,b)",
			eA:        newEdge(plan.JoinTypeLeftOuter, "110", "010", "100"),
			eB:        newEdge(plan.JoinTypeInner, "101", "001", "110"),
			transform: rightAsscom,
		},
		{
			name:      "left join r-asscom(b,a)",
			eA:        newEdge(plan.JoinTypeInner, "110", "010", "101"),
			eB:        newEdge(plan.JoinTypeLeftOuter, "101", "001", "100"),
			transform: rightAsscom,
			rev:       true,
		},
		{
			name:      "left join l-asscom(a,b)",
			eA:        newEdge(plan.JoinTypeFullOuter, "110", "100", "010"),
			eB:        newEdge(plan.JoinTypeInner, "101", "110", "001"),
			transform: leftAsscom,
		},
	}

	for _, tt := range invalidTests {
		t.Run(fmt.Sprintf("Invalid %s", tt.name), func(t *testing.T) {
			var res bool
			if tt.rev {
				res = tt.transform(tt.eB, tt.eA)
			} else {
				res = tt.transform(tt.eA, tt.eB)
			}
			require.False(t, res)
		})
	}
}

var childSchema = sql.NewPrimaryKeySchema(sql.Schema{
	{Name: "x", Type: types.Int64, Nullable: true},
	{Name: "y", Type: types.Text, Nullable: true},
	{Name: "z", Type: types.Int64, Nullable: true},
}, 0)

func tableNode(name string) sql.Node {
	t := memory.NewTable(name, childSchema, nil)
	return plan.NewResolvedTable(t, nil, nil)
}

func newVertexSet(s string) vertexSet {
	v := vertexSet(0)
	for i, c := range s {
		if string(c) == "1" {
			v = v.add(uint64(i))
		}
	}
	return v
}

func newEdge(op plan.JoinType, ses, leftV, rightV string) *edge {
	return &edge{
		op: &operator{
			joinType:      op,
			rightVertices: newVertexSet(rightV),
			leftVertices:  newVertexSet(leftV),
		},
		ses: newVertexSet(ses),
	}
}

func newEdge2(op plan.JoinType, ses, tes, leftV, rightV string, rules []conflictRule, filter ScalarExpr, nullRej string) edge {
	var filters []ScalarExpr
	if filter != nil {
		filters = []ScalarExpr{filter}
	}
	return edge{
		op: &operator{
			joinType:      op,
			rightVertices: newVertexSet(rightV),
			leftVertices:  newVertexSet(leftV),
		},
		ses:              newVertexSet(ses),
		tes:              newVertexSet(tes),
		rules:            rules,
		filters:          filters,
		nullRejectedRels: newVertexSet(nullRej),
	}
}

func edgesEq(t *testing.T, edges1, edges2 []edge) bool {
	if len(edges1) != len(edges2) {
		return false
	}
	for i := range edges1 {
		e1 := edges1[i]
		e2 := edges2[i]
		require.Equal(t, e1.op.joinType, e2.op.joinType)
		require.Equal(t, e1.op.leftVertices.String(), e2.op.leftVertices.String())
		require.Equal(t, e1.op.rightVertices.String(), e2.op.rightVertices.String())
		require.Equal(t, len(e1.filters), len(e2.filters))
		for i := range e1.filters {
			assertScalarEq(t, e1.filters[i], e2.filters[i])
		}
		require.Equal(t, e1.nullRejectedRels, e2.nullRejectedRels)
		require.Equal(t, e1.tes, e2.tes)
		require.Equal(t, e1.ses, e2.ses)
		require.Equal(t, e1.rules, e2.rules)
	}
	return true
}

func assertScalarEq(t *testing.T, exp, cmp ScalarExpr) {
	switch cmp := cmp.(type) {
	case *Equal:
		exp, ok := exp.(*Equal)
		require.True(t, ok)
		assertScalarEq(t, exp.Left.Scalar, cmp.Left.Scalar)
		assertScalarEq(t, exp.Right.Scalar, cmp.Right.Scalar)
	case *Literal:
		exp, ok := exp.(*Literal)
		require.True(t, ok)
		require.Equal(t, exp.Val, cmp.Val)
	case *ColRef:
		exp, ok := exp.(*ColRef)
		require.True(t, ok)
		require.Equal(t, exp.Table, cmp.Table)
		require.Equal(t, exp.Col, cmp.Col)
		require.Equal(t, exp.Gf.String(), cmp.Gf.String())
	}
}
