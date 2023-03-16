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

package analyzer

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
						newEq("a.i = b.i"),
					),
					tableNode("c"),
					newEq("b.i = c.i"),
				),
				tableNode("d"),
				newEq("c.i = d.i"),
			),
			plans: `memo:
├── G1: (tableScan: a)
├── G2: (tableScan: b)
├── G3: (innerJoin 2 1) (innerJoin 1 2)
├── G4: (tableScan: c)
├── G5: (innerJoin 4 3) (innerJoin 8 1) (innerJoin 1 8) (innerJoin 3 4)
├── G6: (tableScan: d)
├── G7: (innerJoin 6 5) (innerJoin 9 3) (innerJoin 3 9) (innerJoin 10 1) (innerJoin 1 10) (innerJoin 5 6)
├── G8: (innerJoin 4 2) (innerJoin 2 4)
├── G9: (innerJoin 6 4) (innerJoin 4 6)
└── G10: (innerJoin 6 8) (innerJoin 8 6) (innerJoin 9 2) (innerJoin 2 9)
`,
		},
		{
			name: "non-inner joins",
			in: plan.NewInnerJoin(
				plan.NewInnerJoin(
					plan.NewLeftOuterJoin(
						tableNode("a"),
						tableNode("b"),
						newEq("a.i = b.i"),
					),
					plan.NewLeftOuterJoin(
						plan.NewFullOuterJoin(
							tableNode("c"),
							tableNode("d"),
							newEq("c.i = d.i"),
						),
						tableNode("e"),
						newEq("c.i = e.i"),
					),
					newEq("a.i = e.i"),
				),
				plan.NewInnerJoin(
					tableNode("f"),
					tableNode("g"),
					newEq("f.i = g.i"),
				),
				newEq("e.i = g.i"),
			),
			plans: `memo:
├── G1: (tableScan: a)
├── G2: (tableScan: b)
├── G3: (leftJoin 1 2)
├── G4: (tableScan: c)
├── G5: (tableScan: d)
├── G6: (fullOuterJoin 4 5)
├── G7: (tableScan: e)
├── G8: (leftJoin 6 7)
├── G9: (innerJoin 8 3) (leftJoin 14 2) (innerJoin 3 8)
├── G10: (tableScan: f)
├── G11: (tableScan: g)
├── G12: (innerJoin 11 10) (innerJoin 10 11)
├── G13: (innerJoin 12 9) (innerJoin 15 3) (innerJoin 3 15) (leftJoin 16 2) (innerJoin 9 12)
├── G14: (innerJoin 8 1) (innerJoin 1 8)
├── G15: (innerJoin 12 8) (innerJoin 8 12)
└── G16: (innerJoin 12 14) (innerJoin 14 12) (innerJoin 15 1) (innerJoin 1 15)
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := newJoinOrderBuilder(NewMemo(nil, nil, nil, NewDefaultCoster(), NewDefaultCarder()))
			j.reorderJoin(tt.in)
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
				newEdge2(plan.JoinTypeLeftOuter, "0011", "0011", "0010", "0001", nil, newEq("c.x=d.x"), ""), // C x D
				newEdge2(plan.JoinTypeInner, "0101", "0111", "0100", "0011", nil, newEq("b.y=d.y"), ""),     // B x (CD)
				newEdge2(plan.JoinTypeCross, "0000", "1111", "1000", "0111", nil, nil, ""),                  // A x (BCD)
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
				newEdge2(plan.JoinTypeLeftOuter, "0011", "0011", "0010", "0001", nil, newEq("c.x=d.x"), ""),                                                                // C x D
				newEdge2(plan.JoinTypeInner, "0101", "0111", "0100", "0011", nil, newEq("b.y=d.y"), ""),                                                                    // B x (CD)
				newEdge2(plan.JoinTypeInner, "1100", "1100", "1000", "0111", []conflictRule{{from: newVertexSet("0001"), to: newVertexSet("0010")}}, newEq("a.z=b.z"), ""), // A x (BCD)
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
				newEdge2(plan.JoinTypeLeftOuter, "1100", "1100", "1000", "0100", nil, newEq("a.x=b.x"), ""), // A x B
				newEdge2(plan.JoinTypeLeftOuter, "0011", "0011", "0010", "0001", nil, newEq("c.x=d.x"), ""), // C x D
				newEdge2(plan.JoinTypeLeftOuter, "0110", "1111", "1100", "0011", nil, newEq("b.y=c.y"), ""), // (AB) x (CD)
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
				newEdge2(plan.JoinTypeCross, "000", "110", "100", "010", nil, nil, ""),                  // A X B
				newEdge2(plan.JoinTypeLeftOuter, "011", "111", "110", "001", nil, newEq("b.x=c.x"), ""), // (AB) x C
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
				newEdge2(plan.JoinTypeInner, "0000", "1100", "1000", "0100", nil, expression.NewLiteral(true, types.Boolean), ""), // A x B
				newEdge2(plan.JoinTypeInner, "0000", "0011", "0010", "0001", nil, expression.NewLiteral(true, types.Boolean), ""), // C x D
				newEdge2(plan.JoinTypeFullOuter, "1010", "1111", "1100", "0011", nil, newEq("a.x=c.x"), ""),                       // (AB) x (CD)
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
				newEdge2(plan.JoinTypeLeftOuter, "110", "110", "100", "010", nil, newEq("b.x=c.x"), ""), // B x C
				newEdge2(plan.JoinTypeSemi, "101", "101", "110", "001", nil, newEq("a.y=b.y"), ""),      // A x (BC)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newJoinOrderBuilder(NewMemo(nil, nil, nil, NewDefaultCoster(), NewDefaultCarder()))
			b.populateSubgraph(tt.join)
			edgesEq(t, tt.expEdges, b.edges)
		})
	}
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
	{Name: "i", Type: types.Int64, Nullable: true},
	{Name: "s", Type: types.Text, Nullable: true},
})

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

func newEdge2(op plan.JoinType, ses, tes, leftV, rightV string, rules []conflictRule, filter sql.Expression, nullRej string) edge {
	var filters []sql.Expression
	if filter != nil {
		filters = []sql.Expression{filter}
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
		require.Equal(t, e1.filters, e2.filters)
		require.Equal(t, e1.nullRejectedRels, e2.nullRejectedRels)
		require.Equal(t, e1.tes, e2.tes)
		require.Equal(t, e1.ses, e2.ses)
		require.Equal(t, e1.rules, e2.rules)
	}
	return true
}
