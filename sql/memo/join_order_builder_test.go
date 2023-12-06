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
	"context"
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
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)

	tests := []struct {
		in               sql.Node
		name             string
		plans            string
		forceFastReorder bool
	}{
		{
			name: "inner joins",
			in: plan.NewInnerJoin(
				plan.NewInnerJoin(
					plan.NewInnerJoin(
						tableNode(db, "a"),
						tableNode(db, "b"),
						newEq("a.x = b.x"),
					),
					tableNode(db, "c"),
					newEq("b.x = c.x"),
				),
				tableNode(db, "d"),
				newEq("c.x = d.x"),
			),
			plans: `memo:
├── G1: (tablescan: a)
├── G2: (tablescan: b)
├── G3: (innerjoin 2 1) (innerjoin 1 2)
├── G4: (tablescan: c)
├── G5: (innerjoin 4 3) (innerjoin 8 2) (innerjoin 2 8) (innerjoin 9 1) (innerjoin 1 9) (innerjoin 3 4)
├── G6: (tablescan: d)
├── G7: (innerjoin 6 5) (innerjoin 10 9) (innerjoin 9 10) (innerjoin 11 8) (innerjoin 8 11) (innerjoin 12 4) (innerjoin 4 12) (innerjoin 13 3) (innerjoin 3 13) (innerjoin 14 2) (innerjoin 2 14) (innerjoin 15 1) (innerjoin 1 15) (innerjoin 5 6)
├── G8: (innerjoin 4 1) (innerjoin 1 4)
├── G9: (innerjoin 4 2) (innerjoin 2 4)
├── G10: (innerjoin 6 1) (innerjoin 1 6)
├── G11: (innerjoin 6 2) (innerjoin 2 6)
├── G12: (innerjoin 6 3) (innerjoin 3 6) (innerjoin 10 2) (innerjoin 2 10) (innerjoin 11 1) (innerjoin 1 11)
├── G13: (innerjoin 6 4) (innerjoin 4 6)
├── G14: (innerjoin 6 8) (innerjoin 8 6) (innerjoin 10 4) (innerjoin 4 10) (innerjoin 13 1) (innerjoin 1 13)
└── G15: (innerjoin 6 9) (innerjoin 9 6) (innerjoin 11 4) (innerjoin 4 11) (innerjoin 13 2) (innerjoin 2 13)
`,
		},
		{
			name: "non-inner joins",
			in: plan.NewInnerJoin(
				plan.NewInnerJoin(
					plan.NewLeftOuterJoin(
						tableNode(db, "a"),
						tableNode(db, "b"),
						newEq("a.x = b.x"),
					),
					plan.NewLeftOuterJoin(
						plan.NewFullOuterJoin(
							tableNode(db, "c"),
							tableNode(db, "d"),
							newEq("c.x = d.x"),
						),
						tableNode(db, "e"),
						newEq("c.x = e.x"),
					),
					newEq("a.x = e.x"),
				),
				plan.NewInnerJoin(
					tableNode(db, "f"),
					tableNode(db, "g"),
					newEq("f.x = g.x"),
				),
				newEq("e.x = g.x"),
			),
			plans: `memo:
├── G1: (tablescan: a)
├── G2: (tablescan: b)
├── G3: (leftjoin 1 2)
├── G4: (tablescan: c)
├── G5: (tablescan: d)
├── G6: (fullouterjoin 4 5)
├── G7: (tablescan: e)
├── G8: (leftjoin 6 7)
├── G9: (innerjoin 8 3) (leftjoin 14 2) (innerjoin 3 8)
├── G10: (tablescan: f)
├── G11: (tablescan: g)
├── G12: (innerjoin 11 10) (innerjoin 10 11)
├── G13: (innerjoin 11 19) (innerjoin 19 11) (innerjoin 21 17) (innerjoin 17 21) (innerjoin 22 16) (innerjoin 16 22) (innerjoin 24 10) (innerjoin 10 24) (innerjoin 12 9) (innerjoin 26 8) (innerjoin 8 26) (innerjoin 27 3) (innerjoin 3 27) (leftjoin 28 2) (innerjoin 9 12)
├── G14: (innerjoin 8 1) (innerjoin 1 8)
├── G15: (innerjoin 10 1) (innerjoin 1 10)
├── G16: (innerjoin 10 3) (innerjoin 3 10) (leftjoin 15 2)
├── G17: (innerjoin 10 8) (innerjoin 8 10)
├── G18: (innerjoin 10 14) (innerjoin 14 10) (innerjoin 15 8) (innerjoin 8 15) (innerjoin 17 1) (innerjoin 1 17)
├── G19: (innerjoin 10 9) (innerjoin 9 10) (innerjoin 16 8) (innerjoin 8 16) (innerjoin 17 3) (innerjoin 3 17) (leftjoin 18 2)
├── G20: (innerjoin 11 1) (innerjoin 1 11)
├── G21: (innerjoin 11 3) (innerjoin 3 11) (leftjoin 20 2)
├── G22: (innerjoin 11 8) (innerjoin 8 11)
├── G23: (innerjoin 11 14) (innerjoin 14 11) (innerjoin 20 8) (innerjoin 8 20) (innerjoin 22 1) (innerjoin 1 22)
├── G24: (innerjoin 11 9) (innerjoin 9 11) (innerjoin 21 8) (innerjoin 8 21) (innerjoin 22 3) (innerjoin 3 22) (leftjoin 23 2)
├── G25: (innerjoin 11 15) (innerjoin 15 11) (innerjoin 20 10) (innerjoin 10 20) (innerjoin 12 1) (innerjoin 1 12)
├── G26: (innerjoin 11 16) (innerjoin 16 11) (innerjoin 21 10) (innerjoin 10 21) (innerjoin 12 3) (innerjoin 3 12) (leftjoin 25 2)
├── G27: (innerjoin 11 17) (innerjoin 17 11) (innerjoin 22 10) (innerjoin 10 22) (innerjoin 12 8) (innerjoin 8 12)
└── G28: (innerjoin 11 18) (innerjoin 18 11) (innerjoin 20 17) (innerjoin 17 20) (innerjoin 22 15) (innerjoin 15 22) (innerjoin 23 10) (innerjoin 10 23) (innerjoin 12 14) (innerjoin 14 12) (innerjoin 25 8) (innerjoin 8 25) (innerjoin 27 1) (innerjoin 1 27)
`,
		},
		{
			name: "test fast reordering algorithm",
			// Optimized plan appears as G11 - (innerjoin 1 12)
			in: plan.NewInnerJoin(
				plan.NewCrossJoin(
					tableNode(db, "a"),
					tableNode(db, "c"),
				),
				tableNode(db, "b"),
				expression.NewAnd(newEq("a.x = b.z"), newEq("b.x = c.z")),
			),

			forceFastReorder: true,
			plans: `memo:
├── G1: (tablescan: a)
├── G2: (tablescan: c)
├── G3: (crossjoin 1 2)
├── G4: (tablescan: b)
├── G5: (innerjoin 1 6) (innerjoin 6 1) (innerjoin 3 4)
└── G6: (innerjoin 4 2) (innerjoin 2 4)
`,
		},
		{
			name: "test fast reordering algorithm on bushy join",
			// Optimized plan appears as G16: (innerjoin 7 17)
			in: plan.NewInnerJoin(
				plan.NewInnerJoin(
					tableNode(db, "c"),
					tableNode(db, "d"),
					newEq("c.x = d.z"),
				),
				plan.NewInnerJoin(
					tableNode(db, "a"),
					tableNode(db, "b"),
					newEq("a.x = b.z"),
				),
				newEq("b.x = c.z"),
			),

			forceFastReorder: true,
			plans: `memo:
├── G1: (tablescan: c)
├── G2: (tablescan: d)
├── G3: (innerjoin 1 2) (innerjoin 2 1) (innerjoin 1 2)
├── G4: (tablescan: a)
├── G5: (tablescan: b)
├── G6: (innerjoin 4 5)
├── G7: (innerjoin 4 8) (innerjoin 8 4) (innerjoin 3 6)
└── G8: (innerjoin 5 3) (innerjoin 3 5)
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := NewJoinOrderBuilder(NewMemo(newContext(pro), nil, nil, 0, NewDefaultCoster(), NewDefaultCarder()))
			j.forceFastDFSLookupForTest = tt.forceFastReorder
			j.ReorderJoin(tt.in)
			require.Equal(t, tt.plans, j.m.String())
		})
	}
}

func newContext(provider *memory.DbProvider) *sql.Context {
	return sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), provider)))
}

func TestJoinOrderBuilder_populateSubgraph(t *testing.T) {
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)

	tests := []struct {
		name     string
		join     sql.Node
		expEdges []edge
	}{
		{
			name: "cross join",
			join: plan.NewCrossJoin(
				tableNode(db, "a"),
				plan.NewInnerJoin(
					tableNode(db, "b"),
					plan.NewLeftOuterJoin(
						tableNode(db, "c"),
						tableNode(db, "d"),
						newEq("c.x=d.x"),
					),
					newEq("b.y=d.y"),
				),
			),
			expEdges: []edge{
				newEdge2(plan.JoinTypeLeftOuter, "0011", "0011", "0010", "0001", nil,
					newEq("c.x=d.x"),
					""), // C x D
				newEdge2(plan.JoinTypeInner, "0101", "0111", "0100", "0011", nil,
					newEq("b.y=d.y"),
					""), // B x (CD)
				newEdge2(plan.JoinTypeCross, "0000", "1111", "1000", "0111", nil, nil, ""), // A x (BCD)
			},
		},
		{
			name: "right deep left join",
			join: plan.NewInnerJoin(
				tableNode(db, "a"),
				plan.NewInnerJoin(
					tableNode(db, "b"),
					plan.NewLeftOuterJoin(
						tableNode(db, "c"),
						tableNode(db, "d"),
						newEq("c.x=d.x"),
					),
					newEq("b.y=d.y"),
				),
				newEq("a.z=b.z"),
			),
			expEdges: []edge{
				newEdge2(plan.JoinTypeLeftOuter, "0011", "0011", "0010", "0001", nil,
					newEq("c.x=d.x"),
					""), // C x D
				newEdge2(plan.JoinTypeInner, "0101", "0111", "0100", "0011", nil,
					newEq("b.y=d.y"),

					""), // B x (CD)
				newEdge2(plan.JoinTypeInner, "1100", "1100", "1000", "0111", []conflictRule{{from: newVertexSet("0001"), to: newVertexSet("0010")}},
					newEq("a.z=b.z"),

					""), // A x (BCD)
			},
		},
		{
			name: "bushy left joins",
			join: plan.NewLeftOuterJoin(
				plan.NewLeftOuterJoin(
					tableNode(db, "a"),
					tableNode(db, "b"),
					newEq("a.x=b.x"),
				),
				plan.NewLeftOuterJoin(
					tableNode(db, "c"),
					tableNode(db, "d"),
					newEq("c.x=d.x"),
				),
				newEq("b.y=c.y"),
			),
			expEdges: []edge{
				newEdge2(plan.JoinTypeLeftOuter, "1100", "1100", "1000", "0100", nil,
					newEq("a.x=b.x"),
					""), // A x B
				newEdge2(plan.JoinTypeLeftOuter, "0011", "0011", "0010", "0001", nil,
					newEq("c.x=d.x"), // offset by filters
					""),              // C x D
				newEdge2(plan.JoinTypeLeftOuter, "0110", "1111", "1100", "0011", nil,
					newEq("b.y=c.y"),
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
					tableNode(db, "a"),
					tableNode(db, "b"),
				),
				tableNode(db, "c"),
				newEq("b.x=c.x"),
			),
			expEdges: []edge{
				newEdge2(plan.JoinTypeCross, "000", "110", "100", "010", nil, nil, ""), // A X B
				newEdge2(plan.JoinTypeLeftOuter, "011", "111", "110", "001", nil,
					newEq("b.x=c.x"),

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
					tableNode(db, "a"),
					tableNode(db, "b"),
					expression.NewLiteral(true, types.Boolean),
				),
				plan.NewInnerJoin(
					tableNode(db, "c"),
					tableNode(db, "d"),
					expression.NewLiteral(true, types.Boolean),
				),
				newEq("a.x=c.x"),
			),
			expEdges: []edge{
				newEdge2(plan.JoinTypeInner, "0000", "1100", "1000", "0100", nil, expression.NewLiteral(true, types.Boolean), ""), // A x B
				newEdge2(plan.JoinTypeInner, "0000", "0011", "0010", "0001", nil, expression.NewLiteral(true, types.Boolean), ""), // C x D
				newEdge2(plan.JoinTypeFullOuter, "1010", "1111", "1100", "0011", nil,
					newEq("a.x=c.x"),
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
					tableNode(db, "b"),
					tableNode(db, "c"),
					newEq("b.x=c.x"),
				),
				tableNode(db, "a"),
				newEq("a.y=b.y"),
			),
			expEdges: []edge{
				newEdge2(plan.JoinTypeLeftOuter, "110", "110", "100", "010", nil,
					newEq("b.x=c.x"),
					""), // B x C
				newEdge2(plan.JoinTypeSemi, "101", "101", "110", "001", nil,
					newEq("a.y=b.y"),
					""), // A x (BC)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewJoinOrderBuilder(NewMemo(newContext(pro), nil, nil, 0, NewDefaultCoster(), NewDefaultCarder()))
			b.populateSubgraph(tt.join)
			edgesEq(t, tt.expEdges, b.edges)
		})
	}
}

func newEq(eq string) sql.Expression {
	vars := strings.Split(strings.Replace(eq, " ", "", -1), "=")
	if len(vars) > 2 {
		panic("invalid equal expression")
	}
	left := strings.Split(vars[0], ".")
	right := strings.Split(vars[1], ".")
	leftTabId, leftColId := getIds(left)
	rightTabId, rightColId := getIds(right)
	return expression.NewEquals(
		expression.NewGetFieldWithTable(leftColId, leftTabId, types.Int64, "", left[0], left[1], false),
		expression.NewGetFieldWithTable(rightColId, rightTabId, types.Int64, "", right[0], right[1], false),
	)
}

func getIds(s []string) (tabId int, colId int) {
	switch s[0] {
	case "a":
		tabId = 1
	case "b":
		tabId = 2
	case "c":
		tabId = 3
	case "d":
		tabId = 4
	case "e":
		tabId = 5
	case "f":
		tabId = 6
	case "g":
		tabId = 7
	case "xy":
		tabId = 1
	case "uv":
		tabId = 2
	case "ab":
		tabId = 3
	case "pq":
		tabId = 4
	}
	switch s[1] {
	case "x":
		colId = (tabId-1)*3 + 1
	case "y":
		colId = (tabId-1)*3 + 2
	case "z":
		colId = (tabId-1)*3 + 3
	}
	return
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

func TestEnsureClosure(t *testing.T) {
	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)

	tests := []struct {
		in       sql.Node
		name     string
		expEdges []edge
	}{
		{
			name: "inner joins",
			in: plan.NewInnerJoin(
				plan.NewInnerJoin(
					plan.NewInnerJoin(
						tableNode(db, "a"),
						tableNode(db, "b"),
						newEq("a.x = b.x"),
					),
					tableNode(db, "c"),
					newEq("b.x = c.x"),
				),
				tableNode(db, "d"),
				newEq("c.x = d.x"),
			),
			expEdges: []edge{
				newEdge2(plan.JoinTypeInner, "1010", "1010", "1100", "0010", nil,
					newEq("a.x=c.x"),

					""), // (A)B x (C)
				newEdge2(plan.JoinTypeInner, "1001", "1001", "1110", "0001", []conflictRule{{from: 4, to: 2}},
					newEq("a.x=d.x"),

					""), // (A)BC x (D)
				newEdge2(plan.JoinTypeInner, "0101", "0101", "1110", "0001", nil,
					newEq("b.x=d.x"),

					""), // A(B)C x (D)
			},
		},
		{
			name: "left joins",
			in: plan.NewLeftOuterJoin(
				plan.NewInnerJoin(
					plan.NewInnerJoin(
						tableNode(db, "a"),
						tableNode(db, "b"),
						newEq("a.x = b.x"),
					),
					tableNode(db, "c"),
					newEq("b.x = c.x"),
				),
				tableNode(db, "d"),
				newEq("c.x = d.x"),
			),
			expEdges: []edge{
				newEdge2(plan.JoinTypeInner, "1010", "1010", "1100", "0010", nil,
					newEq("a.x=c.x"),
					""), // (A)B x (C)
			},
		},
		{
			name: "left join equivalence doesn't hold",
			in: plan.NewLeftOuterJoin(
				plan.NewInnerJoin(
					plan.NewInnerJoin(
						tableNode(db, "a"),
						tableNode(db, "b"),
						newEq("a.x = b.x"),
					),
					tableNode(db, "c"),
					newEq("b.x = c.x"),
				),
				tableNode(db, "d"),
				newEq("c.x = d.x"),
			),
			expEdges: []edge{
				newEdge2(plan.JoinTypeInner, "1010", "1010", "1100", "0010", nil,
					newEq("a.x=c.x"),
					""), // (A)B x (C)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewJoinOrderBuilder(NewMemo(newContext(pro), nil, nil, 0, NewDefaultCoster(), NewDefaultCarder()))
			b.populateSubgraph(tt.in)
			beforeLen := len(b.edges)
			b.ensureClosure(b.m.Root())
			newEdges := b.edges[beforeLen:]
			edgesEq(t, tt.expEdges, newEdges)
		})
	}
}

func childSchema(source string) sql.PrimaryKeySchema {
	return sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "x", Source: source, Type: types.Int64, Nullable: false},
		{Name: "y", Source: source, Type: types.Text, Nullable: true},
		{Name: "z", Source: source, Type: types.Int64, Nullable: true},
	}, 0)
}

func tableNode(db *memory.Database, name string) sql.Node {
	t := memory.NewTable(db, name, childSchema(name), nil)
	t.EnablePrimaryKeyIndexes()
	tabId, colId := getIds([]string{name, "x"})
	colset := sql.NewColSet(sql.ColumnId(colId), sql.ColumnId(colId+1), sql.ColumnId(colId+2))
	return plan.NewResolvedTable(t, nil, nil).WithId(sql.TableId(tabId)).WithColumns(colset)
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

func assertScalarEq(t *testing.T, exp, cmp sql.Expression) {
	switch cmp := cmp.(type) {
	case *expression.Equals:
		exp, ok := exp.(*expression.Equals)
		require.True(t, ok)
		assertScalarEq(t, exp.Left(), cmp.Left())
		assertScalarEq(t, exp.Right(), cmp.Right())
	case *expression.Literal:
		exp, ok := exp.(*expression.Literal)
		require.True(t, ok)
		require.Equal(t, exp.Value(), cmp.Value())
	case *expression.GetField:
		exp, ok := exp.(*expression.GetField)
		require.True(t, ok)
		require.Equal(t, exp.Table(), cmp.Table())
		require.Equal(t, exp.Name(), cmp.Name())
		require.Equal(t, exp.String(), cmp.String())
	}
}
