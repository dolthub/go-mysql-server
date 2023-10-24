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
├── G3: (colref: 'a.x')
├── G4: (colref: 'b.x')
├── G5: (equal 3 4)
├── G6: (innerjoin 2 1) (innerjoin 1 2)
├── G7: (tablescan: c)
├── G8: (colref: 'c.x')
├── G9: (equal 4 8)
├── G10: (innerjoin 7 6) (innerjoin 18 2) (innerjoin 2 18) (innerjoin 19 1) (innerjoin 1 19) (innerjoin 6 7)
├── G11: (tablescan: d)
├── G12: (colref: 'd.x')
├── G13: (equal 8 12)
├── G14: (innerjoin 11 10) (innerjoin 20 19) (innerjoin 19 20) (innerjoin 21 18) (innerjoin 18 21) (innerjoin 22 7) (innerjoin 7 22) (innerjoin 23 6) (innerjoin 6 23) (innerjoin 24 2) (innerjoin 2 24) (innerjoin 25 1) (innerjoin 1 25) (innerjoin 10 11)
├── G15: (equal 3 8)
├── G16: (equal 3 12)
├── G17: (equal 4 12)
├── G18: (innerjoin 7 1) (innerjoin 1 7)
├── G19: (innerjoin 7 2) (innerjoin 2 7)
├── G20: (innerjoin 11 1) (innerjoin 1 11)
├── G21: (innerjoin 11 2) (innerjoin 2 11)
├── G22: (innerjoin 11 6) (innerjoin 6 11) (innerjoin 20 2) (innerjoin 2 20) (innerjoin 21 1) (innerjoin 1 21)
├── G23: (innerjoin 11 7) (innerjoin 7 11)
├── G24: (innerjoin 11 18) (innerjoin 18 11) (innerjoin 20 7) (innerjoin 7 20) (innerjoin 23 1) (innerjoin 1 23)
└── G25: (innerjoin 11 19) (innerjoin 19 11) (innerjoin 21 7) (innerjoin 7 21) (innerjoin 23 2) (innerjoin 2 23)
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
├── G18: (innerjoin 16 6) (leftjoin 30 2) (innerjoin 6 16)
├── G19: (tablescan: f)
├── G20: (tablescan: g)
├── G21: (colref: 'f.x')
├── G22: (colref: 'g.x')
├── G23: (equal 21 22)
├── G24: (innerjoin 20 19) (innerjoin 19 20)
├── G25: (equal 14 22)
├── G26: (innerjoin 20 35) (innerjoin 35 20) (innerjoin 37 33) (innerjoin 33 37) (innerjoin 38 32) (innerjoin 32 38) (innerjoin 40 19) (innerjoin 19 40) (innerjoin 24 18) (innerjoin 42 16) (innerjoin 16 42) (innerjoin 43 6) (innerjoin 6 43) (leftjoin 44 2) (innerjoin 18 24)
├── G27: (equal 3 21)
├── G28: (equal 3 22)
├── G29: (equal 14 21)
├── G30: (innerjoin 16 1) (innerjoin 1 16)
├── G31: (innerjoin 19 1) (innerjoin 1 19)
├── G32: (innerjoin 19 6) (innerjoin 6 19) (leftjoin 31 2)
├── G33: (innerjoin 19 16) (innerjoin 16 19)
├── G34: (innerjoin 19 30) (innerjoin 30 19) (innerjoin 31 16) (innerjoin 16 31) (innerjoin 33 1) (innerjoin 1 33)
├── G35: (innerjoin 19 18) (innerjoin 18 19) (innerjoin 32 16) (innerjoin 16 32) (innerjoin 33 6) (innerjoin 6 33) (leftjoin 34 2)
├── G36: (innerjoin 20 1) (innerjoin 1 20)
├── G37: (innerjoin 20 6) (innerjoin 6 20) (leftjoin 36 2)
├── G38: (innerjoin 20 16) (innerjoin 16 20)
├── G39: (innerjoin 20 30) (innerjoin 30 20) (innerjoin 36 16) (innerjoin 16 36) (innerjoin 38 1) (innerjoin 1 38)
├── G40: (innerjoin 20 18) (innerjoin 18 20) (innerjoin 37 16) (innerjoin 16 37) (innerjoin 38 6) (innerjoin 6 38) (leftjoin 39 2)
├── G41: (innerjoin 20 31) (innerjoin 31 20) (innerjoin 36 19) (innerjoin 19 36) (innerjoin 24 1) (innerjoin 1 24)
├── G42: (innerjoin 20 32) (innerjoin 32 20) (innerjoin 37 19) (innerjoin 19 37) (innerjoin 24 6) (innerjoin 6 24) (leftjoin 41 2)
├── G43: (innerjoin 20 33) (innerjoin 33 20) (innerjoin 38 19) (innerjoin 19 38) (innerjoin 24 16) (innerjoin 16 24)
└── G44: (innerjoin 20 34) (innerjoin 34 20) (innerjoin 36 33) (innerjoin 33 36) (innerjoin 38 31) (innerjoin 31 38) (innerjoin 39 19) (innerjoin 19 39) (innerjoin 24 30) (innerjoin 30 24) (innerjoin 41 16) (innerjoin 16 41) (innerjoin 43 1) (innerjoin 1 43)
`,
		},
		{
			name: "test fast reordering algorithm",
			// Optimized plan appears as G11 - (innerjoin 1 12)
			in: plan.NewInnerJoin(
				plan.NewCrossJoin(
					tableNode(db, "t1"),
					tableNode(db, "t3"),
				),
				tableNode(db, "t2"),
				expression.NewAnd(newEq("t1.x = t2.z"), newEq("t2.x = t3.z")),
			),

			forceFastReorder: true,
			plans: `memo:
├── G1: (tablescan: t1)
├── G2: (tablescan: t3)
├── G3: (crossjoin 1 2)
├── G4: (tablescan: t2)
├── G5: (colref: 't1.x')
├── G6: (colref: 't2.z')
├── G7: (equal 5 6)
├── G8: (colref: 't2.x')
├── G9: (colref: 't3.z')
├── G10: (equal 8 9)
├── G11: (innerjoin 1 12) (innerjoin 12 1) (innerjoin 3 4)
└── G12: (innerjoin 4 2) (innerjoin 2 4)
`,
		},
		{
			name: "test fast reordering algorithm on bushy join",
			// Optimized plan appears as G16: (innerjoin 7 17)
			in: plan.NewInnerJoin(
				plan.NewInnerJoin(
					tableNode(db, "t3"),
					tableNode(db, "t4"),
					newEq("t3.x = t4.z"),
				),
				plan.NewInnerJoin(
					tableNode(db, "t1"),
					tableNode(db, "t2"),
					newEq("t1.x = t2.z"),
				),
				newEq("t2.x = t3.z"),
			),

			forceFastReorder: true,
			plans: `memo:
├── G1: (tablescan: t3)
├── G2: (tablescan: t4)
├── G3: (colref: 't3.x')
├── G4: (colref: 't4.z')
├── G5: (equal 3 4)
├── G6: (innerjoin 1 2) (innerjoin 2 1) (innerjoin 1 2)
├── G7: (tablescan: t1)
├── G8: (tablescan: t2)
├── G9: (colref: 't1.x')
├── G10: (colref: 't2.z')
├── G11: (equal 9 10)
├── G12: (innerjoin 7 8)
├── G13: (colref: 't2.x')
├── G14: (colref: 't3.z')
├── G15: (equal 13 14)
├── G16: (innerjoin 7 17) (innerjoin 17 7) (innerjoin 6 12)
└── G17: (innerjoin 8 6) (innerjoin 6 8)
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
					tableNode(db, "a"),
					tableNode(db, "b"),
				),
				tableNode(db, "c"),
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
					tableNode(db, "b"),
					tableNode(db, "c"),
					newEq("b.x=c.x"),
				),
				tableNode(db, "a"),
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
			b := NewJoinOrderBuilder(NewMemo(newContext(pro), nil, nil, 0, NewDefaultCoster(), NewDefaultCarder()))
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
			Gf:         expression.NewGetFieldWithTable(0, types.Int64, "db", parts[0], parts[1], false),
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
		expression.NewGetFieldWithTable(0, types.Int64, "db", left[0], left[1], false),
		expression.NewGetFieldWithTable(0, types.Int64, "db", right[0], right[1], false),
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
					&Equal{
						scalarBase: &scalarBase{},
						Left:       newColRef(1, 1, "a.x"),
						Right:      newColRef(7, 7, "c.x"),
					}, ""), // (A)B x (C)
				newEdge2(plan.JoinTypeInner, "1001", "1001", "1110", "0001", []conflictRule{{from: 4, to: 2}},
					&Equal{
						scalarBase: &scalarBase{},
						Left:       newColRef(1, 1, "a.x"),
						Right:      newColRef(11, 10, "d.x"),
					},
					""), // (A)BC x (D)
				newEdge2(plan.JoinTypeInner, "0101", "0101", "1110", "0001", nil,
					&Equal{
						scalarBase: &scalarBase{},
						Left:       newColRef(2, 4, "b.x"),
						Right:      newColRef(11, 10, "d.x"),
					},
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
					&Equal{
						scalarBase: &scalarBase{},
						Left:       newColRef(1, 1, "a.x"),
						Right:      newColRef(7, 7, "c.x"),
					}, ""), // (A)B x (C)
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
					&Equal{
						scalarBase: &scalarBase{},
						Left:       newColRef(1, 1, "a.x"),
						Right:      newColRef(7, 7, "c.x"),
					}, ""), // (A)B x (C)
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
