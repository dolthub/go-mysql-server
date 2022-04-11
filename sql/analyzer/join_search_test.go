// Copyright 2020-2021 Dolthub, Inc.
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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestBuildJoinTree(t *testing.T) {
	type joinTreeTest struct {
		name       string
		tableOrder *joinOrderNode
		joinConds  []*joinCond
		joinTree   *joinSearchNode
	}

	db := memory.NewDatabase("db")
	cat := map[string]*plan.ResolvedTable{
		"A": plan.NewResolvedTable(
			memory.NewTable(
				"A",
				sql.NewPrimaryKeySchema(sql.Schema{{Name: "col", Type: sql.Text, Nullable: true, Source: "A"}}),
				nil,
			), db, nil),

		"B": plan.NewResolvedTable(
			memory.NewTable(
				"A",
				sql.NewPrimaryKeySchema(sql.Schema{{Name: "col", Type: sql.Text, Nullable: true, Source: "B"}}),
				nil,
			), db, nil),
		"C": plan.NewResolvedTable(
			memory.NewTable(
				"A",
				sql.NewPrimaryKeySchema(sql.Schema{{Name: "col", Type: sql.Text, Nullable: true, Source: "C"}}),
				nil,
			), db, nil),
		"D": plan.NewResolvedTable(
			memory.NewTable(
				"A",
				sql.NewPrimaryKeySchema(sql.Schema{{Name: "col", Type: sql.Text, Nullable: true, Source: "D"}}),
				nil,
			), db, nil),
		"E": plan.NewResolvedTable(
			memory.NewTable(
				"A",
				sql.NewPrimaryKeySchema(sql.Schema{{Name: "col", Type: sql.Text, Nullable: true, Source: "E"}}),
				nil,
			), db, nil),
	}

	// These tests are a little fragile: for many of these joins, there is more than one correct join tree.
	// For example, a join on tables A, B, C that wants the tables visited in that order has two solutions:
	// join(A, join(B, C, B=C), A=B), OR join(join(A, B, A=B), C, B=C)
	// The test cases obviously choose the one actually produced by the current algorithm, but it's very arbitrary which
	// one is returned.
	testCases := []joinTreeTest{
		{
			name:       "linear join, ABC",
			tableOrder: tableOrder(db, cat, "A", "B", "C"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("B", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "B"),
				left:     jt(cat, "A"),
				right: &joinSearchNode{
					joinCond: jc("B", "C"),
					left:     jt(cat, "B"),
					right:    jt(cat, "C"),
				},
			},
		},
		{
			name:       "linear join, ACB", // 👩‍⚖️
			tableOrder: tableOrder(db, cat, "A", "C", "B"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("B", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "B"),
				left:     jt(cat, "A"),
				right: &joinSearchNode{
					joinCond: jc("B", "C"),
					left:     jt(cat, "C"),
					right:    jt(cat, "B"),
				},
			},
		},
		{
			name:       "linear join, BAC",
			tableOrder: tableOrder(db, cat, "B", "A", "C"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("B", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("B", "C"),
				left: &joinSearchNode{
					joinCond: jc("A", "B"),
					left:     jt(cat, "B"),
					right:    jt(cat, "A"),
				},
				right: jt(cat, "C"),
			},
		},
		{
			name:       "linear join, BCA",
			tableOrder: tableOrder(db, cat, "B", "C", "A"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("B", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "B"),
				left: &joinSearchNode{
					joinCond: jc("B", "C"),
					left:     jt(cat, "B"),
					right:    jt(cat, "C"),
				},
				right: jt(cat, "A"),
			},
		},
		{
			name:       "linear join, CAB",
			tableOrder: tableOrder(db, cat, "C", "A", "B"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("B", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("B", "C"),
				left:     jt(cat, "C"),
				right: &joinSearchNode{
					joinCond: jc("A", "B"),
					left:     jt(cat, "A"),
					right:    jt(cat, "B"),
				},
			},
		},
		{
			name:       "linear join, CBA",
			tableOrder: tableOrder(db, cat, "C", "B", "A"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("B", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("B", "C"),
				left:     jt(cat, "C"),
				right: &joinSearchNode{
					joinCond: jc("A", "B"),
					left:     jt(cat, "B"),
					right:    jt(cat, "A"),
				},
			},
		},
		{
			name:       "all joined to A, ABC",
			tableOrder: tableOrder(db, cat, "A", "B", "C"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("A", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "C"),
				left: &joinSearchNode{
					joinCond: jc("A", "B"),
					left:     jt(cat, "A"),
					right:    jt(cat, "B"),
				},
				right: jt(cat, "C"),
			},
		},
		{
			name:       "all joined to A, CBA",
			tableOrder: tableOrder(db, cat, "C", "B", "A"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("A", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "C"),
				left:     jt(cat, "C"),
				right: &joinSearchNode{
					joinCond: jc("A", "B"),
					left:     jt(cat, "B"),
					right:    jt(cat, "A"),
				},
			},
		},
		{
			name:       "all joined to A, BAC",
			tableOrder: tableOrder(db, cat, "B", "A", "C"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("A", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "B"),
				left:     jt(cat, "B"),
				right: &joinSearchNode{
					joinCond: jc("A", "C"),
					left:     jt(cat, "A"),
					right:    jt(cat, "C"),
				},
			},
		},
		{
			name:       "A to B, A+B to C",
			tableOrder: tableOrder(db, cat, "A", "B", "C"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jce(and(jceq("A", "C"), jceq("B", "C"))),
			},
			joinTree: &joinSearchNode{
				joinCond: jce(and(jceq("A", "C"), jceq("B", "C"))),
				left: &joinSearchNode{
					joinCond: jc("A", "B"),
					left:     jt(cat, "A"),
					right:    jt(cat, "B"),
				},
				right: jt(cat, "C"),
			},
		},
		{
			name:       "B to A, A+B to C",
			tableOrder: tableOrder(db, cat, "B", "A", "C"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jce(and(jceq("A", "C"), jceq("B", "C"))),
			},
			joinTree: &joinSearchNode{
				joinCond: jce(and(jceq("A", "C"), jceq("B", "C"))),
				left: &joinSearchNode{
					joinCond: jc("A", "B"),
					left:     jt(cat, "B"),
					right:    jt(cat, "A"),
				},
				right: jt(cat, "C"),
			},
		},
		{
			name:       "linear join, ABCD",
			tableOrder: tableOrder(db, cat, "A", "B", "C", "D"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("B", "C"),
				jc("C", "D"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "B"),
				left:     jt(cat, "A"),
				right: &joinSearchNode{
					joinCond: jc("B", "C"),
					left:     jt(cat, "B"),
					right: &joinSearchNode{
						joinCond: jc("C", "D"),
						left:     jt(cat, "C"),
						right:    jt(cat, "D"),
					},
				},
			},
		},
		{
			name:       "linear join, BCDA",
			tableOrder: tableOrder(db, cat, "B", "C", "D", "A"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("B", "C"),
				jc("C", "D"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "B"),
				left: &joinSearchNode{
					joinCond: jc("B", "C"),
					left:     jt(cat, "B"),
					right: &joinSearchNode{
						joinCond: jc("C", "D"),
						left:     jt(cat, "C"),
						right:    jt(cat, "D"),
					},
				},
				right: jt(cat, "A"),
			},
		},
		{
			name:       "linear join, DABC",
			tableOrder: tableOrder(db, cat, "D", "A", "B", "C"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("B", "C"),
				jc("C", "D"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("C", "D"),
				left:     jt(cat, "D"),
				right: &joinSearchNode{
					joinCond: jc("A", "B"),
					left:     jt(cat, "A"),
					right: &joinSearchNode{
						joinCond: jc("B", "C"),
						left:     jt(cat, "B"),
						right:    jt(cat, "C"),
					},
				},
			},
		},
		{
			name:       "linear join, CDBA",
			tableOrder: tableOrder(db, cat, "C", "D", "B", "A"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("B", "C"),
				jc("C", "D"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("B", "C"),
				left: &joinSearchNode{
					joinCond: jc("C", "D"),
					left:     jt(cat, "C"),
					right:    jt(cat, "D"),
				},
				right: &joinSearchNode{
					joinCond: jc("A", "B"),
					left:     jt(cat, "B"),
					right:    jt(cat, "A"),
				},
			},
		},
		{
			name:       "all joined to A, ABCD",
			tableOrder: tableOrder(db, cat, "A", "B", "C", "D"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("A", "C"),
				jc("A", "D"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "D"),
				left: &joinSearchNode{
					joinCond: jc("A", "C"),
					left: &joinSearchNode{
						joinCond: jc("A", "B"),
						left:     jt(cat, "A"),
						right:    jt(cat, "B"),
					},
					right: jt(cat, "C"),
				},
				right: jt(cat, "D"),
			},
		},
		{
			name:       "all joined to A, BDAC",
			tableOrder: tableOrder(db, cat, "B", "D", "A", "C"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("A", "C"),
				jc("A", "D"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "B"),
				left:     jt(cat, "B"),
				right: &joinSearchNode{
					joinCond: jc("A", "D"),
					left:     jt(cat, "D"),
					right: &joinSearchNode{
						joinCond: jc("A", "C"),
						left:     jt(cat, "A"),
						right:    jt(cat, "C"),
					},
				},
			},
		},
		{
			name:       "all joined to A, CABD",
			tableOrder: tableOrder(db, cat, "C", "A", "B", "D"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("A", "C"),
				jc("A", "D"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "C"),
				left:     jt(cat, "C"),
				right: &joinSearchNode{
					joinCond: jc("A", "D"),
					left: &joinSearchNode{
						joinCond: jc("A", "B"),
						left:     jt(cat, "A"),
						right:    jt(cat, "B"),
					},
					right: jt(cat, "D"),
				},
			},
		},
		{
			name:       "all joined to A, DCBA",
			tableOrder: tableOrder(db, cat, "D", "C", "B", "A"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("A", "C"),
				jc("A", "D"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "D"),
				left:     jt(cat, "D"),
				right: &joinSearchNode{
					joinCond: jc("A", "C"),
					left:     jt(cat, "C"),
					right: &joinSearchNode{
						joinCond: jc("A", "B"),
						left:     jt(cat, "B"),
						right:    jt(cat, "A"),
					},
				},
			},
		},
		{
			name:       "A to B, A+B to C, A+B+C to D",
			tableOrder: tableOrder(db, cat, "A", "B", "C", "D"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jce(and(jceq("A", "C"), jceq("B", "C"))),
				jce(and(jceq("A", "D"), and(jceq("B", "D"), jceq("C", "D")))),
			},
			joinTree: &joinSearchNode{
				joinCond: jce(and(jceq("A", "D"), and(jceq("B", "D"), jceq("C", "D")))),
				left: &joinSearchNode{
					joinCond: jce(and(jceq("A", "C"), jceq("B", "C"))),
					left: &joinSearchNode{
						joinCond: jc("A", "B"),
						left:     jt(cat, "A"),
						right:    jt(cat, "B"),
					},
					right: jt(cat, "C"),
				},
				right: jt(cat, "D"),
			},
		},
		{
			name:       "linear join, ABCDE",
			tableOrder: tableOrder(db, cat, "A", "B", "C", "D", "E"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("B", "C"),
				jc("C", "D"),
				jc("D", "E"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "B"),
				left:     jt(cat, "A"),
				right: &joinSearchNode{
					joinCond: jc("B", "C"),
					left:     jt(cat, "B"),
					right: &joinSearchNode{
						joinCond: jc("C", "D"),
						left:     jt(cat, "C"),
						right: &joinSearchNode{
							joinCond: jc("D", "E"),
							left:     jt(cat, "D"),
							right:    jt(cat, "E"),
						},
					},
				},
			},
		},
		{
			name:       "linear join, ECBAD",
			tableOrder: tableOrder(db, cat, "E", "C", "B", "A", "D"),
			joinConds: []*joinCond{
				jc("A", "B"),
				jc("B", "C"),
				jc("C", "D"),
				jc("D", "E"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("D", "E"),
				left:     jt(cat, "E"),
				right: &joinSearchNode{
					joinCond: jc("C", "D"),
					left: &joinSearchNode{
						joinCond: jc("B", "C"),
						left:     jt(cat, "C"),
						right: &joinSearchNode{
							joinCond: jc("A", "B"),
							left:     jt(cat, "B"),
							right:    jt(cat, "A"),
						},
					},
					right: jt(cat, "D"),
				},
			},
		},
		{
			name:       "star join with C in middle, BDACE",
			tableOrder: tableOrder(db, cat, "B", "D", "A", "C", "E"),
			joinConds: []*joinCond{
				jc("A", "C"),
				jc("B", "C"),
				jc("D", "C"),
				jc("E", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("B", "C"),
				left:     jt(cat, "B"),
				right: &joinSearchNode{
					joinCond: jc("D", "C"),
					left:     jt(cat, "D"),
					right: &joinSearchNode{
						joinCond: jc("A", "C"),
						left:     jt(cat, "A"),
						right: &joinSearchNode{
							joinCond: jc("E", "C"),
							left:     jt(cat, "C"),
							right:    jt(cat, "E"),
						},
					},
				},
			},
		},
		{
			name:       "branching join, EBDCA",
			tableOrder: tableOrder(db, cat, "E", "B", "D", "C", "A"),
			joinConds: []*joinCond{
				jc("A", "C"),
				jc("B", "C"),
				jc("B", "E"),
				jc("B", "D"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("B", "E"),
				left:     jt(cat, "E"),
				right: &joinSearchNode{
					joinCond: jc("B", "C"),
					left: &joinSearchNode{
						joinCond: jc("B", "D"),
						left:     jt(cat, "B"),
						right:    jt(cat, "D"),
					},
					right: &joinSearchNode{
						joinCond: jc("A", "C"),
						left:     jt(cat, "C"),
						right:    jt(cat, "A"),
					},
				},
			},
		},
		{
			name: "explicit subtree, A((EB)(DC))",
			tableOrder: tableOrder(db, cat, "A", &joinOrderNode{
				left:  tableOrder(db, cat, "E", "B"),
				right: tableOrder(db, cat, "D", "C"),
			}),
			joinConds: []*joinCond{
				jc("A", "E"),
				jc("B", "D"),
				jc("B", "E"),
				jc("D", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "E"),
				left:     jt(cat, "A"),
				right: &joinSearchNode{
					joinCond: jc("B", "D"),
					left: &joinSearchNode{
						joinCond: jc("B", "E"),
						left:     jt(cat, "E"),
						right:    jt(cat, "B"),
					},
					right: &joinSearchNode{
						joinCond: jc("D", "C"),
						left:     jt(cat, "D"),
						right:    jt(cat, "C"),
					},
				},
			},
		},
		{
			name: "explicit subtree, A((EB)(D))C",
			tableOrder: tableOrder(db, cat, "A", &joinOrderNode{
				left:  tableOrder(db, cat, "E", "B"),
				right: tableOrder(db, cat, "D"),
			}, "C"),
			joinConds: []*joinCond{
				jc("A", "E"),
				jc("B", "D"),
				jc("B", "E"),
				jc("D", "C"),
			},
			joinTree: &joinSearchNode{
				joinCond: jc("A", "E"),
				left:     jt(cat, "A"),
				right: &joinSearchNode{
					joinCond: jc("D", "C"),
					left: &joinSearchNode{
						joinCond: jc("B", "D"),
						left: &joinSearchNode{
							joinCond: jc("B", "E"),
							left:     jt(cat, "E"),
							right:    jt(cat, "B"),
						},
						right: jt(cat, "D"),
					},
					right: jt(cat, "C"),
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			joinTree := buildJoinTree(tt.tableOrder, tt.joinConds)
			if !assert.Equal(t, tt.joinTree, joinTree) {
				fmt.Printf("Expected:\n%s, but got:\n%s", tt.joinTree, joinTree)
			}
		})
	}
}

func TestJoinFactors(t *testing.T) {
	tests := []struct {
		name string
		join string
		exp  int
	}{
		{
			name: "cross join",
			join: `SELECT t1.*
					  FROM
						mytable as t1,
						mytable as t2,
						mytable as t3,
						mytable as t4,
						mytable as t5,
						mytable as t6,
						mytable as t7,
						mytable as t8,
						mytable as t9,
						mytable as t10,
						mytable as t11,
						mytable as t12,
						mytable as t13,
						mytable as t14,
						mytable as t15`,
			exp: 15,
		},
		{
			name: "left join",
			join: `SELECT t1.*
					  FROM
						mytable as t1
						LEFT JOIN mytable as t2 ON t1.i = t2.i
						LEFT JOIN mytable as t3 ON t2.i = t3.i
						LEFT JOIN mytable as t4 ON t3.i = t4.i
						LEFT JOIN mytable as t5 ON t4.i = t5.i
						LEFT JOIN mytable as t6 ON t5.i = t6.i
						LEFT JOIN mytable as t7 ON t6.i = t7.i
						LEFT JOIN mytable as t8 ON t7.i = t8.i
						LEFT JOIN mytable as t9 ON t8.i = t9.i
						LEFT JOIN mytable as t10 ON t9.i = t10.i
						LEFT JOIN mytable as t11 ON t10.i = t11.i
						LEFT JOIN mytable as t12 ON t11.i = t12.i
						LEFT JOIN mytable as t13 ON t12.i = t13.i
						LEFT JOIN mytable as t14 ON t13.i = t14.i
						LEFT JOIN mytable as t15 ON t14.i = t15.i`,
			exp: 15,
		},
		{
			name: "right join",
			join: `SELECT t1.*
					  FROM
						mytable as t1
						RIGHT JOIN mytable as t2 ON t1.i = t2.i
						RIGHT JOIN mytable as t3 ON t2.i = t3.i
						RIGHT JOIN mytable as t4 ON t3.i = t4.i
						RIGHT JOIN mytable as t5 ON t4.i = t5.i
						RIGHT JOIN mytable as t6 ON t5.i = t6.i
						RIGHT JOIN mytable as t7 ON t6.i = t7.i
						RIGHT JOIN mytable as t8 ON t7.i = t8.i
						RIGHT JOIN mytable as t9 ON t8.i = t9.i
						RIGHT JOIN mytable as t10 ON t9.i = t10.i
						RIGHT JOIN mytable as t11 ON t10.i = t11.i
						RIGHT JOIN mytable as t12 ON t11.i = t12.i
						RIGHT JOIN mytable as t13 ON t12.i = t13.i
						RIGHT JOIN mytable as t14 ON t13.i = t14.i
						RIGHT JOIN mytable as t15 ON t14.i = t15.i`,
			exp: 15,
		},
		{
			name: "inner join",
			join: `SELECT t1.*
					  FROM
						mytable as t1,
						mytable as t2,
						mytable as t3,
						mytable as t4,
						mytable as t5,
						mytable as t6,
						mytable as t7,
						mytable as t8,
						mytable as t9,
						mytable as t10,
						mytable as t11,
						mytable as t12,
						mytable as t13,
						mytable as t14,
						mytable as t15
					  WHERE
						t1.i = t2.i and
						t2.i = t3.i and
						t3.i = t4.i and
						t4.i = t5.i and
						t5.i = t6.i and
						t6.i = t7.i and
						t7.i = t8.i and
						t8.i = t9.i and
						t9.i = t10.i and
						t10.i = t11.i and
						t11.i = t12.i and
						t12.i = t13.i and
						t13.i = t14.i and
						t14.i = t15.i`,
			exp: 15,
		},
		{
			name: "filter -> cross join",
			join: `SELECT t1.i, t2.s
					  FROM
						mytable as t1,
						mytable as t2,
						mytable as t3,
						mytable as t4,
						mytable as t5,
						mytable as t6,
						mytable as t7,
						mytable as t8,
						mytable as t9,
						mytable as t10,
						mytable as t11,
						mytable as t12,
						mytable as t13,
						mytable as t14,
						mytable as t15
					  WHERE
						t1.s = t2.i`,
			exp: 15,
		},
		{
			name: "filter -> cross join",
			join: `INSERT into a select t1.*
					  FROM
						mytable as t1,
						mytable as t2,
						mytable as t3,
						mytable as t4,
						mytable as t5,
						mytable as t6`,
			exp: 6,
		},
		{
			name: "filter -> cross join",
			join: `select * from (
					select t1.*
					  FROM
						mytable as t1,
						mytable as t2,
						mytable as t3,
						mytable as t4,
						mytable as t5,
						mytable as t6
					  WHERE t1.s = t2.i) as sq`,
			exp: 6,
		},
		{
			name: "simple union",
			join: `select * from (
					select t1.*
					  FROM
						mytable as t1,
						mytable as t2,
						mytable as t3,
						mytable as t4,
						mytable as t5,
						mytable as t6
					  WHERE t1.s = t2.i
					  union
					select t1.*
					  FROM
						mytable as t1,
						mytable as t2,
						mytable as t3,
						mytable as t4,
						mytable as t5,
						mytable as t6
					  WHERE t1.s = t2.i) as sq`,
			exp: 12,
		},
		{
			name: "join -> subquery",
			join: `select * from (
					select t1.*
					  FROM
						mytable as t1,
						mytable as t2,
						mytable as t3,
						mytable as t4,
						mytable as t5,
						mytable as t6
					  WHERE t1.s = t2.i) as sq
					join mytable on sq.i = mytable.i`,
			exp: 7,
		},
		{
			name: "join -> subquery -> union",
			join: `select * from (
					select t1.*
					  FROM
						mytable as t1,
						mytable as t2,
						mytable as t3,
						mytable as t4,
						mytable as t5,
						mytable as t6
					  WHERE t1.s = t2.i
					  union
					select t1.*
					  FROM
						mytable as t1,
						mytable as t2,
						mytable as t3,
						mytable as t4,
						mytable as t5,
						mytable as t6
					  WHERE t1.s = t2.i) as sq
					join mytable on sq.i = mytable.i`,

			exp: 13,
		},
		{
			name: "insert -> source = join -> subquery -> union",
			join: `insert into a select * from (
					select t1.*
					  FROM
						mytable as t1,
						mytable as t2,
						mytable as t3,
						mytable as t4,
						mytable as t5,
						mytable as t6
					  WHERE t1.s = t2.i
					  union
					select t1.*
					  FROM
						mytable as t1,
						mytable as t2,
						mytable as t3,
						mytable as t4,
						mytable as t5,
						mytable as t6
					  WHERE t1.s = t2.i) as sq
					join mytable on sq.i = mytable.i`,

			exp: 13,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			n, err := parse.Parse(ctx, tt.join)
			assert.NoError(t, err)
			cmp := countTableFactors(n)
			assert.Equal(t, tt.exp, cmp)
		})
	}
}

// jc == join cond
func jc(leftTable, rightTable string) *joinCond {
	return &joinCond{
		cond: jceq(leftTable, rightTable),
	}
}

// jce == joinCond from expression
func jce(expression sql.Expression) *joinCond {
	return &joinCond{
		cond: expression,
	}
}

// jceq == joinCond equals expression
func jceq(leftTable string, rightTable string) sql.Expression {
	return eq(gf(0, leftTable, "col"), gf(0, rightTable, "col"))
}

// jt == join table
func jt(cat map[string]*plan.ResolvedTable, name string) *joinSearchNode {
	return &joinSearchNode{
		table: name,
		node:  cat[name],
	}
}

func tableOrder(db sql.Database, cat map[string]*plan.ResolvedTable, tables ...interface{}) *joinOrderNode {
	jo := &joinOrderNode{}
	for i, table := range tables {
		switch t := table.(type) {
		case string:
			jo.commutes = append(jo.commutes, joinOrderNode{name: t, node: cat[t]})
		case *joinOrderNode:
			jo.commutes = append(jo.commutes, *t)
		default:
			panic("unknown type for argument to tableOrder")
		}
		jo.order = append(jo.order, i)
	}
	return jo
}
