// Copyright 2020 Liquidata, Inc.
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

package enginetest

type QueryPlanTest struct {
	Query        string
	ExpectedPlan string
}

// QueryPlanTest is a test of generating the right query plans for different queries in the presence of indexes and
// other features. These tests are fragile because they rely on string representations of query plans, but they're much
// easier to construct this way.
var PlanTests = []QueryPlanTest{
	{
		Query: "SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2",
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: "SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2",
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: "SELECT i, i2, s2 FROM othertable JOIN mytable ON i = i2",
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: "SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2",
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: "SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i2 = i",
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: "SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i2 = i",
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: "SELECT i, i2, s2 FROM othertable JOIN mytable ON i2 = i",
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: "SELECT s2, i2, i FROM othertable JOIN mytable ON i2 = i",
		ExpectedPlan: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: "SELECT * FROM mytable mt INNER JOIN othertable ot ON mt.i = ot.i2 AND mt.i > 2",
		ExpectedPlan: "IndexedJoin(mt.i = ot.i2)\n" +
			" ├─ Filter(mt.i > 2)\n" +
			" │   └─ TableAlias(mt)\n" +
			" │       └─ Indexed table access on index [mytable.i]\n" +
			" │           └─ Table(mytable)\n" +
			" └─ TableAlias(ot)\n" +
			"     └─ Table(othertable)\n" +
			"",
	},
	{
		Query: "SELECT i, i2, s2 FROM mytable RIGHT JOIN othertable ON i = i2 - 1",
		ExpectedPlan: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ RightIndexedJoin(mytable.i = othertable.i2 - 1)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2",
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2",
		ExpectedPlan: "Project(opk.pk, tpk.pk1, tpk.pk2)\n" +
			" └─ IndexedJoin(opk.pk = tpk.pk1 AND opk.pk = tpk.pk2)\n" +
			"     ├─ TableAlias(opk)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ TableAlias(tpk)\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2",
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2",
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ RightIndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
			"     ├─ Table(two_pk)\n" +
			"     └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: "SELECT i,pk1,pk2 FROM mytable JOIN two_pk ON i-1=pk1 AND i-2=pk2",
		ExpectedPlan: "Project(mytable.i, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin(mytable.i - 1 = two_pk.pk1 AND mytable.i - 2 = two_pk.pk2)\n" +
			"     ├─ Table(mytable)\n" +
			"     └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1",
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i",
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(niltable)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i",
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(niltable)\n" +
			"     └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i AND f IS NOT NULL",
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftJoin(one_pk.pk = niltable.i AND NOT(niltable.f IS NULL))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(niltable)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0",
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightJoin(one_pk.pk = niltable.i AND one_pk.pk > 0)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(niltable)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL",
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(NOT(niltable.f IS NULL))\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ Table(niltable)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i2 > 1",
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(niltable.i2 > 1)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ Table(niltable)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i > 1",
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(niltable.i > 1)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ Table(niltable)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE c1 > 10",
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Filter(one_pk.c1 > 10)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ Table(niltable)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL",
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Filter(NOT(niltable.f IS NULL))\n" +
			"     │   └─ Table(niltable)\n" +
			"     └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1",
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Indexed table access on index [one_pk.pk]\n" +
			"     │   └─ Filter(one_pk.pk > 1)\n" +
			"     │       └─ Table(one_pk)\n" +
			"     └─ Table(niltable)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0",
		ExpectedPlan: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(one_pk.pk > 0)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(niltable)\n" +
			"         └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1",
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(two_pk)\n" +
			"     └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin(a.pk1 = b.pk1 AND a.pk2 = b.pk2)\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin(a.pk1 = b.pk2 AND a.pk2 = b.pk1)\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON b.pk1=a.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin(b.pk1 = a.pk1 AND a.pk2 = b.pk2)\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1+1=b.pk1 AND a.pk2+1=b.pk2 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ IndexedJoin(a.pk1 + 1 = b.pk1 AND a.pk2 + 1 = b.pk2)\n" +
			"         ├─ TableAlias(a)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(b)\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		// TODO: this should use an index. CrossJoin needs to be converted to InnerJoin, where clause to join cond
		Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ Filter(a.pk1 = b.pk1 AND a.pk2 = b.pk2)\n" +
			"         └─ CrossJoin\n" +
			"             ├─ TableAlias(a)\n" +
			"             │   └─ Table(two_pk)\n" +
			"             └─ TableAlias(b)\n" +
			"                 └─ Table(two_pk)\n" +
			"",
	},
	{
		// TODO: this should use an index. CrossJoin needs to be converted to InnerJoin, where clause to join cond
		Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(a.pk1 ASC, a.pk2 ASC, b.pk1 ASC)\n" +
			" └─ Project(a.pk1, a.pk2, b.pk1, b.pk2)\n" +
			"     └─ Filter(a.pk1 = b.pk2 AND a.pk2 = b.pk1)\n" +
			"         └─ CrossJoin\n" +
			"             ├─ TableAlias(a)\n" +
			"             │   └─ Table(two_pk)\n" +
			"             └─ TableAlias(b)\n" +
			"                 └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT one_pk.c5,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(one_pk.c5 ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.c5, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"         ├─ Table(two_pk)\n" +
			"         └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.c5, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(opk)\n" +
			"             └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.c5, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin(opk.pk = tpk.pk1)\n" +
			"         ├─ TableAlias(tpk)\n" +
			"         │   └─ Table(two_pk)\n" +
			"         └─ TableAlias(opk)\n" +
			"             └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk, two_pk tpk WHERE pk=pk1 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(opk.c5 ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.c5, tpk.pk1, tpk.pk2)\n" +
			"     └─ Filter(opk.pk = tpk.pk1)\n" +
			"         └─ CrossJoin\n" +
			"             ├─ TableAlias(opk)\n" +
			"             │   └─ Table(one_pk)\n" +
			"             └─ TableAlias(tpk)\n" +
			"                 └─ Table(two_pk)\n" +
			"",
	},
	{
		// TODO: this should use an index. CrossJoin needs to be converted to InnerJoin, where clause to join cond
		Query: "SELECT one_pk.c5,pk1,pk2 FROM one_pk,two_pk WHERE pk=pk1 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(one_pk.c5 ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.c5, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ Filter(one_pk.pk = two_pk.pk1)\n" +
			"         └─ CrossJoin\n" +
			"             ├─ Table(one_pk)\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i ORDER BY 1",
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ Table(niltable)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 1",
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ Filter(NOT(niltable.f IS NULL))\n" +
			"         └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"             ├─ Table(one_pk)\n" +
			"             └─ Table(niltable)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1 ORDER BY 1",
		ExpectedPlan: "Sort(one_pk.pk ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Indexed table access on index [one_pk.pk]\n" +
			"         │   └─ Filter(one_pk.pk > 1)\n" +
			"         │       └─ Table(one_pk)\n" +
			"         └─ Table(niltable)\n",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i ORDER BY 2,3",
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(niltable)\n" +
			"         └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3",
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Filter(NOT(niltable.f IS NULL))\n" +
			"         │   └─ Table(niltable)\n" +
			"         └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0 ORDER BY 2,3",
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ Filter(one_pk.pk > 0)\n" +
			"         └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"             ├─ Table(niltable)\n" +
			"             └─ Table(one_pk)\n" +
			"",
	},
	{
		// TODO: this should use an index. Extra join condition should get moved out of the join clause into a filter
		Query: "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0 ORDER BY 2,3",
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ RightJoin(one_pk.pk = niltable.i AND one_pk.pk > 0)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ Table(niltable)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ IndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk1-pk>0 AND pk2<1",
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ InnerJoin(two_pk.pk1 - one_pk.pk > 0)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Filter(two_pk.pk2 < 1)\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ORDER BY 1,2,3",
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ LeftJoin(one_pk.pk = two_pk.pk1)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
			"         ├─ Table(two_pk)\n" +
			"         └─ Table(one_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(opk.pk ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.pk, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin(opk.pk = tpk.pk1 AND opk.pk = tpk.pk2)\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=tpk.pk1 AND pk=tpk.pk2 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(opk.pk ASC, tpk.pk1 ASC, tpk.pk2 ASC)\n" +
			" └─ Project(opk.pk, tpk.pk1, tpk.pk2)\n" +
			"     └─ IndexedJoin(opk.pk = tpk.pk1 AND opk.pk = tpk.pk2)\n" +
			"         ├─ TableAlias(opk)\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ TableAlias(tpk)\n" +
			"             └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE one_pk.c1=two_pk.c1 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			"     └─ Filter(one_pk.c1 = two_pk.c1)\n" +
			"         └─ CrossJoin\n" +
			"             ├─ Table(one_pk)\n" +
			"             └─ Table(two_pk)\n",
	},
	{
		Query: "SELECT pk,pk1,pk2,one_pk.c1 AS foo, two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 ORDER BY 1,2,3",
		ExpectedPlan: "Sort(one_pk.pk ASC, two_pk.pk1 ASC, two_pk.pk2 ASC)\n" +
			" └─ Project(one_pk.pk, two_pk.pk1, two_pk.pk2, one_pk.c1 as foo, two_pk.c1 as bar)\n" +
			"     └─ InnerJoin(one_pk.c1 = two_pk.c1)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk1,pk2,one_pk.c1 AS foo,two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE one_pk.c1=10",
		ExpectedPlan: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2, one_pk.c1 as foo, two_pk.c1 as bar)\n" +
			" └─ InnerJoin(one_pk.c1 = two_pk.c1)\n" +
			"     ├─ Filter(one_pk.c1 = 10)\n" +
			"     │   └─ Table(one_pk)\n" +
			"     └─ Table(two_pk)\n" +
			"",
	},
	{
		Query: "SELECT pk,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2",
		ExpectedPlan: "Sort(t1.pk ASC, t2.pk2 ASC)\n" +
			" └─ Project(t1.pk, t2.pk2)\n" +
			"     └─ CrossJoin\n" +
			"         ├─ Filter(t1.pk = 1)\n" +
			"         │   └─ TableAlias(t1)\n" +
			"         │       └─ Indexed table access on index [one_pk.pk]\n" +
			"         │           └─ Table(one_pk)\n" +
			"         └─ Filter(t2.pk2 = 1)\n" +
			"             └─ TableAlias(t2)\n" +
			"                 └─ Table(two_pk)\n" +
			"",
	},
}
