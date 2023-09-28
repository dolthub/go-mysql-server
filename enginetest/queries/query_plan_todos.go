// Copyright 2020-2023 Dolthub, Inc.
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

package queries

type QueryPlanTest struct {
	Query        string
	ExpectedPlan string
	Skip         bool
}

// QueryPlanTODOs are queries where the query planner produces a correct (results) but suboptimal plan.
var QueryPlanTODOs = []QueryPlanTest{
	{
		// TODO: this should use an index. Extra join condition should get moved out of the join clause into a filter
		Query: `SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0 ORDER BY 2,3`,
		ExpectedPlan: "Sort(niltable.i ASC, niltable.f ASC)\n" +
			" └─ Project(one_pk.pk, niltable.i, niltable.f)\n" +
			"     └─ RightJoin((one_pk.pk = niltable.i) AND (one_pk.pk > 0))\n" +
			"         ├─ Projected table access on [pk]\n" +
			"         │   └─ Table(one_pk)\n" +
			"         └─ Projected table access on [i f]\n" +
			"             └─ Table(niltable)\n" +
			"",
	},
}
