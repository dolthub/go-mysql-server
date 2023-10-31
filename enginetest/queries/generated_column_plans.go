// Copyright 2023 Dolthub, Inc.
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

var GeneratedColumnPlanTests = []QueryPlanTest{
	{
		Query: "explain select * from generated_stored_1 where b = 2 order by a",
	},
	{
		Query: "explain select * from generated_stored_2 where b = 2 and c = 3 order by a",
	},
	{
		Query: "explain delete from generated_stored_2 where b = 3 and c = 4",
	},
	{
		Query: "explain update generated_stored_2 set a = 5, c = 10 where b = 2 and c = 3",
	},
	{
		Query: "explain select * from generated_virtual_1 where c = 7",
	},
	{
		Query: "explain update generated_virtual_1 set b = 5 where c = 3",
	},
	{
		Query: "explain delete from generated_virtual_1 where c = 6",
	},
	{
		Query: "explain select * from t1 where v = 2",
	},
	{
		Query: "explain update t1 set j = '{\"a\": 5}' where v = 2",
	},
	{
		Query: "explain delete from t1 where v = 5",
	},
}
