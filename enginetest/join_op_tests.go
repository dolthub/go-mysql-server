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

package enginetest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/memo"
)

type JoinOpTests struct {
	Query    string
	Expected []sql.UntypedSqlRow
	Skip     bool
}

var biasedCosters = map[string]memo.Coster{
	"inner":     memo.NewInnerBiasedCoster(),
	"lookup":    memo.NewLookupBiasedCoster(),
	"hash":      memo.NewHashBiasedCoster(),
	"merge":     memo.NewMergeBiasedCoster(),
	"partial":   memo.NewPartialBiasedCoster(),
	"rangeHeap": memo.NewRangeHeapBiasedCoster(),
}

func TestJoinOps(t *testing.T, harness Harness, tests []joinOpTest) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := mustNewEngine(t, harness)
			defer e.Close()

			for _, setup := range tt.setup {
				for _, statement := range setup {
					if sh, ok := harness.(SkippingHarness); ok {
						if sh.SkipQueryTest(statement) {
							t.Skip()
						}
					}
					ctx := NewContext(harness)
					RunQueryWithContext(t, e, harness, ctx, statement)
				}
			}

			if pro, ok := e.EngineAnalyzer().Catalog.DbProvider.(*memory.DbProvider); ok {
				newProv, err := pro.WithTableFunctions(memory.RequiredLookupTable{})
				require.NoError(t, err)
				e.EngineAnalyzer().Catalog.DbProvider = newProv.(sql.DatabaseProvider)
			}

			for k, c := range biasedCosters {
				e.EngineAnalyzer().Coster = c
				for _, tt := range tt.tests {
					evalJoinCorrectness(t, harness, e, fmt.Sprintf("%s join: %s", k, tt.Query), tt.Query, tt.Expected, tt.Skip)
				}
			}
		})
	}
}

type joinOpTest struct {
	name  string
	setup [][]string
	tests []JoinOpTests
}

var EngineOnlyJoinOpTests = []joinOpTest{
	{
		name: "required indexes avoid invalid plans",
		setup: [][]string{
			setup.MydbData[0],
			{
				"CREATE table xy (x int primary key, y int, unique index y_idx(y));",
				"CREATE table uv (u int primary key, v int);",
				"insert into xy values (1,0), (2,1), (0,2), (3,3);",
				"insert into uv values (0,1), (1,1), (2,2), (3,2);",
				`analyze table xy update histogram on x using data '{"row_count":1000}'`,
				`analyze table uv update histogram on u using data '{"row_count":1000}'`,
			},
		},
		tests: []JoinOpTests{
			{
				Query:    "select * from xy left join required_lookup_table('s', 2) on x = s",
				Expected: []sql.UntypedSqlRow{{0, 2, 0}, {1, 0, 1}, {2, 1, nil}, {3, 3, nil}},
			},
		},
	},
}

var DefaultJoinOpTests = []joinOpTest{
	{
		name: "bug where transitive join edge drops filters",
		setup: [][]string{
			setup.MydbData[0],
			{
				"CREATE table xy (x int primary key, y int, unique index y_idx(y));",
				"CREATE table uv (u int primary key, v int);",
				"CREATE table ab (a int primary key, b int);",
				"insert into xy values (1,0), (2,1), (0,2), (3,3);",
				"insert into uv values (0,1), (1,1), (2,2), (3,2);",
				"insert into ab values (0,2), (1,2), (2,2), (3,1);",
				`analyze table xy update histogram on x using data '{"row_count":1000}'`,
				`analyze table ab update histogram on a using data '{"row_count":1000}'`,
				`analyze table uv update histogram on u using data '{"row_count":1000}'`,
			},
		},
		tests: []JoinOpTests{
			{
				// This query is a small repro of a larger query caused by the intersection of several
				// bugs. The query below should 1) move the filters out of the join condition, and then
				// 2) push those hoisted filters on top of |uv|, where they are safe for join planning.
				// At the time of this addition, filters in the middle of join trees are unsafe and
				// at risk of being lost.
				Query:    "select /*+ JOIN_ORDER(ab,xy,uv) */ * from xy join uv on (x = u and u in (0,2)) join ab on (x = a and v < 2)",
				Expected: []sql.UntypedSqlRow{{0, 2, 0, 1, 0, 2}},
			},
		},
	},
	{
		name: "unique covering source index",
		setup: [][]string{
			setup.MydbData[0],
			{
				"Create table ab (a int primary key, b int);",
				"Create table xyz (x int primary key, y int, z int, unique key(y, z));",
				"insert into ab values (4,0), (7,1)",
				"insert into xyz values (0,2,4), (1,2,7)",
			},
		},
		tests: []JoinOpTests{
			{
				Query:    "select /*+ JOIN_ORDER(ab,xyz) */ a from ab join xyz on a = z where y = 2;",
				Expected: []sql.UntypedSqlRow{{4}, {7}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(xyz,ab) */ a from ab join xyz on a = z where y = 2;",
				Expected: []sql.UntypedSqlRow{{4}, {7}},
			},
		},
	},
	{
		name: "keyless lookup join indexes",
		setup: [][]string{
			setup.MydbData[0],
			{
				"CREATE table xy (x int, y int, z int, index y_idx(y));",
				"CREATE table ab (a int primary key, b int, c int);",
				"insert into xy values (1,0,3), (1,0,3), (0,2,1),(0,2,1);",
				"insert into ab values (0,1,1), (1,2,2), (2,3,3), (3,2,2);",
			},
		},
		tests: []JoinOpTests{
			// covering tablescan
			{
				Query:    "select /*+ JOIN_ORDER(ab,xy) */ y,a from xy join ab on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0}, {0, 0}, {2, 2}, {2, 2}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(xy,ab) */ y,a from xy join ab on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0}, {0, 0}, {2, 2}, {2, 2}},
			},
			// covering indexed source
			{
				Query:    "select /*+ JOIN_ORDER(ab,xy) */ y,a from xy join ab on y = a where y = 2",
				Expected: []sql.UntypedSqlRow{{2, 2}, {2, 2}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(xy,ab) */ y,a from xy join ab on y = a where y = 2",
				Expected: []sql.UntypedSqlRow{{2, 2}, {2, 2}},
			},
			// non-covering tablescan
			{
				Query:    "select /*+ JOIN_ORDER(ab,xy) */ y,a,z from xy join ab on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0, 3}, {0, 0, 3}, {2, 2, 1}, {2, 2, 1}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(xy,ab) */ y,a,z from xy join ab on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0, 3}, {0, 0, 3}, {2, 2, 1}, {2, 2, 1}},
			},
			// non-covering indexed source
			{
				Query:    "select /*+ JOIN_ORDER(ab,xy) */ y,a,z from xy join ab on y = a where y = 2",
				Expected: []sql.UntypedSqlRow{{2, 2, 1}, {2, 2, 1}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(xy,ab) */ y,a,z from xy join ab on y = a where y = 2",
				Expected: []sql.UntypedSqlRow{{2, 2, 1}, {2, 2, 1}},
			},
		},
	},
	{
		name: "keyed null lookup join indexes",
		setup: [][]string{
			setup.MydbData[0],
			{
				"CREATE table xy (x int, y int, z int primary key, index y_idx(y));",
				"CREATE table ab (a int, b int primary key, c int);",
				"insert into xy values (1,0,0), (1,null,1), (0,2,2),(0,2,3);",
				"insert into ab values (0,1,0), (1,2,1), (2,3,2), (null,4,3);",
			},
		},
		tests: []JoinOpTests{
			// non-covering tablescan
			{
				Query:    "select /*+ JOIN_ORDER(ab,xy) */ y,a,x from xy join ab on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0, 1}, {2, 2, 0}, {2, 2, 0}},
			},
			// covering
			{
				Query:    "select /*+ JOIN_ORDER(ab,xy) */ y,a,z from xy join ab on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0, 0}, {2, 2, 2}, {2, 2, 3}},
			},
		},
	},
	{
		name: "partial key null lookup join indexes",
		setup: [][]string{
			setup.MydbData[0],
			{
				"CREATE table xy (x int, y int, z int primary key, index y_idx(y,x));",
				"CREATE table ab (a int, b int primary key, c int);",
				"insert into xy values (1,0,0), (1,null,1), (0,2,2),(0,2,3);",
				"insert into ab values (0,1,0), (1,2,1), (2,3,2), (null,4,3);",
			},
		},
		tests: []JoinOpTests{
			// non-covering tablescan
			{
				Query:    "select /*+ JOIN_ORDER(ab,xy) */ y,a,x from xy join ab on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0, 1}, {2, 2, 0}, {2, 2, 0}},
			},
			// covering
			{
				Query:    "select /*+ JOIN_ORDER(ab,xy) */ y,a,z from xy join ab on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0, 0}, {2, 2, 2}, {2, 2, 3}},
			},
		},
	},
	{
		name: "keyed lookup join indexes",
		setup: [][]string{
			setup.MydbData[0],
			{
				"CREATE table xy (x int, y int, z int primary key, index y_idx(y));",
				"CREATE table ab (a int, b int primary key, c int);",
				"insert into xy values (1,0,0), (1,0,1), (0,2,2),(0,2,3);",
				"insert into ab values (0,1,0), (1,2,1), (2,3,2), (3,4,3);",
			},
		},
		tests: []JoinOpTests{
			// covering tablescan
			{
				Query:    "select /*+ JOIN_ORDER(ab,xy) */ y,a from xy join ab on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0}, {0, 0}, {2, 2}, {2, 2}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(xy,ab) */ y,a from xy join ab on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0}, {0, 0}, {2, 2}, {2, 2}},
			},
			// covering indexed source
			{
				Query:    "select /*+ JOIN_ORDER(ab,xy) */ y,a from xy join ab on y = a where y = 2",
				Expected: []sql.UntypedSqlRow{{2, 2}, {2, 2}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(xy,ab) */ y,a from xy join ab on y = a where y = 2",
				Expected: []sql.UntypedSqlRow{{2, 2}, {2, 2}},
			},
			// non-covering tablescan
			{
				Query:    "select /*+ JOIN_ORDER(ab,xy) */ y,a,x from xy join ab on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0, 1}, {0, 0, 1}, {2, 2, 0}, {2, 2, 0}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(xy,ab) */ y,a,x from xy join ab on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0, 1}, {0, 0, 1}, {2, 2, 0}, {2, 2, 0}},
			},
			// non-covering indexed source
			{
				Query:    "select /*+ JOIN_ORDER(ab,xy) */ y,a,x from xy join ab on y = a where y = 2",
				Expected: []sql.UntypedSqlRow{{2, 2, 0}, {2, 2, 0}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(xy,ab) */ y,a,x from xy join ab on y = a where y = 2",
				Expected: []sql.UntypedSqlRow{{2, 2, 0}, {2, 2, 0}},
			},
		},
	},
	{
		name: "multi pk lax lookup join",
		setup: [][]string{
			setup.MydbData[0],
			{
				"CREATE table wxyz (w int, x int, y int, z int, primary key (x,w), index yw_idx(y,w));",
				"CREATE table abcd (a int, b int, c int, d int, primary key (a,b), index ca_idx(c,a));",
				"insert into wxyz values (1,0,0,0), (1,1,1,1), (0,2,2,1),(0,1,3,1);",
				"insert into abcd values (0,0,0,0), (0,1,1,1), (0,2,2,1),(2,1,3,1);",
			},
		},
		tests: []JoinOpTests{
			{
				Query:    "select /*+ JOIN_ORDER(abcd,wxyz) */ y,a,z from wxyz join abcd on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {2, 2, 1}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(abcd,wxyz) */ y,a,w from wxyz join abcd on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0, 1}, {0, 0, 1}, {0, 0, 1}, {2, 2, 0}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(wxyz,abcd) */ y,a,d from wxyz join abcd on y = c",
				Expected: []sql.UntypedSqlRow{{0, 0, 0}, {1, 0, 1}, {2, 0, 1}, {3, 2, 1}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(abcd,wxyz) */ y,a,d from wxyz join abcd on y = c",
				Expected: []sql.UntypedSqlRow{{0, 0, 0}, {1, 0, 1}, {2, 0, 1}, {3, 2, 1}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(wxyz,abcd) */ y,a,d from wxyz join abcd on y = c and w = c",
				Expected: []sql.UntypedSqlRow{{1, 0, 1}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(abcd,wxyz) */ y,a,d from wxyz join abcd on y = c and w = c",
				Expected: []sql.UntypedSqlRow{{1, 0, 1}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(wxyz,abcd) */ y,a,d from wxyz join abcd on y = c and w = a",
				Expected: []sql.UntypedSqlRow{{2, 0, 1}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(wxyz,abcd) */ y,a,d from wxyz join abcd on w = c and w = a",
				Expected: []sql.UntypedSqlRow{{3, 0, 0}, {2, 0, 0}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(wxyz,abcd) */ y,a,d from wxyz join abcd on y = c and y = a",
				Expected: []sql.UntypedSqlRow{{0, 0, 0}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(wxyz,abcd) */ y,a,d from wxyz join abcd on y = a and  c = 0",
				Expected: []sql.UntypedSqlRow{{0, 0, 0}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(abcd,wxyz) */ y,a,d from wxyz join abcd on y = a and  c = 0",
				Expected: []sql.UntypedSqlRow{{0, 0, 0}},
			},
		},
	},
	{
		name: "multi pk strict lookup join",
		setup: [][]string{
			setup.MydbData[0],
			{
				"CREATE table wxyz (w int not null, x int, y int not null, z int, primary key (x,w), unique index yw_idx(y,w));",
				"CREATE table abcd (a int not null, b int, c int not null, d int, primary key (a,b), unique index ca_idx(c,a));",
				"insert into wxyz values (1,0,0,0), (1,1,1,1), (0,2,2,1),(0,1,3,1);",
				"insert into abcd values (0,0,0,0), (0,1,1,1), (0,2,2,1),(2,1,3,1);",
			},
		},
		tests: []JoinOpTests{
			{
				Query:    "select /*+ JOIN_ORDER(abcd,wxyz) */ y,a,z from wxyz join abcd on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {2, 2, 1}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(abcd,wxyz) */ y,a,w from wxyz join abcd on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0, 1}, {0, 0, 1}, {0, 0, 1}, {2, 2, 0}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(abcd,wxyz) */ y,a,d from wxyz join abcd on y = c",
				Expected: []sql.UntypedSqlRow{{0, 0, 0}, {1, 0, 1}, {2, 0, 1}, {3, 2, 1}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(wxyz,abcd) */ y,a,d from wxyz join abcd on y = c",
				Expected: []sql.UntypedSqlRow{{0, 0, 0}, {1, 0, 1}, {2, 0, 1}, {3, 2, 1}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(abcd,wxyz) */ y,a,d from wxyz join abcd on y = c and w = c",
				Expected: []sql.UntypedSqlRow{{1, 0, 1}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(wxyz,abcd) */ y,a,d from wxyz join abcd on y = c and w = a",
				Expected: []sql.UntypedSqlRow{{2, 0, 1}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(wxyz,abcd) */ y,a,d from wxyz join abcd on w = c and w = a",
				Expected: []sql.UntypedSqlRow{{3, 0, 0}, {2, 0, 0}},
			},
			{
				Query:    "select /*+ JOIN_ORDER(wxyz,abcd) */ y,a,d from wxyz join abcd on y = c and y = a",
				Expected: []sql.UntypedSqlRow{{0, 0, 0}},
			},
		},
	},
	{
		name: "redundant keyless index",
		setup: [][]string{
			setup.MydbData[0],
			{
				"CREATE table xy (x int, y int, z int, index y_idx(x,y,z));",
				"CREATE table ab (a int, b int primary key, c int);",
				"insert into xy values (1,0,0), (1,0,1), (0,2,2),(0,2,3);",
				"insert into ab values (0,1,0), (1,2,1), (2,3,2), (3,4,3);",
			},
		},
		tests: []JoinOpTests{
			{
				Query:    "select /*+ JOIN_ORDER(ab,xy) */ y,a,z from xy join ab on y = a",
				Expected: []sql.UntypedSqlRow{{0, 0, 1}, {0, 0, 0}, {2, 2, 3}, {2, 2, 2}},
			},
		},
	},
	{
		name: "empty join tests",
		setup: [][]string{
			setup.MydbData[0],
			{
				"CREATE table xy (x int primary key, y int);",
				"CREATE table uv (u int primary key, v int);",
				"insert into xy values (1,0), (2,1), (0,2), (3,3);",
				"insert into uv values (0,1), (1,1), (2,2), (3,2);",
			},
		},
		tests: []JoinOpTests{
			{
				Query:    "select * from xy where y-1 = (select u from uv where u = 4);",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select * from xy where x = 1 and x != (select u from uv where u = 4);",
				Expected: []sql.UntypedSqlRow{{1, 0}},
			},
			{
				Query:    "select * from xy where x = 1 and x not in (select u from uv where u = 4);",
				Expected: []sql.UntypedSqlRow{{1, 0}},
			},
			{
				Query:    "select * from xy where x = 1 and not exists (select u from uv where u = 4);",
				Expected: []sql.UntypedSqlRow{{1, 0}},
			},
		},
	},
	{
		name: "issue 5633, nil comparison in merge join",
		setup: [][]string{
			setup.MydbData[0],
			{
				"create table xyz (x int primary key, y int, z int, key(y), key(z))",
				"create table uv (u int primary key, v int, unique key(u,v))",
				"insert into xyz values (0,0,0),(1,1,1),(2,1,null),(3,2,null)",
				"insert into uv values (0,0),(1,1),(2,null),(3,null)",
			},
		},
		tests: []JoinOpTests{
			{
				Query:    "select x,u,z from xyz join uv on z = u where y = 1 order by 1,2",
				Expected: []sql.UntypedSqlRow{{1, 1, 1}},
			},
		},
	},
	{
		name: "issue 5633 2, nil comparison in merge join",
		setup: [][]string{
			setup.MydbData[0],
			{
				"create table xyz (x int primary key, y int, z int, key(y), key(z))",
				"create table uv (u int primary key, v int, unique key(u,v))",
				"insert into xyz values (1,1,3),(2,1,2),(3,1,1)",
				"insert into uv values (1,1),(2,2),(3,3)",
			},
		},
		tests: []JoinOpTests{
			{
				Query:    "select x,u from xyz join uv on z = u where y = 1 order by 1,2",
				Expected: []sql.UntypedSqlRow{{1, 3}, {2, 2}, {3, 1}},
			},
		},
	},
	{
		name: "left join tests",
		setup: [][]string{
			{
				"create table xy (x int primary key, y int)",
				"create table uv (u int primary key, v int, key(v))",
				"insert into xy values (0,0),(2,2),(3,3),(4,4),(5,5),(7,7),(8,8),(10,10);",
				"insert into uv values (0,0),(1,1),(3,3),(5,5),(6,5),(7,7),(9,9),(10,10);",
			},
		},
		tests: []JoinOpTests{
			{
				Query:    "select x from xy left join uv on x = v",
				Expected: []sql.UntypedSqlRow{{0}, {2}, {3}, {4}, {5}, {5}, {7}, {8}, {10}},
			},
		},
	},
	{
		name: "left join on array data",
		setup: [][]string{
			{
				"create table xy (x binary(2) primary key, y binary(2))",
				"create table uv (u binary(2) primary key, v binary(2))",
				"insert into xy values (x'F0F0',x'1234'),(x'2345',x'3456');",
				"insert into uv values (x'fedc',x'F0F0');",
			},
		},
		tests: []JoinOpTests{
			{
				Query: "select HEX(x),HEX(u) from xy left join uv on x = v OR y = u",
				Expected: []sql.UntypedSqlRow{
					{"2345", nil},
					{"F0F0", "FEDC"},
				},
			},
		},
	},
	{
		name: "point lookups",
		setup: [][]string{
			setup.MydbData[0],
			{
				"create table uv (u int primary key, v int, unique key(v));",
				"insert into uv values (1,1),(2,2);",
				"create table xy (x int primary key, v int);",
				"insert into xy values (0,0),(1,1);",
			},
		},
		tests: []JoinOpTests{
			{
				Query:    "select * from xy where x not in (select v from uv)",
				Expected: []sql.UntypedSqlRow{{0, 0}},
			},
		},
	},
	{
		name: "ordered distinct",
		setup: [][]string{
			setup.MydbData[0],
			{
				"create table uv (u int primary key, v int);",
				"insert into uv values (1,1),(2,2),(3,1),(4,2);",
				"create table xy (x int primary key, y int);",
				"insert into xy values (1,1),(2,2);",
			},
		},
		tests: []JoinOpTests{
			{
				Query:    `select /*+ JOIN_ORDER(scalarSubq0,xy) */ count(*) from xy where y in (select distinct v from uv);`,
				Expected: []sql.UntypedSqlRow{{2}},
			},
			{
				Query:    `SELECT /*+ JOIN_ORDER(scalarSubq0,xy) */ count(*) from xy where y in (select distinct u from uv);`,
				Expected: []sql.UntypedSqlRow{{2}},
			},
		},
	},
	{
		name: "union/intersect/except joins",
		setup: [][]string{
			setup.MydbData[0],
			{
				"create table uv (u int primary key, v int);",
				"insert into uv values (1,1),(2,2),(3,1),(4,2);",
				"create table xy (x int primary key, y int);",
				"insert into xy values (1,1),(2,2);",
			},
		},
		tests: []JoinOpTests{
			{
				Query:    "select * from xy where x = 1 and exists (select 1 union select 1)",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			{
				Query:    "select * from xy where x = 1 and x in (select y from xy union select 1)",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			{
				Query:    "select * from xy where x = 1 and x in (select y from xy intersect select 1)",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			{
				Query:    "select * from xy where x = 1 and x in (select y from xy except select 2)",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			{
				Query: "select * from xy where x = 1 intersect select * from uv;",
				Expected: []sql.UntypedSqlRow{
					{1, 1},
				},
			},
			{
				Query: "select * from uv where u < 4 except select * from xy;",
				Expected: []sql.UntypedSqlRow{
					{3, 1},
				},
			},
			{
				Query: "select * from xy, uv where x = u intersect select * from xy, uv where x = u order by x, y, u, v;",
				Expected: []sql.UntypedSqlRow{
					{1, 1, 1, 1},
					{2, 2, 2, 2},
				},
			},
			{
				Query: "select * from xy, uv where x != u except select * from xy, uv where y != v order by x, y, u, v;",
				Expected: []sql.UntypedSqlRow{
					{1, 1, 3, 1},
					{2, 2, 4, 2},
				},
			},
			{
				Query: "select * from (select * from uv where u < 4 except select * from xy) a, (select * from xy intersect select * from uv) b order by u, v, x, y;",
				Expected: []sql.UntypedSqlRow{
					{3, 1, 1, 1},
					{3, 1, 2, 2},
				},
			},
		},
	},
	{
		name: "4-way join tests",
		setup: [][]string{
			setup.MydbData[0],
			setup.MytableData[0],
			setup.OthertableData[0],
			setup.Pk_tablesData[0],
			setup.NiltableData[0],
			setup.TabletestData[0],
			setup.XyData[0],
		},
		tests: []JoinOpTests{
			{
				Query:    `SELECT * from xy join uv on x = u and y = NOW()`,
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: `SELECT xy.x, xy.y
					FROM xy
					WHERE EXISTS (
					SELECT 1 FROM uv WHERE xy.x = uv.v AND (EXISTS (
					SELECT 1 FROM ab WHERE uv.u = ab.b)))`,
				Expected: []sql.UntypedSqlRow{{1, 0}, {2, 1}},
			},
			{
				// natural join w/ inner join
				Query: "select * from mytable t1 natural join mytable t2 join othertable t3 on t2.i = t3.i2;",
				Expected: []sql.UntypedSqlRow{
					{1, "first row", "third", 1},
					{2, "second row", "second", 2},
					{3, "third row", "first", 3},
				},
			},
			{
				Query: `
SELECT SUM(x) FROM xy WHERE x IN (
  SELECT u FROM uv WHERE u IN (
    SELECT a FROM ab WHERE a = 2
    )
  ) AND
  x = 2;`,
				Expected: []sql.UntypedSqlRow{{float64(2)}},
			},
			{
				Query:    "select * from ab left join uv on a = u where exists (select * from uv where false)",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "select * from ab left join (select * from uv where false) s on a = u order by 1;",
				Expected: []sql.UntypedSqlRow{
					{0, 2, nil, nil},
					{1, 2, nil, nil},
					{2, 2, nil, nil},
					{3, 1, nil, nil},
				},
			},
			{
				Query:    "select * from ab right join (select * from uv where false) s on a = u order by 1;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "select * from mytable where exists (select * from mytable where i = 1) order by 1;",
				Expected: []sql.UntypedSqlRow{
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},
			// queries that test subquery hoisting
			{
				// case 1: condition uses columns from both sides
				Query: "/*+case1*/ select * from ab where exists (select * from xy where ab.a = xy.x + 3)",
				Expected: []sql.UntypedSqlRow{
					{3, 1},
				},
			},
			{
				// case 1N: NOT EXISTS condition uses columns from both sides
				Query: "/*+case1N*/ select * from ab where not exists (select * from xy where ab.a = xy.x + 3)",
				Expected: []sql.UntypedSqlRow{
					{0, 2},
					{1, 2},
					{2, 2},
				},
			},
			{
				// case 2: condition uses columns from left side only
				Query:    "/*+case2*/ select * from ab where exists (select * from xy where a = 1)",
				Expected: []sql.UntypedSqlRow{{1, 2}},
			},
			{
				// case 2N: NOT EXISTS condition uses columns from left side only
				Query: "/*+case2N*/ select * from ab where not exists (select * from xy where a = 1)",
				Expected: []sql.UntypedSqlRow{
					{0, 2},
					{2, 2},
					{3, 1},
				},
			},
			{
				// case 3: condition uses columns from right side only
				Query: "/*+case3*/ select * from ab where exists (select * from xy where 1 = xy.x)",
				Expected: []sql.UntypedSqlRow{
					{0, 2},
					{1, 2},
					{2, 2},
					{3, 1},
				},
			},
			{
				// case 3N: NOT EXISTS condition uses columns from right side only
				Query: "/*+case3N*/ select * from ab where not exists (select * from xy where 10 = xy.x)",
				Expected: []sql.UntypedSqlRow{
					{0, 2},
					{1, 2},
					{2, 2},
					{3, 1},
				},
			},
			{
				// case 4a: condition uses no columns from either side, and condition is true
				Query: "/*+case4a*/ select * from ab where exists (select * from xy where 1 = 1)",
				Expected: []sql.UntypedSqlRow{
					{0, 2},
					{1, 2},
					{2, 2},
					{3, 1},
				},
			},
			{
				// case 4aN: NOT EXISTS condition uses no columns from either side, and condition is true
				Query:    "/*+case4aN*/ select * from ab where not exists (select * from xy where 1 = 1)",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				// case 4b: condition uses no columns from either side, and condition is false
				Query:    "/*+case4b*/ select * from ab where exists (select * from xy where 1 = 0)",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				// case 4bN: NOT EXISTS condition uses no columns from either side, and condition is false
				Query:    "/*+case4bN*/ select * from ab where not exists (select * from xy where 1 = 0)",
				Expected: []sql.UntypedSqlRow{{0, 2}, {1, 2}, {2, 2}, {3, 1}},
			},
			{
				// test more complex scopes
				Query: "select x, 1 in (select a from ab where exists (select * from uv where a = u)) s from xy",
				Expected: []sql.UntypedSqlRow{
					{0, true},
					{1, true},
					{2, true},
					{3, true},
				},
			},
			{
				Query:    `select a.i,a.f, b.i2 from niltable a left join niltable b on a.i = b.i2`,
				Expected: []sql.UntypedSqlRow{{1, nil, nil}, {2, nil, 2}, {3, nil, nil}, {4, 4.0, 4}, {5, 5.0, nil}, {6, 6.0, 6}},
			},
			{
				Query: `SELECT i, s, i2, s2 FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 <=> s)`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row", 1, "third"},
					{2, "second row", 2, "second"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query: `SELECT i, s, i2, s2 FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 = s)`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row", 1, "third"},
					{2, "second row", 2, "second"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query: `SELECT i, s, i2, s2 FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND CONCAT(s, s2) IS NOT NULL`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row", 1, "third"},
					{2, "second row", 2, "second"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query: `SELECT * FROM mytable mt JOIN othertable ot ON ot.i2 = (SELECT i2 FROM othertable WHERE s2 = "second") AND mt.i = ot.i2 JOIN mytable mt2 ON mt.i = mt2.i`,
				Expected: []sql.UntypedSqlRow{
					{2, "second row", "second", 2, 2, "second row"},
				},
			},
			{
				Query:    "SELECT l.i, r.i2 FROM niltable l INNER JOIN niltable r ON l.i2 = r.i2 ORDER BY 1",
				Expected: []sql.UntypedSqlRow{{2, 2}, {4, 4}, {6, 6}},
			},
			{
				Query:    "SELECT l.i, r.i2 FROM niltable l INNER JOIN niltable r ON l.i2 != r.i2 ORDER BY 1, 2",
				Expected: []sql.UntypedSqlRow{{2, 4}, {2, 6}, {4, 2}, {4, 6}, {6, 2}, {6, 4}},
			},
			{
				Query:    "SELECT l.i, r.i2 FROM niltable l INNER JOIN niltable r ON l.i2 <=> r.i2 ORDER BY 1 ASC",
				Expected: []sql.UntypedSqlRow{{1, nil}, {1, nil}, {1, nil}, {2, 2}, {3, nil}, {3, nil}, {3, nil}, {4, 4}, {5, nil}, {5, nil}, {5, nil}, {6, 6}},
			},
			{
				// TODO: ORDER BY should apply to the union. The parser is wrong.
				Query: `SELECT s2, i2, i
			FROM (SELECT * FROM mytable) mytable
			RIGHT JOIN
				((SELECT i2, s2 FROM othertable ORDER BY i2 ASC)
				 UNION ALL
				 SELECT CAST(4 AS SIGNED) AS i2, "not found" AS s2 FROM DUAL) othertable
			ON i2 = i`,
				Expected: []sql.UntypedSqlRow{
					{"third", 1, 1},
					{"second", 2, 2},
					{"first", 3, 3},
					{"not found", 4, nil},
				},
			},
			// re: https://github.com/dolthub/go-mysql-server/pull/2292
			{
				Query: `SELECT
			"testing" AS s,
			(SELECT max(i)
			FROM (SELECT * FROM mytable) mytable
			RIGHT JOIN
				((SELECT i2, s2 FROM othertable ORDER BY i2 ASC)
				 UNION ALL
				 SELECT CAST(4 AS SIGNED) AS i2, "not found" AS s2 FROM DUAL) othertable
				ON i2 = i) AS rj
			FROM DUAL`,
				Expected: []sql.UntypedSqlRow{
					{"testing", 3},
				},
			},
			{
				Query: `SELECT
			"testing" AS s,
			(SELECT max(i2)
			FROM (SELECT * FROM mytable) mytable
			RIGHT JOIN
				((SELECT i2, s2 FROM othertable ORDER BY i2 ASC)
				 UNION ALL
				 SELECT CAST(4 AS SIGNED) AS i2, "not found" AS s2 FROM DUAL) othertable
				ON i2 = i) AS rj
			FROM DUAL`,
				Expected: []sql.UntypedSqlRow{
					{"testing", 4},
				},
			},
			{
				Query: "SELECT substring(mytable.s, 1, 5) AS s FROM mytable INNER JOIN othertable ON (substring(mytable.s, 1, 5) = SUBSTRING(othertable.s2, 1, 5)) GROUP BY 1",
				Expected: []sql.UntypedSqlRow{
					{"third"},
					{"secon"},
					{"first"},
				},
			},
			{
				Query: "SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.UntypedSqlRow{
					{2},
				},
			},
			{
				Query: "SELECT /*+ JOIN_ORDER(t1,t2) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.UntypedSqlRow{
					{2},
				},
			},
			{
				Query: "SELECT /*+ JOIN_ORDER(t2,t1) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.UntypedSqlRow{
					{2},
				},
			},
			{
				Query: "SELECT /*+ JOIN_ORDER(t1) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.UntypedSqlRow{
					{2},
				},
			},
			{
				Query: "SELECT /*+ JOIN_ORDER(t1, mytable) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.UntypedSqlRow{
					{2},
				},
			},
			{
				Query: "SELECT /*+ JOIN_ORDER(t1, not_exist) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.UntypedSqlRow{
					{2},
				},
			},
			{
				Query: "SELECT /*+ NOTHING(abc) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.UntypedSqlRow{
					{2},
				},
			},
			{
				Query: "SELECT /*+ JOIN_ORDER( */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.UntypedSqlRow{
					{2},
				},
			},
			{
				Query: "select mytable.i as i2, othertable.i2 as i from mytable join othertable on i = i2 order by 1",
				Expected: []sql.UntypedSqlRow{
					{1, 1},
					{2, 2},
					{3, 3},
				},
			},
			{
				Query: "SELECT i, s, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR s = s2 order by 1",
				Expected: []sql.UntypedSqlRow{
					{1, "first row", 1, "third"},
					{2, "second row", 2, "second"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query: "SELECT i, s, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 order by 1, 3",
				Expected: []sql.UntypedSqlRow{
					{1, "first row", 1, "third"},
					{1, "first row", 3, "first"},
					{2, "second row", 2, "second"},
					{3, "third row", 1, "third"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query: "SELECT i, s, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 OR SUBSTRING_INDEX(s, ' ', 2) = s2 order by 1, 3",
				Expected: []sql.UntypedSqlRow{
					{1, "first row", 1, "third"},
					{1, "first row", 3, "first"},
					{2, "second row", 2, "second"},
					{3, "third row", 1, "third"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query: "SELECT i, s, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 2) = s2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 order by 1, 3",
				Expected: []sql.UntypedSqlRow{
					{1, "first row", 1, "third"},
					{1, "first row", 3, "first"},
					{2, "second row", 2, "second"},
					{3, "third row", 1, "third"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query: "SELECT i, s, i2, s2 FROM mytable INNER JOIN othertable ON SUBSTRING_INDEX(s, ' ', 2) = s2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 OR i = i2 order by 1, 3",
				Expected: []sql.UntypedSqlRow{
					{1, "first row", 1, "third"},
					{1, "first row", 3, "first"},
					{2, "second row", 2, "second"},
					{3, "third row", 1, "third"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query:    "SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 3",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i",
				Expected: []sql.UntypedSqlRow{
					{int64(1), int64(1), "third"},
					{int64(2), int64(2), "second"},
					{int64(3), int64(3), "first"},
				},
			},
			{
				Query: "SELECT i, i2, s2 FROM mytable as OTHERTABLE INNER JOIN othertable as MYTABLE ON i = i2 ORDER BY i",
				Expected: []sql.UntypedSqlRow{
					{int64(1), int64(1), "third"},
					{int64(2), int64(2), "second"},
					{int64(3), int64(3), "first"},
				},
			},
			{
				Query: "SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i",
				Expected: []sql.UntypedSqlRow{
					{"third", int64(1), int64(1)},
					{"second", int64(2), int64(2)},
					{"first", int64(3), int64(3)},
				},
			},
			{
				Query: "SELECT i, i2, s2 FROM othertable JOIN mytable  ON i = i2 ORDER BY i",
				Expected: []sql.UntypedSqlRow{
					{int64(1), int64(1), "third"},
					{int64(2), int64(2), "second"},
					{int64(3), int64(3), "first"},
				},
			},
			{
				Query: "SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2 ORDER BY i",
				Expected: []sql.UntypedSqlRow{
					{"third", int64(1), int64(1)},
					{"second", int64(2), int64(2)},
					{"first", int64(3), int64(3)},
				},
			},
			{
				Query: "SELECT s FROM mytable INNER JOIN othertable " +
					"ON substring(s2, 1, 2) != '' AND i = i2 ORDER BY 1",
				Expected: []sql.UntypedSqlRow{
					{"first row"},
					{"second row"},
					{"third row"},
				},
			},
			{
				Query: `SELECT i FROM mytable NATURAL JOIN tabletest`,
				Expected: []sql.UntypedSqlRow{
					{int64(1)},
					{int64(2)},
					{int64(3)},
				},
			},
			{
				Query: `SELECT i FROM mytable AS t NATURAL JOIN tabletest AS test`,
				Expected: []sql.UntypedSqlRow{
					{int64(1)},
					{int64(2)},
					{int64(3)},
				},
			},
			{
				Query: `SELECT t.i, test.s FROM mytable AS t NATURAL JOIN tabletest AS test`,
				Expected: []sql.UntypedSqlRow{
					{int64(1), "first row"},
					{int64(2), "second row"},
					{int64(3), "third row"},
				},
			},
			{
				Query: `SELECT * FROM tabletest, mytable mt INNER JOIN othertable ot ON mt.i = ot.i2`,
				Expected: []sql.UntypedSqlRow{
					{int64(1), "first row", int64(1), "first row", "third", int64(1)},
					{int64(1), "first row", int64(2), "second row", "second", int64(2)},
					{int64(1), "first row", int64(3), "third row", "first", int64(3)},
					{int64(2), "second row", int64(1), "first row", "third", int64(1)},
					{int64(2), "second row", int64(2), "second row", "second", int64(2)},
					{int64(2), "second row", int64(3), "third row", "first", int64(3)},
					{int64(3), "third row", int64(1), "first row", "third", int64(1)},
					{int64(3), "third row", int64(2), "second row", "second", int64(2)},
					{int64(3), "third row", int64(3), "third row", "first", int64(3)},
				},
			},
			{
				Query: `SELECT * FROM tabletest join mytable mt INNER JOIN othertable ot ON tabletest.i = ot.i2 order by 1,3,6`,
				Expected: []sql.UntypedSqlRow{
					{int64(1), "first row", int64(1), "first row", "third", int64(1)},
					{int64(1), "first row", int64(2), "second row", "third", int64(1)},
					{int64(1), "first row", int64(3), "third row", "third", int64(1)},
					{int64(2), "second row", int64(1), "first row", "second", int64(2)},
					{int64(2), "second row", int64(2), "second row", "second", int64(2)},
					{int64(2), "second row", int64(3), "third row", "second", int64(2)},
					{int64(3), "third row", int64(1), "first row", "first", int64(3)},
					{int64(3), "third row", int64(2), "second row", "first", int64(3)},
					{int64(3), "third row", int64(3), "third row", "first", int64(3)},
				},
			},
			{
				Query: `SELECT * FROM mytable mt INNER JOIN othertable ot ON mt.i = ot.i2 AND mt.i > 2`,
				Expected: []sql.UntypedSqlRow{
					{int64(3), "third row", "first", int64(3)},
				},
			},
			{
				Query: `SELECT * FROM othertable ot INNER JOIN mytable mt ON mt.i = ot.i2 AND mt.i > 2`,
				Expected: []sql.UntypedSqlRow{
					{"first", int64(3), int64(3), "third row"},
				},
			},
			{
				Query: "SELECT i, i2, s2 FROM mytable LEFT JOIN othertable ON i = i2 - 1",
				Expected: []sql.UntypedSqlRow{
					{int64(1), int64(2), "second"},
					{int64(2), int64(3), "first"},
					{int64(3), nil, nil},
				},
			},
			{
				Query: "SELECT i, i2, s2 FROM mytable RIGHT JOIN othertable ON i = i2 - 1",
				Expected: []sql.UntypedSqlRow{
					{nil, int64(1), "third"},
					{int64(1), int64(2), "second"},
					{int64(2), int64(3), "first"},
				},
			},
			{
				Query: "SELECT i, i2, s2 FROM mytable LEFT OUTER JOIN othertable ON i = i2 - 1",
				Expected: []sql.UntypedSqlRow{
					{int64(1), int64(2), "second"},
					{int64(2), int64(3), "first"},
					{int64(3), nil, nil},
				},
			},
			{
				Query: "SELECT i, i2, s2 FROM mytable RIGHT OUTER JOIN othertable ON i = i2 - 1",
				Expected: []sql.UntypedSqlRow{
					{nil, int64(1), "third"},
					{int64(1), int64(2), "second"},
					{int64(2), int64(3), "first"},
				},
			},
			{
				Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2
				FROM othertable ot INNER JOIN
					(SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub
				ON sub.i = ot.i2 order by 1`,
				Expected: []sql.UntypedSqlRow{
					{1, 1, "third", 1, "third"},
					{2, 2, "second", 2, "second"},
					{3, 3, "first", 3, "first"},
				},
			},
			{
				Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2
				FROM (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub
				INNER JOIN othertable ot
				ON sub.i = ot.i2 order by 1`,
				Expected: []sql.UntypedSqlRow{
					{1, 1, "third", 1, "third"},
					{2, 2, "second", 2, "second"},
					{3, 3, "first", 3, "first"},
				},
			},
			{
				Query: "SELECT one_pk.c5,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{4, 0, 0},
					{4, 0, 1},
					{14, 1, 0},
					{14, 1, 1},
				},
			},
			{
				Query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{4, 0, 0},
					{4, 0, 1},
					{14, 1, 0},
					{14, 1, 1},
				},
			},
			{
				Query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{4, 0, 0},
					{4, 0, 1},
					{14, 1, 0},
					{14, 1, 1},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE pk=1 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{1, 0, 1},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0},
					{1, 1, 1},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0},
					{1, 1, 1},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=tpk.pk1 AND pk=tpk.pk2 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0},
					{1, 1, 1},
				},
			},
			{
				Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk-1=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1
						ORDER BY 1`,
				Expected: []sql.UntypedSqlRow{
					{0, nil, nil, nil, nil},
					{1, 1, 0, 0, 1},
					{2, nil, nil, nil, nil},
					{3, nil, nil, nil, nil},
				},
			},
			{
				Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk
						JOIN two_pk tpk ON pk=tpk.pk1 AND pk-1=tpk.pk2
						JOIN two_pk tpk2 ON pk-1=TPK2.pk1 AND pk=tpk2.pk2
						ORDER BY 1`,
				Expected: []sql.UntypedSqlRow{
					{1, 1, 0, 0, 1},
				},
			},
			{
				Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk
						JOIN two_pk tpk ON pk=tpk.pk1 AND pk-1=tpk.pk2
						JOIN two_pk tpk2 ON pk-1=TPK2.pk1 AND pk=tpk2.pk2
						ORDER BY 1`,
				Expected: []sql.UntypedSqlRow{
					{1, 1, 0, 0, 1},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0},
					{1, 1, 1},
					{2, nil, nil},
					{3, nil, nil},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{nil, 0, 1},
					{nil, 1, 0},
					{0, 0, 0},
					{1, 1, 1},
				},
			},
			{
				Query: "SELECT i,pk1,pk2 FROM mytable JOIN two_pk ON i-1=pk1 AND i-2=pk2 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{int64(2), 1, 0},
				},
			},
			{
				Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0, 0},
					{0, 1, 1, 0},
					{1, 0, 0, 1},
					{1, 1, 1, 1},
				},
			},
			{
				Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0, 0},
					{0, 1, 0, 1},
					{1, 0, 1, 0},
					{1, 1, 1, 1},
				},
			},
			{
				Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0, 0},
					{0, 1, 0, 1},
					{1, 0, 1, 0},
					{1, 1, 1, 1},
				},
			},
			{
				Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON b.pk1=a.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0, 0},
					{0, 1, 0, 1},
					{1, 0, 1, 0},
					{1, 1, 1, 1},
				},
			},
			{
				Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1+1=b.pk1 AND a.pk2+1=b.pk2 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{0, 0, 1, 1},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0},
					{0, 0, 1},
					{1, 1, 0},
					{1, 1, 1},
					{2, nil, nil},
					{3, nil, nil},
				},
			},
			{
				Query: "SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i2 ORDER BY 1",
				Expected: []sql.UntypedSqlRow{
					{0, nil, nil},
					{1, nil, nil},
					{2, int64(2), nil},
					{3, nil, nil},
				},
			},
			{
				Query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i2 ORDER BY 2,3",
				Expected: []sql.UntypedSqlRow{
					{nil, nil, nil},
					{nil, nil, nil},
					{nil, nil, 5.0},
					{2, int64(2), nil},
					{nil, int64(4), 4.0},
					{nil, int64(6), 6.0},
				},
			},
			{
				Query: "SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i2 AND f IS NOT NULL ORDER BY 1", // AND clause causes right table join miss
				Expected: []sql.UntypedSqlRow{
					{0, nil, nil},
					{1, nil, nil},
					{2, nil, nil},
					{3, nil, nil},
				},
			},
			{
				Query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i2 and pk > 0 ORDER BY 2,3", // > 0 clause in join condition is ignored
				Expected: []sql.UntypedSqlRow{
					{nil, nil, nil},
					{nil, nil, nil},
					{nil, nil, 5.0},
					{2, int64(2), nil},
					{nil, int64(4), 4.0},
					{nil, int64(6), 6.0},
				},
			},
			{
				Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NULL AND pk < 2 ORDER BY 1",
				Expected: []sql.UntypedSqlRow{
					{0, nil, nil},
					{1, 1, nil},
				},
			},
			{
				Query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3",
				Expected: []sql.UntypedSqlRow{
					{nil, nil, 5.0},
					{nil, int64(4), 4.0},
					{nil, int64(6), 6.0},
				},
			},
			{
				Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1 ORDER BY 1",
				Expected: []sql.UntypedSqlRow{
					{2, 2, nil},
					{3, 3, nil},
				},
			},
			{
				Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE c1 > 10 ORDER BY 1",
				Expected: []sql.UntypedSqlRow{
					{2, 2, nil},
					{3, 3, nil},
				},
			},
			{
				Query: "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3",
				Expected: []sql.UntypedSqlRow{
					{nil, 4, 4.0},
					{nil, 5, 5.0},
					{nil, 6, 6.0},
				},
			},
			{
				Query: "SELECT t1.i,t1.i2 FROM niltable t1 LEFT JOIN niltable t2 ON t1.i=t2.i2 WHERE t2.f IS NULL ORDER BY 1,2",
				Expected: []sql.UntypedSqlRow{
					{1, nil},
					{2, 2},
					{3, nil},
					{5, nil},
				},
			},
			{
				Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i2 > 1 ORDER BY 1",
				Expected: []sql.UntypedSqlRow{
					{2, 2, nil},
				},
			},
			{
				Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i > 1 ORDER BY 1",
				Expected: []sql.UntypedSqlRow{
					{2, 2, nil},
					{3, 3, nil},
				},
			},
			{
				Query: "SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i2 IS NOT NULL ORDER BY 1",
				Expected: []sql.UntypedSqlRow{
					{2, int64(2), nil},
				},
			},
			{
				Query: "SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i2 WHERE pk > 1 ORDER BY 1",
				Expected: []sql.UntypedSqlRow{
					{2, int64(2), nil},
					{3, nil, nil},
				},
			},
			{
				Query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i2 WHERE pk > 0 ORDER BY 2,3",
				Expected: []sql.UntypedSqlRow{
					{2, int64(2), nil},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2,one_pk.c1 AS foo, two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0, 0, 0},
					{1, 0, 1, 10, 10},
					{2, 1, 0, 20, 20},
					{3, 1, 1, 30, 30},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2,one_pk.c1 AS foo,two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE one_pk.c1=10",
				Expected: []sql.UntypedSqlRow{
					{1, 0, 1, 10, 10},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk1-pk>0 AND pk2<1",
				Expected: []sql.UntypedSqlRow{
					{0, 1, 0},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ORDER BY 1,2,3",
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0},
					{0, 0, 1},
					{0, 1, 0},
					{0, 1, 1},
					{1, 0, 0},
					{1, 0, 1},
					{1, 1, 0},
					{1, 1, 1},
					{2, 0, 0},
					{2, 0, 1},
					{2, 1, 0},
					{2, 1, 1},
					{3, 0, 0},
					{3, 0, 1},
					{3, 1, 0},
					{3, 1, 1},
				},
			},
			{
				Query: "SELECT a.pk,b.pk FROM one_pk a JOIN one_pk b ON a.pk = b.pk order by a.pk",
				Expected: []sql.UntypedSqlRow{
					{0, 0},
					{1, 1},
					{2, 2},
					{3, 3},
				},
			},
			{
				Query: "SELECT a.pk,b.pk FROM one_pk a, one_pk b WHERE a.pk = b.pk order by a.pk",
				Expected: []sql.UntypedSqlRow{
					{0, 0},
					{1, 1},
					{2, 2},
					{3, 3},
				},
			},
			{
				Query: "SELECT one_pk.pk,b.pk FROM one_pk JOIN one_pk b ON one_pk.pk = b.pk order by one_pk.pk",
				Expected: []sql.UntypedSqlRow{
					{0, 0},
					{1, 1},
					{2, 2},
					{3, 3},
				},
			},
			{
				Query: "SELECT one_pk.pk,b.pk FROM one_pk, one_pk b WHERE one_pk.pk = b.pk order by one_pk.pk",
				Expected: []sql.UntypedSqlRow{
					{0, 0},
					{1, 1},
					{2, 2},
					{3, 3},
				},
			},
			{
				Query: "select sum(x.i) + y.i from mytable as x, mytable as y where x.i = y.i GROUP BY x.i",
				Expected: []sql.UntypedSqlRow{
					{float64(2)},
					{float64(4)},
					{float64(6)},
				},
			},
			{
				Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
				Expected: []sql.UntypedSqlRow{
					{0, 0, 0, 0, 0},
					{1, 1, 1, 1, 1},
				},
			},
			{
				Query: `SELECT pk,nt.i,nt2.i FROM one_pk
						RIGHT JOIN niltable nt ON pk=nt.i
						RIGHT JOIN niltable nt2 ON pk=nt2.i - 1
						ORDER BY 3`,
				Expected: []sql.UntypedSqlRow{
					{nil, nil, 1},
					{1, 1, 2},
					{2, 2, 3},
					{3, 3, 4},
					{nil, nil, 5},
					{nil, nil, 6},
				},
			},
			{
				Query: `SELECT pk,pk2,
							(SELECT opk.c5 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1 LIMIT 1)
							FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
				Expected: []sql.UntypedSqlRow{
					{1, 1, 4},
					{1, 1, 4},
				},
			},
			{
				Query: `SELECT pk,pk2,
							(SELECT opk.c5 FROM one_pk opk JOIN two_pk tpk ON opk.c5=tpk.c5 ORDER BY 1 LIMIT 1)
							FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
				Expected: []sql.UntypedSqlRow{
					{1, 1, 4},
					{1, 1, 4},
				},
			},
			{
				Query: `SELECT /*+ JOIN_ORDER(mytable, othertable) */ s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
				Expected: []sql.UntypedSqlRow{
					{"third", 1, 1},
					{"second", 2, 2},
					{"first", 3, 3},
				},
			},
			{
				Query: `SELECT lefttable.i, righttable.s
			FROM (SELECT * FROM mytable) lefttable
			JOIN (SELECT * FROM mytable) righttable
			ON lefttable.i = righttable.i AND righttable.s = lefttable.s
			ORDER BY lefttable.i ASC`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b where a.i = b.i`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b where a.i = b.i OR a.i = 1`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row"},
					{1, "first row"},
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b where NOT(a.i = b.i OR a.s = b.i)`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row"},
					{1, "first row"},
					{2, "second row"},
					{2, "second row"},
					{3, "third row"},
					{3, "third row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where NOT(a.i = b.i OR a.s = b.i)`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row"},
					{1, "first row"},
					{2, "second row"},
					{2, "second row"},
					{3, "third row"},
					{3, "third row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b where a.i = b.s OR a.s = b.i IS FALSE`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.s OR a.s = b.i IS FALSE`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b where a.i >= b.i`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row"},
					{2, "second row"},
					{2, "second row"},
					{3, "third row"},
					{3, "third row"},
					{3, "third row"},
				},
			},
			{
				Query:    `SELECT a.* FROM mytable a, mytable b where a.i = a.s`,
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b where a.i in (2, 432, 7)`,
				Expected: []sql.UntypedSqlRow{
					{2, "second row"},
					{2, "second row"},
					{2, "second row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i AND c.i = d.i AND c.i = 2`,
				Expected: []sql.UntypedSqlRow{
					{2, "second row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
				Expected: []sql.UntypedSqlRow{
					{2, "second row"},
					{2, "second row"},
					{2, "second row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.s = c.s`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.i`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.i OR a.i = 1`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row"},
					{1, "first row"},
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i >= b.i`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row"},
					{2, "second row"},
					{2, "second row"},
					{3, "third row"},
					{3, "third row"},
					{3, "third row"},
				},
			},
			{
				Query:    `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = a.s`,
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.i = c.i AND c.i = d.i AND c.i = 2`,
				Expected: []sql.UntypedSqlRow{
					{2, "second row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
				Expected: []sql.UntypedSqlRow{
					{2, "second row"},
					{2, "second row"},
					{2, "second row"}},
			},
			{
				Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.s = c.s`,
				Expected: []sql.UntypedSqlRow{
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},

			{
				Query: `SELECT * FROM mytable WHERE (
			EXISTS (SELECT * FROM mytable Alias1 JOIN mytable Alias2 WHERE Alias1.i = (mytable.i + 1))
			AND EXISTS (SELECT * FROM othertable Alias1 JOIN othertable Alias2 WHERE Alias1.i2 = (mytable.i + 2)));`,
				Expected: []sql.UntypedSqlRow{{1, "first row"}},
			},
			{
				Query: `SELECT * FROM ab WHERE (
			EXISTS (SELECT * FROM ab Alias1 JOIN ab Alias2 WHERE Alias1.a = (ab.a + 1))
			AND EXISTS (SELECT * FROM xy Alias1 JOIN xy Alias2 WHERE Alias1.x = (ab.a + 2)));`,
				Expected: []sql.UntypedSqlRow{
					{0, 2},
					{1, 2}},
			},
			{
				// verify that duplicate aliases in different subqueries are allowed
				Query: `SELECT * FROM mytable Alias0 WHERE (
				      	EXISTS (SELECT * FROM mytable Alias WHERE Alias.i = Alias0.i + 1)
				      	AND EXISTS (SELECT * FROM othertable Alias WHERE Alias.i2 = Alias0.i + 2));`,
				Expected: []sql.UntypedSqlRow{{1, "first row"}},
			},
			{
				Query: `SELECT * FROM mytable
						WHERE
  							i = (SELECT i2 FROM othertable alias1 WHERE i2 = 2) AND
  							i+1 = (SELECT i2 FROM othertable alias1 WHERE i2 = 3);`,
				Expected: []sql.UntypedSqlRow{{2, "second row"}},
			},
			{
				Query: `SELECT * FROM mytable WHERE (
      					EXISTS (SELECT * FROM mytable Alias1 join mytable Alias2 WHERE Alias1.i = (mytable.i + 1))
      					AND EXISTS (SELECT * FROM othertable Alias1 join othertable Alias2 WHERE Alias1.i2 = (mytable.i + 2)))`,
				Expected: []sql.UntypedSqlRow{{1, "first row"}},
			},
		},
	},
	{
		name: "primary key range join",
		setup: [][]string{
			setup.MydbData[0],
			{
				"create table vals (val int primary key)",
				"create table ranges (min int primary key, max int, unique key(min,max))",
				"insert into vals values (0), (1), (2), (3), (4), (5), (6)",
				"insert into ranges values (0,2), (1,3), (2,4), (3,5), (4,6)",
			},
		},
		tests: rangeJoinOpTests,
	},
	{
		name: "keyless range join",
		setup: [][]string{
			setup.MydbData[0],
			{
				"create table vals (val int)",
				"create table ranges (min int, max int)",
				"insert into vals values (0), (1), (2), (3), (4), (5), (6)",
				"insert into ranges values (0,2), (1,3), (2,4), (3,5), (4,6)",
			},
		},
		tests: rangeJoinOpTests,
	},
	{
		name: "recursive range join",
		setup: [][]string{
			setup.MydbData[0],
		},
		tests: []JoinOpTests{{
			Query: "with recursive vals as (select 0 as val union all select val + 1 from vals where val < 6), " +
				"ranges as (select 0 as min, 2 as max union all select min+1, max+1 from ranges where max < 6) " +
				"select * from vals join ranges on val > min and val < max",
			Expected: []sql.UntypedSqlRow{
				{1, 0, 2},
				{2, 1, 3},
				{3, 2, 4},
				{4, 3, 5},
				{5, 4, 6},
			},
		}},
	},
	{
		name: "where x not in (...)",
		setup: [][]string{
			setup.XyData[0],
		},
		tests: []JoinOpTests{
			{
				Query:    `SELECT * from xy_hasnull where y not in (SELECT b from ab_hasnull)`,
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    `SELECT * from xy_hasnull where y not in (SELECT b from ab)`,
				Expected: []sql.UntypedSqlRow{{1, 0}},
			},
			{
				Query:    `SELECT * from xy where y not in (SELECT b from ab_hasnull)`,
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    `SELECT * from xy where null not in (SELECT b from ab)`,
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		name: "multi-column merge join",
		setup: [][]string{
			setup.Pk_tablesData[0],
		},
		tests: []JoinOpTests{
			{
				Query:    `SELECT l.pk1, l.pk2, l.c1, r.pk1, r.pk2, r.c1 FROM two_pk l JOIN two_pk r ON l.pk1=r.pk1 AND l.pk2=r.pk2`,
				Expected: []sql.UntypedSqlRow{{0, 0, 0, 0, 0, 0}, {0, 1, 10, 0, 1, 10}, {1, 0, 20, 1, 0, 20}, {1, 1, 30, 1, 1, 30}},
			},
			{
				Query:    `SELECT l.pk, r.pk FROM one_pk_two_idx l JOIN one_pk_two_idx r ON l.v1=r.v1 AND l.v2=r.v2`,
				Expected: []sql.UntypedSqlRow{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}},
			},
			{
				Query:    `SELECT l.pk, r.pk FROM one_pk_three_idx l JOIN one_pk_three_idx r ON l.v1=r.v1 AND l.v2=r.v2 AND l.pk=r.v1`,
				Expected: []sql.UntypedSqlRow{{0, 0}, {0, 1}},
			},
			{
				Query:    `SELECT l.pk1, l.pk2, r.pk FROM two_pk l JOIN one_pk_three_idx r ON l.pk2=r.v1 WHERE l.pk1 = 1`,
				Expected: []sql.UntypedSqlRow{{1, 0, 0}, {1, 0, 1}, {1, 0, 2}, {1, 0, 3}, {1, 1, 4}},
			},
			{
				Query:    `SELECT l.pk1, l.pk2, r.pk FROM two_pk l JOIN one_pk_three_idx r ON l.pk1=r.v1 WHERE l.pk2 = 1`,
				Expected: []sql.UntypedSqlRow{{0, 1, 0}, {0, 1, 1}, {0, 1, 2}, {0, 1, 3}, {1, 1, 4}},
			},
			{
				Query:    `SELECT l.pk, r.pk FROM one_pk_three_idx l JOIN one_pk_three_idx r ON l.pk=r.v1 WHERE l.pk = 1`,
				Expected: []sql.UntypedSqlRow{{1, 4}},
			},
		},
	},
	{
		name: "case insensitive key column names",
		setup: [][]string{
			{
				"CREATE TABLE DEPARTMENTS (ID VARCHAR(8) PRIMARY KEY, NAME VARCHAR(255));",
				"CREATE TABLE EMPLOYEES (ID VARCHAR(8) PRIMARY KEY, FIRSTNAME VARCHAR(255), DEPARTMENT_ID VARCHAR(8));",
				"INSERT INTO DEPARTMENTS (ID, NAME) VALUES ('101', 'Human Resources'), ('102', 'Finance'), ('103', 'IT');",
				"INSERT INTO EMPLOYEES (ID, FIRSTNAME,DEPARTMENT_ID) VALUES ('001', 'John', '101'), ('002', 'Jane','102'), ('003', 'Emily','103');",
			},
		},
		tests: []JoinOpTests{
			{
				Query: "SELECT * FROM EMPLOYEES e INNER JOIN DEPARTMENTS d ON e.DEPARTMENT_ID = d.ID WHERE e.DEPARTMENT_ID = '102';",
				Expected: []sql.UntypedSqlRow{
					{"002", "Jane", "102", "102", "Finance"},
				},
			},
			{
				Query: "SELECT * FROM EMPLOYEES e INNER JOIN DEPARTMENTS d ON e.department_id = d.ID WHERE e.department_id = '102';",
				Expected: []sql.UntypedSqlRow{
					{"002", "Jane", "102", "102", "Finance"},
				},
			},
			{
				Query: "SELECT * FROM EMPLOYEES e INNER JOIN DEPARTMENTS d ON e.DePaRtMeNt_Id = d.ID WHERE e.dEpArTmEnT_iD = '102';",
				Expected: []sql.UntypedSqlRow{
					{"002", "Jane", "102", "102", "Finance"},
				},
			},
		},
	},
}

var rangeJoinOpTests = []JoinOpTests{
	{
		Query: "select * from vals join ranges on val between min and max",
		Expected: []sql.UntypedSqlRow{
			{0, 0, 2},
			{1, 0, 2},
			{1, 1, 3},
			{2, 0, 2},
			{2, 1, 3},
			{2, 2, 4},
			{3, 1, 3},
			{3, 2, 4},
			{3, 3, 5},
			{4, 2, 4},
			{4, 3, 5},
			{4, 4, 6},
			{5, 3, 5},
			{5, 4, 6},
			{6, 4, 6},
		},
	},
	{
		Query: "select * from vals join ranges on val > min and val < max",
		Expected: []sql.UntypedSqlRow{
			{1, 0, 2},
			{2, 1, 3},
			{3, 2, 4},
			{4, 3, 5},
			{5, 4, 6},
		},
	},
	{
		Query: "select * from vals join ranges on min < val and max > val",
		Expected: []sql.UntypedSqlRow{
			{1, 0, 2},
			{2, 1, 3},
			{3, 2, 4},
			{4, 3, 5},
			{5, 4, 6},
		},
	},
	{
		Query: "select * from vals join ranges on val >= min and val < max",
		Expected: []sql.UntypedSqlRow{
			{0, 0, 2},
			{1, 0, 2},
			{1, 1, 3},
			{2, 1, 3},
			{2, 2, 4},
			{3, 2, 4},
			{3, 3, 5},
			{4, 3, 5},
			{4, 4, 6},
			{5, 4, 6},
		},
	},
	{
		Query: "select * from vals join ranges on val > min and val <= max",
		Expected: []sql.UntypedSqlRow{
			{1, 0, 2},
			{2, 0, 2},
			{2, 1, 3},
			{3, 1, 3},
			{3, 2, 4},
			{4, 2, 4},
			{4, 3, 5},
			{5, 3, 5},
			{5, 4, 6},
			{6, 4, 6},
		},
	},
	{
		Query: "select * from vals join ranges on val >= min and val <= max",
		Expected: []sql.UntypedSqlRow{
			{0, 0, 2},
			{1, 0, 2},
			{1, 1, 3},
			{2, 0, 2},
			{2, 1, 3},
			{2, 2, 4},
			{3, 1, 3},
			{3, 2, 4},
			{3, 3, 5},
			{4, 2, 4},
			{4, 3, 5},
			{4, 4, 6},
			{5, 3, 5},
			{5, 4, 6},
			{6, 4, 6},
		},
	},
	{
		Query: "select * from vals left join ranges on val > min and val < max",
		Expected: []sql.UntypedSqlRow{
			{0, nil, nil},
			{1, 0, 2},
			{2, 1, 3},
			{3, 2, 4},
			{4, 3, 5},
			{5, 4, 6},
			{6, nil, nil},
		},
	},
	{
		Query: "select * from ranges l join ranges r on l.min > r.min and l.min < r.max",
		Expected: []sql.UntypedSqlRow{
			{1, 3, 0, 2},
			{2, 4, 1, 3},
			{3, 5, 2, 4},
			{4, 6, 3, 5},
		},
	},
	{
		Query: "select * from vals left join ranges r1 on val > r1.min and val < r1.max left join ranges r2 on r1.min > r2.min and r1.min < r2.max",
		Expected: []sql.UntypedSqlRow{
			{0, nil, nil, nil, nil},
			{1, 0, 2, nil, nil},
			{2, 1, 3, 0, 2},
			{3, 2, 4, 1, 3},
			{4, 3, 5, 2, 4},
			{5, 4, 6, 3, 5},
			{6, nil, nil, nil, nil},
		},
	},
	{
		Query: "select * from (select vals.val * 2 as val from vals) as newVals join (select ranges.min * 2 as min, ranges.max * 2 as max from ranges) as newRanges on val > min and val < max;",
		Expected: []sql.UntypedSqlRow{
			{2, 0, 4},
			{4, 2, 6},
			{6, 4, 8},
			{8, 6, 10},
			{10, 8, 12},
		},
	},
	{
		// This tests that the RangeHeapJoin node functions correctly even if its rows are iterated over multiple times.
		Query: "select * from (select 1 union select 2) as l left join (select * from vals join ranges on val > min and val < max) as r on max = max",
		Expected: []sql.UntypedSqlRow{
			{1, 1, 0, 2},
			{1, 2, 1, 3},
			{1, 3, 2, 4},
			{1, 4, 3, 5},
			{1, 5, 4, 6},
			{2, 1, 0, 2},
			{2, 2, 1, 3},
			{2, 3, 2, 4},
			{2, 4, 3, 5},
			{2, 5, 4, 6},
		},
	},
	{
		Query: "select * from vals left join (select * from ranges where 0) as newRanges on val > min and val < max;",
		Expected: []sql.UntypedSqlRow{
			{0, nil, nil},
			{1, nil, nil},
			{2, nil, nil},
			{3, nil, nil},
			{4, nil, nil},
			{5, nil, nil},
			{6, nil, nil},
		},
	},
}
