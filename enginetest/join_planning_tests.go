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

package enginetest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type JoinPlanTest struct {
	q             string
	types         []plan.JoinType
	indexes       []string
	mergeCompares []string
	exp           []sql.Row
	// order is a list of acceptable join plan orders.
	// used for statistics test plans that are unlikely but otherwise
	// cause flakes in CI for lack of seed control.
	order   [][]string
	skipOld bool
}

type joinPlanScript struct {
	name  string
	setup []string
	tests []JoinPlanTest
}

var JoinPlanningTests = []joinPlanScript{
	{
		name: "filter pushdown through join uppercase name",
		setup: []string{
			"create database mydb1",
			"create database mydb2",
			"create table mydb1.xy (x int primary key, y int)",
			"create table mydb2.xy (x int primary key, y int)",
			"insert into mydb1.xy values (0,0)",
			"insert into mydb2.xy values (1,1)",
		},
		tests: []JoinPlanTest{
			{
				q:   "select * from mydb1.xy, mydb2.xy",
				exp: []sql.Row{{0, 0, 1, 1}},
			},
		},
	},
	{
		name: "info schema plans",
		setup: []string{
			"CREATE table xy (x int primary key, y int);",
		},
		tests: []JoinPlanTest{
			{
				q:     "select count(t.*) from information_schema.columns c join information_schema.tables t on `t`.`TABLE_NAME` = `c`.`TABLE_NAME`",
				types: []plan.JoinType{plan.JoinTypeHash},
				exp:   []sql.Row{{734}},
			},
		},
	},
	{
		name: "block merge join",
		setup: []string{
			"CREATE table xy (x int primary key, y int, unique index y_idx(y));",
			"CREATE table ab (a int primary key, b int);",
			"insert into xy values (1,0), (2,1), (0,2), (3,3);",
			"insert into ab values (0,2), (1,2), (2,2), (3,1);",
			`analyze table xy update histogram on x using data '{"row_count":1000000}'`,
			`analyze table ab update histogram on a using data '{"row_count":1000000}'`,
		},
		tests: []JoinPlanTest{
			{
				q:     "select /*+ JOIN_ORDER(ab, xy) MERGE_JOIN(ab, xy)*/ * from ab join xy on y = a order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 2, 1, 0}, {1, 2, 2, 1}, {2, 2, 0, 2}, {3, 1, 3, 3}},
			},
			{
				q:     "select * from ab join xy on x = a and y = a order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{3, 1, 3, 3}},
			},
			{
				q:   "set @@SESSION.disable_merge_join = 1",
				exp: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				q:     "select /*+ JOIN_ORDER(ab, xy) MERGE_JOIN(ab, xy)*/ * from ab join xy on y = a order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeLookup},
				exp:   []sql.Row{{0, 2, 1, 0}, {1, 2, 2, 1}, {2, 2, 0, 2}, {3, 1, 3, 3}},
			},
			{
				q:     "select * from ab join xy on x = a and y = a order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeLookup},
				exp:   []sql.Row{{3, 1, 3, 3}},
			},
		},
	},
	{
		name: "merge join unary index",
		setup: []string{
			"CREATE table xy (x int primary key, y int, unique index y_idx(y));",
			"create table rs (r int primary key, s int, index s_idx(s));",
			"CREATE table uv (u int primary key, v int);",
			"CREATE table ab (a int primary key, b int);",
			"insert into xy values (1,0), (2,1), (0,2), (3,3);",
			"insert into rs values (0,0), (1,0), (2,0), (4,4), (5,4);",
			"insert into uv values (0,1), (1,1), (2,2), (3,2);",
			"insert into ab values (0,2), (1,2), (2,2), (3,1);",
			`analyze table xy update histogram on x using data '{"row_count":1000}'`,
			`analyze table rs update histogram on r using data '{"row_count":1000}'`,
			`analyze table uv update histogram on u using data '{"row_count":1000}'`,
			`analyze table ab update histogram on a using data '{"row_count":1000}'`,
		},
		tests: []JoinPlanTest{
			{
				q:     "select u,a,y from uv join (select /*+ JOIN_ORDER(ab, xy) MERGE_JOIN(ab, xy) */ * from ab join xy on y = a) r on u = r.a order by 1",
				types: []plan.JoinType{plan.JoinTypeLookup, plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 0, 0}, {1, 1, 1}, {2, 2, 2}, {3, 3, 3}},
			},
			{
				q:     "select /*+ JOIN_ORDER(ab, xy) MERGE_JOIN(ab, xy)*/ * from ab join xy on y = a order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 2, 1, 0}, {1, 2, 2, 1}, {2, 2, 0, 2}, {3, 1, 3, 3}},
			},
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) MERGE_JOIN(rs, xy) */ * from rs left join xy on y = s order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeLeftOuterMerge},
				exp:   []sql.Row{{0, 0, 1, 0}, {1, 0, 1, 0}, {2, 0, 1, 0}, {4, 4, nil, nil}, {5, 4, nil, nil}},
			},
			{
				// extra join condition does not filter left-only rows
				q:     "select /*+ JOIN_ORDER(rs, xy) MERGE_JOIN(rs, xy) */ * from rs left join xy on y = s and y+s = 0 order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeLeftOuterMerge},
				exp:   []sql.Row{{0, 0, 1, 0}, {1, 0, 1, 0}, {2, 0, 1, 0}, {4, 4, nil, nil}, {5, 4, nil, nil}},
			},
			{
				// extra join condition does not filter left-only rows
				q:     "select /*+ JOIN_ORDER(rs, xy) MERGE_JOIN(rs, xy) */ * from rs left join xy on y+2 = s and s-y = 2 order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeLeftOuterMerge},
				exp:   []sql.Row{{0, 0, nil, nil}, {1, 0, nil, nil}, {2, 0, nil, nil}, {4, 4, 0, 2}, {5, 4, 0, 2}},
			},
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) MERGE_JOIN(rs, xy) */ * from rs join xy on y = r order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 0, 1, 0}, {1, 0, 2, 1}, {2, 0, 0, 2}},
			},
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) MERGE_JOIN(rs, xy) */ * from rs join xy on r = y order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 0, 1, 0}, {1, 0, 2, 1}, {2, 0, 0, 2}},
			},
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) MERGE_JOIN(rs, xy) */ * from rs join xy on y = s order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 0, 1, 0}, {1, 0, 1, 0}, {2, 0, 1, 0}},
			},
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) MERGE_JOIN(rs, xy) */ * from rs join xy on y = s and y = r order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 0, 1, 0}},
			},
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) MERGE_JOIN(rs, xy) */ * from rs join xy on y+2 = s order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{4, 4, 0, 2}, {5, 4, 0, 2}},
			},
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on y = s-1 order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeLookup},
				exp:   []sql.Row{{4, 4, 3, 3}, {5, 4, 3, 3}},
			},
			// {
			// TODO: cannot hash join on compound expressions
			//	q:     "select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on y = mod(s,2) order by 1, 3",
			//	types: []plan.JoinType{plan.JoinTypeInner},
			//	exp:   []sql.Row{{0,0,1,0},{0, 0, 1, 0},{2,0,1,0},{4,4,1,0}},
			// },
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) MERGE_JOIN(rs, xy) */ * from rs join xy on 2 = s+y order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeInner},
				exp:   []sql.Row{{0, 0, 0, 2}, {1, 0, 0, 2}, {2, 0, 0, 2}},
			},
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) MERGE_JOIN(rs, xy) */ * from rs join xy on y > s+2 order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeInner},
				exp:   []sql.Row{{0, 0, 3, 3}, {1, 0, 3, 3}, {2, 0, 3, 3}},
			},
		},
	},
	{
		name: "merge join multi match",
		setup: []string{
			"CREATE table xy (x int primary key, y int, index y_idx(y));",
			"create table rs (r int primary key, s int, index s_idx(s));",
			"insert into xy values (1,0), (2,1), (0,8), (3,7), (5,4), (4,0);",
			"insert into rs values (0,0),(2,3),(3,0), (4,8), (5,4);",
			`analyze table xy update histogram on x using data '{"row_count":1000}'`,
			`analyze table rs update histogram on r using data '{"row_count":1000}'`,
		},
		tests: []JoinPlanTest{
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) MERGE_JOIN(rs, xy) */ * from rs join xy on y = s order by 1,3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 0, 1, 0}, {0, 0, 4, 0}, {3, 0, 1, 0}, {3, 0, 4, 0}, {4, 8, 0, 8}, {5, 4, 5, 4}},
			},
		},
	},
	{
		name: "merge join zero rows",
		setup: []string{
			"CREATE table xy (x int primary key, y int, index y_idx(y));",
			"create table rs (r int primary key, s int, index s_idx(s));",
			"insert into xy values (1,0);",
			`analyze table xy update histogram on x using data '{"row_count":10}'`,
			`analyze table rs update histogram on r using data '{"row_count":1000}'`,
		},
		tests: []JoinPlanTest{
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) MERGE_JOIN(rs, xy) */ * from rs join xy on y = s order by 1,3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{},
			},
		},
	},
	{
		// todo: rewrite implementing new stats interface
		name: "merge join large and small table",
		setup: []string{
			"CREATE table xy (x int primary key, y int, index y_idx(y));",
			"create table rs (r int primary key, s int, index s_idx(s));",
			"insert into xy values (1,0), (2,1), (0,8), (3,7), (5,4), (4,0);",
			"insert into rs values (0,0),(2,3),(3,0), (4,8), (5,4);",
			`analyze table xy update histogram on x using data '{"row_count":10}'`,
			`analyze table rs update histogram on r using data '{"row_count":1000000000}'`,
		},
		tests: []JoinPlanTest{
			{
				// When primary table is much larger, doing many lookups is expensive: prefer merge
				q:     "select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on x = r order by 1,3",
				types: []plan.JoinType{plan.JoinTypeLookup},
				exp:   []sql.Row{{0, 0, 0, 8}, {2, 3, 2, 1}, {3, 0, 3, 7}, {4, 8, 4, 0}, {5, 4, 5, 4}},
			},
			{
				// When secondary table is much larger, avoid reading the entire table: prefer lookup
				q:     "select /*+ JOIN_ORDER(xy, rs) */ * from xy join rs on x = r order by 1,3",
				types: []plan.JoinType{plan.JoinTypeLookup},
				exp:   []sql.Row{{0, 8, 0, 0}, {2, 1, 2, 3}, {3, 7, 3, 0}, {4, 0, 4, 8}, {5, 4, 5, 4}},
			},
		},
	},
	{
		name: "merge join multi arity",
		setup: []string{
			"CREATE table xy (x int primary key, y int, index yx_idx(y,x));",
			"create table rs (r int primary key, s int, index s_idx(s));",
			"insert into xy values (1,0), (2,1), (0,8), (3,7), (5,4), (4,0);",
			"insert into rs values (0,0),(2,3),(3,0), (4,8), (5,4);",
			`analyze table xy update histogram on x using data '{"row_count":1000}'`,
			`analyze table rs update histogram on r using data '{"row_count":1000}'`,
		},
		tests: []JoinPlanTest{
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) MERGE_JOIN(rs, xy) */ * from rs join xy on y = s order by 1,3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 0, 1, 0}, {0, 0, 4, 0}, {3, 0, 1, 0}, {3, 0, 4, 0}, {4, 8, 0, 8}, {5, 4, 5, 4}},
			},
		},
	},
	{
		name:  "multi-column merge join",
		setup: setup.Pk_tablesData[0],

		tests: []JoinPlanTest{
			{
				// Find a unique index, even if it has multiple columns
				q:             `SELECT /*+ MERGE_JOIN(l,r) JOIN_ORDER(r,l) */ l.pk1, l.pk2, l.c1, r.pk1, r.pk2, r.c1 FROM two_pk l JOIN two_pk r ON l.pk1=r.pk1 AND l.pk2=r.pk2`,
				types:         []plan.JoinType{plan.JoinTypeMerge},
				mergeCompares: []string{"((r.pk1, r.pk2) = (l.pk1, l.pk2))"},
				exp:           []sql.Row{{0, 0, 0, 0, 0, 0}, {0, 1, 10, 0, 1, 10}, {1, 0, 20, 1, 0, 20}, {1, 1, 30, 1, 1, 30}},
			},
			{
				// Prefer a two-column non-unique index over a one-column non-unique index
				q:             `SELECT /*+ MERGE_JOIN(l,r) JOIN_ORDER(r,l) */ l.pk, r.pk FROM one_pk_two_idx l JOIN one_pk_two_idx r ON l.v1=r.v1 AND l.v2=r.v2`,
				types:         []plan.JoinType{plan.JoinTypeMerge},
				mergeCompares: []string{"((r.v1, r.v2) = (l.v1, l.v2))"},
				exp:           []sql.Row{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}},
			},
			{
				// Prefer a one-column unique index over a two-column non-unique index
				q:             `SELECT /*+ MERGE_JOIN(l,r) */ l.pk, r.pk FROM one_pk_three_idx l JOIN one_pk_three_idx r ON l.v1=r.v1 AND l.v2=r.v2 AND l.pk=r.v1`,
				types:         []plan.JoinType{plan.JoinTypeMerge},
				mergeCompares: []string{"(l.pk = r.v1)"},
				exp:           []sql.Row{{0, 0}, {0, 1}},
			},
			{
				// Allow an index with a prefix that is determined to be constant.
				q:             `SELECT /*+ MERGE_JOIN(l,r) */ l.pk1, l.pk2, r.pk FROM two_pk l JOIN one_pk_three_idx r ON l.pk2=r.v1 WHERE l.pk1 = 1`,
				types:         []plan.JoinType{plan.JoinTypeMerge},
				mergeCompares: []string{"(l.pk2 = r.v1)"},
				exp:           []sql.Row{{1, 0, 0}, {1, 0, 1}, {1, 0, 2}, {1, 0, 3}, {1, 1, 4}},
			},
			{
				// Allow an index where the final index column is determined to be constant.
				q:             `SELECT /*+ MERGE_JOIN(l,r) */ l.pk1, l.pk2, r.pk FROM two_pk l JOIN one_pk_three_idx r ON l.pk1=r.v1 WHERE l.pk2 = 1`,
				types:         []plan.JoinType{plan.JoinTypeMerge},
				mergeCompares: []string{"(r.v1 = l.pk1)"},
				exp:           []sql.Row{{0, 1, 0}, {0, 1, 1}, {0, 1, 2}, {0, 1, 3}, {1, 1, 4}},
			},
			{
				// Allow an index where the key expression is determined to be constant.
				q:             `SELECT /*+ MERGE_JOIN(l,r) */ l.pk, r.pk FROM one_pk_three_idx l JOIN one_pk_three_idx r ON l.pk=r.v1 WHERE l.pk = 1`,
				types:         []plan.JoinType{plan.JoinTypeMerge},
				mergeCompares: []string{"(r.v1 = l.pk)"},
				exp:           []sql.Row{{1, 4}},
			},
		},
	},
	{
		name: "merge join keyless index",
		setup: []string{
			"CREATE table xy (x int, y int, index yx_idx(y,x));",
			"create table rs (r int, s int, index s_idx(s));",
			"insert into xy values (1,0), (2,1), (0,8), (3,7), (5,4), (4,0);",
			"insert into rs values (0,0),(2,3),(3,0), (4,8), (5,4);",
			`analyze table xy update histogram on x using data '{"row_count":1000}'`,
			`analyze table rs update histogram on r using data '{"row_count":1000}'`,
		},
		tests: []JoinPlanTest{
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) MERGE_JOIN(rs, xy) */ * from rs join xy on y = s order by 1,3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 0, 1, 0}, {0, 0, 4, 0}, {3, 0, 1, 0}, {3, 0, 4, 0}, {4, 8, 0, 8}, {5, 4, 5, 4}},
			},
		},
	},
	{
		name: "partial [lookup] join tests",
		setup: []string{
			"CREATE table xy (x int primary key, y int);",
			"create table rs (r int primary key, s int);",
			"CREATE table uv (u int primary key, v int);",
			"CREATE table ab (a int primary key, b int);",
			"insert into xy values (1,0), (2,1), (0,2), (3,3);",
			"insert into rs values (0,0), (1,0), (2,0), (4,4);",
			"insert into uv values (0,1), (1,1), (2,2), (3,2);",
			"insert into ab values (0,2), (1,2), (2,2), (3,1);",
			`analyze table xy update histogram on x using data '{"row_count":100}'`,
			`analyze table rs update histogram on r using data '{"row_count":100}'`,
			`analyze table uv update histogram on u using data '{"row_count":100}'`,
			`analyze table ab update histogram on a using data '{"row_count":100}'`,
		},
		tests: []JoinPlanTest{
			{
				q:     "select /*+ LOOKUP_JOIN(ab,xy) JOIN_ORDER(ab,xy) */ * from xy where x = 1 and y in (select a from ab);",
				types: []plan.JoinType{plan.JoinTypeLookup},
				exp:   []sql.Row{{1, 0}},
			},
			{
				q:     "select /*+ LOOKUP_JOIN(xy,ab) */ * from xy where x in (select b from ab where a in (0,1,2));",
				types: []plan.JoinType{plan.JoinTypeLookup},
				exp:   []sql.Row{{2, 1}},
			},
			{
				// TODO: RIGHT_SEMI_JOIN tuple equalities
				q:     "select /*+ LOOKUP_JOIN(xy,ab) */ * from xy where (x,y) in (select b,a from ab where a in (0,1,2));",
				types: []plan.JoinType{plan.JoinTypeInner},
				exp:   []sql.Row{{2, 1}},
			},
			{
				q:     "select /*+ LOOKUP_JOIN(xy,ab) */ * from xy where x in (select a from ab);",
				types: []plan.JoinType{plan.JoinTypeSemiLookup},
				exp:   []sql.Row{{2, 1}, {1, 0}, {0, 2}, {3, 3}},
			},
			{
				q:     "select /*+ LOOKUP_JOIN(xy,ab) */ * from xy where x in (select a from ab where a in (1,2));",
				types: []plan.JoinType{plan.JoinTypeLookup},
				exp:   []sql.Row{{2, 1}, {1, 0}},
			},
			{
				q:     "select /*+ LOOKUP_JOIN(xy,ab)  */* from xy where x in (select a from ab);",
				types: []plan.JoinType{plan.JoinTypeSemiLookup},
				exp:   []sql.Row{{2, 1}, {1, 0}, {0, 2}, {3, 3}},
			},
			{
				q:     "select /*+ LOOKUP_JOIN(xy,ab) MERGE_JOIN(ab,uv) JOIN_ORDER(ab,uv,xy) */ * from xy where EXISTS (select 1 from ab join uv on a = u where x = a);",
				types: []plan.JoinType{plan.JoinTypeLookup, plan.JoinTypeMerge},
				exp:   []sql.Row{{2, 1}, {1, 0}, {0, 2}, {3, 3}},
			},
			{
				q:     "select * from xy where y+1 not in (select u from uv);",
				types: []plan.JoinType{plan.JoinTypeLeftOuterHashExcludeNulls},
				exp:   []sql.Row{{3, 3}},
			},
			{
				q:     "select * from xy where x not in (select u from uv where u not in (select a from ab where a not in (select r from rs where r = 1))) order by 1;",
				types: []plan.JoinType{plan.JoinTypeLeftOuterHashExcludeNulls, plan.JoinTypeLeftOuterHashExcludeNulls, plan.JoinTypeLeftOuter},
				exp:   []sql.Row{{0, 2}, {2, 1}, {3, 3}},
			},
			{
				q:     "select * from xy where x != (select r from rs where r = 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeLeftOuter},
				exp:   []sql.Row{{0, 2}, {2, 1}, {3, 3}},
			},
			{
				// anti join will be cross-join-right, be passed non-nil parent row
				q:     "select x,a from ab, (select * from xy where x != (select r from rs where r = 1) order by 1) sq where x = 2 and b = 2 order by 1,2;",
				types: []plan.JoinType{plan.JoinTypeCrossHash, plan.JoinTypeLeftOuter},
				exp:   []sql.Row{{2, 0}, {2, 1}, {2, 2}},
			},
			{
				// scope and parent row are non-nil
				q: `
select * from uv where u > (
  select x from ab, (
    select x from xy where x != (
      select r from rs where r = 1
    ) order by 1
  ) sq
  order by 1 limit 1
)
order by 1;`,
				types: []plan.JoinType{plan.JoinTypeSemi, plan.JoinTypeCrossHash, plan.JoinTypeLeftOuter},
				exp:   []sql.Row{{1, 1}, {2, 2}, {3, 2}},
			},
			{
				// cast prevents scope merging
				q:     "select * from xy where x != (select cast(r as signed) from rs where r = 1) order by 1;",
				types: []plan.JoinType{},
				exp:   []sql.Row{{0, 2}, {2, 1}, {3, 3}},
			},
			{
				// order by will be discarded
				q:     "select * from xy where x != (select r from rs where r = 1 order by 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeLeftOuter},
				exp:   []sql.Row{{0, 2}, {2, 1}, {3, 3}},
			},
			{
				// limit prevents scope merging
				q:     "select * from xy where x != (select r from rs where r = 1 limit 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeLeftOuter},
				exp:   []sql.Row{{0, 2}, {2, 1}, {3, 3}},
			},
			{
				q:     "select * from xy where y-1 in (select u from uv) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemiLookup},
				exp:   []sql.Row{{0, 2}, {2, 1}, {3, 3}},
			},
			{
				// semi join will be right-side, be passed non-nil parent row
				q:     "select x,a from ab, (select * from xy where x = (select r from rs where r = 1) order by 1) sq order by 1,2",
				types: []plan.JoinType{plan.JoinTypeCrossHash, plan.JoinTypeLookup},
				exp:   []sql.Row{{1, 0}, {1, 1}, {1, 2}, {1, 3}},
			},
			// {
			// scope and parent row are non-nil
			// TODO: subquery alias unable to track parent row from a different scope
			//				q: `
			// select * from uv where u > (
			//  select x from ab, (
			//    select x from xy where x = (
			//      select r from rs where r = 1
			//    ) order by 1
			//  ) sq
			//  order by 1 limit 1
			// )
			// order by 1;`,
			// types: []plan.JoinType{plan.JoinTypeCrossHash, plan.JoinTypeLookup},
			// exp:   []sql.Row{{2, 2}, {3, 2}},
			// },
			{
				q:     "select * from xy where y-1 in (select cast(u as signed) from uv) order by 1;",
				types: []plan.JoinType{},
				exp:   []sql.Row{{0, 2}, {2, 1}, {3, 3}},
			},
			{
				q:     "select * from xy where y-1 in (select u from uv order by 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemiLookup},
				exp:   []sql.Row{{0, 2}, {2, 1}, {3, 3}},
			},
			{
				q:     "select * from xy where y-1 in (select u from uv order by 1 limit 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeHash},
				exp:   []sql.Row{{2, 1}},
			},
			{
				q:     "select * from xy where x in (select u from uv join ab on u = a and a = 2) order by 1;",
				types: []plan.JoinType{plan.JoinTypeLookup, plan.JoinTypeLookup},
				exp:   []sql.Row{{2, 1}},
			},
			{
				// group by doesn't transform
				q:     "select * from xy where y-1 in (select u from uv group by u having u = 2 order by 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemi},
				exp:   []sql.Row{{3, 3}},
			},
			{
				// window doesn't transform
				q:     "select * from xy where y-1 in (select row_number() over (order by v) from uv) order by 1;",
				types: []plan.JoinType{},
				exp:   []sql.Row{{0, 2}, {3, 3}},
			},
		},
	},
	{
		name: "empty join tests",
		setup: []string{
			"CREATE table xy (x int primary key, y int);",
			"CREATE table uv (u int primary key, v int);",
			"insert into xy values (1,0), (2,1), (0,2), (3,3);",
			"insert into uv values (0,1), (1,1), (2,2), (3,2);",
		},
		tests: []JoinPlanTest{
			{
				q:     "select * from xy where y-1 = (select u from uv where u = 4);",
				types: []plan.JoinType{plan.JoinTypeSemi},
				exp:   []sql.Row{},
			},
			{
				q:     "select * from xy where x = 1 and x != (select u from uv where u = 4);",
				types: []plan.JoinType{plan.JoinTypeLeftOuter},
				exp:   []sql.Row{{1, 0}},
			},
			{
				q:     "select * from xy where x = 1 and x not in (select u from uv where u = 4);",
				types: []plan.JoinType{plan.JoinTypeLeftOuter},
				exp:   []sql.Row{{1, 0}},
			},
			{
				q:     "select * from xy where x = 1 and not exists (select u from uv where u = 4);",
				types: []plan.JoinType{plan.JoinTypeLeftOuter},
				exp:   []sql.Row{{1, 0}},
			},
		},
	},
	{
		name: "unnest with scope filters",
		setup: []string{
			"create table ab (a int primary key, b int);",
			"create table rs (r int primary key, s int);",
			"CREATE table xy (x int primary key, y int);",
			"CREATE table uv (u int primary key, v int);",
			"insert into ab values (0,2), (1,2), (2,2), (3,1);",
			"insert into rs values (0,0), (1,0), (2,0), (4,4), (5,4);",
			"insert into xy values (1,0), (2,1), (0,2), (3,3);",
			"insert into uv values (0,1), (1,1), (2,2), (3,2);",
			`analyze table xy update histogram on x using data '{"row_count":100}'`,
			`analyze table rs update histogram on r using data '{"row_count":100}'`,
			`analyze table uv update histogram on u using data '{"row_count":100}'`,
			`analyze table ab update histogram on a using data '{"row_count":100}'`,
		},
		tests: []JoinPlanTest{
			{
				q: `
SELECT x
FROM xy 
WHERE EXISTS (SELECT count(v) AS count_1 
FROM uv 
WHERE y = v and v = 1 GROUP BY v
HAVING count(v) >= 1)`,
				types: []plan.JoinType{},
				exp:   []sql.Row{{2}},
			},
			{
				q:     "select * from xy where y-1 = (select u from uv where v = 2 order by 1 limit 1);",
				types: []plan.JoinType{plan.JoinTypeHash},
				exp:   []sql.Row{{3, 3}},
			},
			{
				q:     "select * from xy where x != (select u from uv where v = 2 order by 1 limit 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeLeftOuterMerge},
				exp:   []sql.Row{{0, 2}, {1, 0}, {3, 3}},
			},
			{
				q:     "select * from xy where x != (select distinct u from uv where v = 2 order by 1 limit 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeLeftOuterMerge},
				exp:   []sql.Row{{0, 2}, {1, 0}, {3, 3}},
			},
			{
				q:     "select * from xy where (x,y+1) = (select u,v from uv where v = 2 order by 1 limit 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeHash},
				exp:   []sql.Row{{2, 1}},
			},
			{
				q:     "select * from xy where x in (select cnt from (select count(u) as cnt from uv group by v having cnt > 0) sq) order by 1,2;",
				types: []plan.JoinType{plan.JoinTypeLookup},
				exp:   []sql.Row{{2, 1}},
			},
			{
				q: `SELECT /*+ LOOKUP_JOIN(xy, alias2) LOOKUP_JOIN(xy, alias1) JOIN_ORDER(xy, alias2, alias1) */ * FROM xy WHERE (
      				EXISTS (SELECT * FROM xy Alias1 WHERE Alias1.x = (xy.x + 1))
      				AND EXISTS (SELECT * FROM uv Alias2 WHERE Alias2.u = (xy.x + 2)));`,
				// These should both be JoinTypeSemiLookup, but for https://github.com/dolthub/go-mysql-server/issues/1893
				types: []plan.JoinType{plan.JoinTypeSemiLookup, plan.JoinTypeSemiLookup},
				exp:   []sql.Row{{0, 2}, {1, 0}},
			},
			{
				q: `SELECT *
FROM ab A0
WHERE EXISTS (
    SELECT U0.a
    FROM
    (
        ab U0
        LEFT OUTER JOIN
        rs U1
        ON (U0.a = U1.s)
    )
    WHERE (U1.s IS NULL AND U0.a = A0.a)
);`,
				types: []plan.JoinType{plan.JoinTypeLookup, plan.JoinTypeLeftOuterHash},
				exp: []sql.Row{
					{1, 2},
					{2, 2},
					{3, 1},
				},
			},
			{
				q:     `select * from xy where exists (select * from uv) and x = 0`,
				types: []plan.JoinType{plan.JoinTypeCross},
				exp:   []sql.Row{{0, 2}},
			},
			{
				q: `
select x from xy where
  not exists (select a from ab where a = x and a = 1) and
  not exists (select a from ab where a = x and a = 2)`,
				types: []plan.JoinType{plan.JoinTypeLeftOuter, plan.JoinTypeLeftOuter},
				exp:   []sql.Row{{0}, {3}},
			},
			{
				q: `
select * from xy where x in (
    with recursive tree(s) AS (
        SELECT 1
    )
    SELECT u FROM uv, tree where u = s
)`,
				types: []plan.JoinType{plan.JoinTypeLookup, plan.JoinTypeLookup},
				exp:   []sql.Row{{1, 0}},
			},
			{
				q: `
SELECT *
FROM xy
  WHERE
    EXISTS (
    SELECT 1
    FROM ab
    WHERE
      xy.x = ab.a AND
      EXISTS (
        SELECT 1
        FROM uv
        WHERE
          ab.a = uv.v
    )
  )`,
				types: []plan.JoinType{plan.JoinTypeLookup, plan.JoinTypeLookup},
				exp:   []sql.Row{{1, 0}, {2, 1}},
			},
			{
				q:     `select * from xy where exists (select * from uv join ab on u = a)`,
				types: []plan.JoinType{plan.JoinTypeCross, plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 2}, {1, 0}, {2, 1}, {3, 3}},
			},
		},
	},
	{
		name: "unnest non-equality comparisons",
		setup: []string{
			"CREATE table xy (x int primary key, y int);",
			"CREATE table uv (u int primary key, v int);",
			"insert into xy values (1,0), (2,1), (0,2), (3,3);",
			"insert into uv values (0,1), (1,1), (2,2), (3,2);",
		},
		tests: []JoinPlanTest{
			{
				q:     "select /*+ LOOKUP_JOIN(uv, xy) */ * from xy where y >= (select u from uv where u = 2) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemiLookup},
				exp:   []sql.Row{{0, 2}, {3, 3}},
			},
			{
				q:     "select /*+ LOOKUP_JOIN(uv, xy) */ * from xy where x <= (select u from uv where u = 2) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemiLookup},
				exp:   []sql.Row{{0, 2}, {1, 0}, {2, 1}},
			},
			{
				q:     "select /*+ LOOKUP_JOIN(uv, xy) */ * from xy where x < (select u from uv where u = 2) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemiLookup},
				exp:   []sql.Row{{0, 2}, {1, 0}},
			},
			{
				q:     "select /*+ LOOKUP_JOIN(uv,xy) */ * from xy where x > (select u from uv where u = 2) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemiLookup},
				exp:   []sql.Row{{3, 3}},
			},
			{
				q:     "select /*+ LOOKUP_JOIN(uv, uv_1) */ * from uv where v <=> (select u from uv where u = 2) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemiLookup},
				exp:   []sql.Row{{2, 2}, {3, 2}},
			},
		},
	},
	{
		name: "unnest twice-nested subquery",
		setup: []string{
			"CREATE table xy (x int primary key, y int);",
			"CREATE table uv (u int primary key, v int);",
			"insert into xy values (1,0), (2,1), (0,2), (3,3);",
			"insert into uv values (0,1), (1,1), (2,2), (3,2);",
		},
		tests: []JoinPlanTest{
			{
				q:     "select * from xy where x in (select * from (select 1) r where x = 1);",
				types: []plan.JoinType{plan.JoinTypeSemi},
				exp:   []sql.Row{{1, 0}},
			},
			{
				q:     "select * from xy where x in (select 1 where 1 in (select 1 where 1 in (select 1 where x != 2)) and x = 1);",
				types: []plan.JoinType{plan.JoinTypeSemi, plan.JoinTypeSemi, plan.JoinTypeSemi},
				exp:   []sql.Row{{1, 0}},
			},
			{
				q:     "select * from xy where x in (select * from (select 1 where 1 in (select 1 where x != 2)) r where x = 1);",
				types: []plan.JoinType{plan.JoinTypeInner, plan.JoinTypeSemi},
				exp:   []sql.Row{{1, 0}},
			},
			{
				q:     "select * from xy where x in (select * from (select 1) r);",
				types: []plan.JoinType{plan.JoinTypeSemi},
				exp:   []sql.Row{{1, 0}},
			},
			{
				q: `
with recursive rec(x) as (select 1 union select 1)
select * from xy where x in (
  select * from rec
);`,
				types: []plan.JoinType{plan.JoinTypeSemi},
				exp:   []sql.Row{{1, 0}},
			},
			{
				q: `
with recursive rec(x) as (
  select 1
  union
  select rec.x from rec join xy on rec.x = xy.y
)
select * from uv
where u in (select * from rec);`,
				types: []plan.JoinType{plan.JoinTypeSemi, plan.JoinTypeInner},
				exp:   []sql.Row{{1, 1}},
			},
			{
				q:     "select x+1 as newX, y from xy having y in (select x from xy where newX=1)",
				types: []plan.JoinType{},
				exp:   []sql.Row{{1, 2}},
			},
			{
				q:     "select x, x+1 as newX from xy having x in (select * from (select 1 where 1 in (select 1 where newX != 1)) r where x = 1);",
				types: []plan.JoinType{},
				exp:   []sql.Row{{1, 2}},
			},
			{
				q:   "select * from uv where not exists (select * from xy where u = 1)",
				exp: []sql.Row{{0, 1}, {2, 2}, {3, 2}},
			},
			{
				q:   "select * from uv where not exists (select * from xy where not exists (select * from xy where u = 1))",
				exp: []sql.Row{{1, 1}},
			},
			{
				q:   "select * from uv where not exists (select * from xy where not exists (select * from xy where u = 1 or v = 2))",
				exp: []sql.Row{{1, 1}, {2, 2}, {3, 2}},
			},
			{
				q:   "select * from uv where not exists (select * from xy where v = 1 and not exists (select * from xy where u = 1))",
				exp: []sql.Row{{1, 1}, {2, 2}, {3, 2}},
			},
			{
				q:   "select * from uv where not exists (select * from xy where not exists (select * from xy where not(u = 1)))",
				exp: []sql.Row{{0, 1}, {2, 2}, {3, 2}},
			},
		},
	},
	{
		name: "convert semi to inner join",
		setup: []string{
			"CREATE table xy (x int, y int, primary key(x,y));",
			"CREATE table uv (u int primary key, v int);",
			"CREATE table ab (a int primary key, b int);",
			"insert into xy values (1,0), (2,1), (0,2), (3,3);",
			"insert into uv values (0,1), (1,1), (2,2), (3,2);",
			"insert into ab values (0,2), (1,2), (2,2), (3,1);",
			`analyze table xy update histogram on x using data '{"row_count":100}'`,
			`analyze table uv update histogram on u using data '{"row_count":100}'`,
			`analyze table ab update histogram on a using data '{"row_count":100}'`,
		},
		tests: []JoinPlanTest{
			{
				q:     "select * from xy where x in (select u from uv join ab on u = a and a = 2) order by 1;",
				types: []plan.JoinType{plan.JoinTypeInner, plan.JoinTypeLookup},
				exp:   []sql.Row{{2, 1}},
			},
			{
				q: `select x from xy where x in (
	select (select u from uv where u = sq.a)
    from (select a from ab) sq);`,
				types: []plan.JoinType{},
				exp:   []sql.Row{{0}, {1}, {2}, {3}},
			},
			{
				q:     "select * /*+ LOOKUP_JOIN(xy,uv) */ from xy where y >= (select u from uv where u = 2) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemi},
				exp:   []sql.Row{{0, 2}, {3, 3}},
			},
			{
				q:     "select * /*+ LOOKUP_JOIN(xy,uv) */ from xy where x <= (select u from uv where u = 2) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemi},
				exp:   []sql.Row{{0, 2}, {1, 0}, {2, 1}},
			},
			{
				q:     "select /*+ LOOKUP_JOIN(xy,uv) */ * from xy where x < (select u from uv where u = 2) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemiLookup},
				exp:   []sql.Row{{0, 2}, {1, 0}},
			},
			{
				q:     "select /*+ LOOKUP_JOIN(xy,uv) */ * from xy where x > (select u from uv where u = 2) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemiLookup},
				exp:   []sql.Row{{3, 3}},
			},
			{
				q:     "select /*+ LOOKUP_JOIN(uv, uv_1) */ * from uv where v <=> (select u from uv where u = 2) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemiLookup},
				exp:   []sql.Row{{2, 2}, {3, 2}},
			},
		},
	},
	{
		name: "convert anti to left join",
		setup: []string{
			"CREATE table xy (x int, y int, primary key(x,y));",
			"CREATE table uv (u int primary key, v int);",
			"create table empty_tbl (a int, b int);",
			"insert into xy values (1,0), (2,1), (0,2), (3,3);",
			"insert into uv values (0,1), (1,1), (2,2), (3,2);",
		},
		// write a bunch of left joins and make sure they are converted to anti joins
		tests: []JoinPlanTest{
			{
				q:     "select /*+ HASH_JOIN(xy,empty_tbl) */ * from xy where x not in (select a from empty_tbl) order by x",
				types: []plan.JoinType{plan.JoinTypeLeftOuterHashExcludeNulls},
				exp: []sql.Row{
					{0, 2},
					{1, 0},
					{2, 1},
					{3, 3},
				},
			},
			{
				q:     "select /*+ HASH_JOIN(xy,uv) */ * from xy where x not in (select v from uv) order by x",
				types: []plan.JoinType{plan.JoinTypeLeftOuterHashExcludeNulls},
				exp: []sql.Row{
					{0, 2},
					{3, 3},
				},
			},
			{
				q:     "select /*+ HASH_JOIN(xy,uv) */ * from xy where x not in (select v from uv where u = 2) order by x",
				types: []plan.JoinType{plan.JoinTypeLeftOuterHashExcludeNulls},
				exp: []sql.Row{
					{0, 2},
					{1, 0},
					{3, 3},
				},
			},
			{
				q:     "select /*+ HASH_JOIN(xy,uv) */ * from xy where x != (select v from uv where u = 2) order by x",
				types: []plan.JoinType{plan.JoinTypeLeftOuterHashExcludeNulls},
				exp: []sql.Row{
					{0, 2},
					{1, 0},
					{3, 3},
				},
			},
			{
				q:     "select * from xy where not exists (select * from empty_tbl) order by x",
				types: []plan.JoinType{plan.JoinTypeLeftOuter},
				exp: []sql.Row{
					{0, 2},
					{1, 0},
					{2, 1},
					{3, 3},
				},
			},
			{
				q:     "select * from xy where not exists (select * from empty_tbl) and x is not null order by x",
				types: []plan.JoinType{plan.JoinTypeLeftOuter},
				exp: []sql.Row{
					{0, 2},
					{1, 0},
					{2, 1},
					{3, 3},
				},
			},
			{
				q:     "select /*+ MERGE_JOIN(xy,uv) */ * from xy where x not in (select u from uv WHERE u = 2) order by x",
				types: []plan.JoinType{plan.JoinTypeLeftOuterMerge},
				exp: []sql.Row{
					{0, 2},
					{1, 0},
					{3, 3},
				},
			},
			{
				q:     "select /*+ LEFT_OUTER_LOOKUP_JOIN(xy,uv) */ * from xy where x not in (select u from uv WHERE u = 2) order by x",
				types: []plan.JoinType{plan.JoinTypeLeftOuterLookup},
				exp: []sql.Row{
					{0, 2},
					{1, 0},
					{3, 3},
				},
			},
		},
	},
	{
		name: "join varchar and text columns",
		setup: []string{
			"CREATE table varchartable (pk int primary key, s varchar(20));",
			"CREATE table texttable (pk int primary key, t text);",
			"insert into varchartable values (1,'first'), (2,'second'), (3,'third');",
			"insert into texttable values (1,'first'), (2,'second'), (3,'third');",
		},
		// write a bunch of left joins and make sure they are converted to anti joins
		tests: []JoinPlanTest{
			{
				q:     "select /*+ HASH_JOIN(varchartable,texttable) */ * from varchartable where s in (select t from texttable) order by pk",
				types: []plan.JoinType{plan.JoinTypeHash},
				exp: []sql.Row{
					{1, "first"},
					{2, "second"},
					{3, "third"},
				},
			},
			{
				q:     "select /*+ HASH_JOIN(varchartable,texttable) */ * from varchartable where s not in (select t from texttable) order by pk",
				types: []plan.JoinType{plan.JoinTypeLeftOuterHashExcludeNulls},
				exp:   []sql.Row{},
			},
		},
	},
	{
		name: "join concat tests",
		setup: []string{
			"CREATE table xy (x int primary key, y int);",
			"CREATE table uv (u int primary key, v int);",
			"insert into xy values (1,0), (2,1), (0,2), (3,3);",
			"insert into uv values (0,1), (1,1), (2,2), (3,2);",
			`analyze table xy update histogram on x using data '{"row_count":100}'`,
			`analyze table uv update histogram on u using data '{"row_count":100}'`,
		},
		tests: []JoinPlanTest{
			{
				q:     "select x, u from xy inner join uv on u+1 = x OR u+2 = x OR u+3 = x;",
				types: []plan.JoinType{plan.JoinTypeLookup},
				exp:   []sql.Row{{3, 0}, {2, 0}, {1, 0}, {3, 1}, {2, 1}, {3, 2}},
			},
		},
	},
	{
		name: "join order hint",
		setup: []string{
			"CREATE table xy (x int primary key, y int);",
			"CREATE table uv (u int primary key, v int);",
			"insert into xy values (1,0), (2,1), (0,2), (3,3);",
			"insert into uv values (0,1), (1,1), (2,2), (3,2);",
			`analyze table xy update histogram on x using data '{"row_count":100}'`,
			`analyze table uv update histogram on u using data '{"row_count":100}'`,
		},
		tests: []JoinPlanTest{
			{
				q:     "select /*+ JOIN_ORDER(b, c, a) */ 1 from xy a join xy b on a.x+3 = b.x join xy c on a.x+3 = c.x and a.x+3 = b.x",
				order: [][]string{{"b", "c", "a"}},
			},
			{
				q:     "select /*+ JOIN_ORDER(a, c, b) */ 1 from xy a join xy b on a.x+3 = b.x join xy c on a.x+3 = c.x and a.x+3 = b.x",
				order: [][]string{{"a", "c", "b"}},
			},
			{
				q:     "select /*+ JOIN_ORDER(a,c,b) */ 1 from xy a join xy b on a.x+3 = b.x WHERE EXISTS (select 1 from uv c where c.u = a.x+2)",
				order: [][]string{{"a", "c", "b"}},
			},
			{
				q:     "select /*+ JOIN_ORDER(b,c,a) */ 1 from xy a join xy b on a.x+3 = b.x WHERE EXISTS (select 1 from uv c where c.u = a.x+2)",
				order: [][]string{{"b", "c", "a"}},
			},
			{
				q:     "select /*+ JOIN_ORDER(b,c,a) */ 1 from xy a join xy b on a.x+3 = b.x WHERE a.x in (select u from uv c)",
				order: [][]string{{"b", "c", "a"}},
			},
		},
	},
	{
		name: "join op hint",
		setup: []string{
			"CREATE table xy (x int primary key, y int);",
			"CREATE table uv (u int primary key, v int, key(v));",
			"insert into xy values (1,0), (2,1), (0,2), (3,3);",
			"insert into uv values (0,1), (1,1), (2,2), (3,2);",
		},
		tests: []JoinPlanTest{
			{
				q:     "select /*+ LOOKUP_JOIN(xy,uv) */ 1 from xy join uv on x = u",
				types: []plan.JoinType{plan.JoinTypeLookup},
			},
			{
				q:     "select /*+ MERGE_JOIN(xy,uv) */ 1 from xy join uv on x = u",
				types: []plan.JoinType{plan.JoinTypeMerge},
			},
			{
				q:     "select /*+ INNER_JOIN(xy,uv) */ 1 from xy join uv on x = u",
				types: []plan.JoinType{plan.JoinTypeInner},
			},
			{
				q:     "select /*+ HASH_JOIN(xy,uv) */ 1 from xy join uv on x = u",
				types: []plan.JoinType{plan.JoinTypeHash},
			},
			{
				q:     "select /*+ JOIN_ORDER(a,b,c) HASH_JOIN(a,b) HASH_JOIN(b,c) */ 1 from xy a join uv b on a.x = b.u join xy c on b.u = c.x",
				types: []plan.JoinType{plan.JoinTypeHash, plan.JoinTypeHash},
				order: [][]string{{"a", "b", "c"}},
			},
			{
				q:     "select /*+ JOIN_ORDER(b,c,a) LOOKUP_JOIN(b,a) HASH_JOIN(b,c) */ 1 from xy a join uv b on a.x = b.u join xy c on b.u = c.x",
				types: []plan.JoinType{plan.JoinTypeLookup, plan.JoinTypeHash},
			},
			{
				q:     "select /*+ LOOKUP_JOIN(b,a) MERGE_JOIN(b,c) */ 1 from xy a join uv b on a.x = b.u join xy c on b.u = c.x",
				types: []plan.JoinType{plan.JoinTypeLookup, plan.JoinTypeMerge},
			},
			{
				q:     "select /*+ JOIN_ORDER(b,c,a) LOOKUP_JOIN(b,a) MERGE_JOIN(b,c) */ 1 from xy a join uv b on a.x = b.u join xy c on b.u = c.x",
				types: []plan.JoinType{plan.JoinTypeLookup, plan.JoinTypeMerge},
				order: [][]string{{"b", "c", "a"}},
			},
			{
				q:     "select /*+ JOIN_ORDER(a,b,c) LOOKUP_JOIN(b,a) HASH_JOIN(b,c) */ 1 from xy a join uv b on a.x = b.u join xy c on b.u = c.x",
				types: []plan.JoinType{plan.JoinTypeHash, plan.JoinTypeLookup},
				order: [][]string{{"a", "b", "c"}},
			},
			{
				q:     "select /*+ JOIN_ORDER(c,a,b) MERGE_JOIN(a,b) HASH_JOIN(b,c) */ 1 from xy a join uv b on a.x = b.u join xy c on b.u = c.x",
				types: []plan.JoinType{plan.JoinTypeHash, plan.JoinTypeMerge},
				order: [][]string{{"c", "a", "b"}},
			},
			{
				q: `
select /*+ JOIN_ORDER(d,c,b,a) MERGE_JOIN(d,c) MERGE_JOIN(b,a) INNER_JOIN(c,a)*/ 1
from xy a
join uv b on a.x = b.u
join xy c on a.x = c.x
join uv d on d.u = c.x`,
				types: []plan.JoinType{plan.JoinTypeInner, plan.JoinTypeMerge, plan.JoinTypeMerge},
				order: [][]string{{"d", "c", "b", "a"}},
			},
			{
				q: `
select /*+ JOIN_ORDER(a,b,c,d) LOOKUP_JOIN(d,c) MERGE_JOIN(b,a) HASH_JOIN(c,a)*/ 1
from xy a
join uv b on a.x = b.u
join xy c on a.x = c.x
join uv d on d.u = c.x`,
				types: []plan.JoinType{plan.JoinTypeLookup, plan.JoinTypeHash, plan.JoinTypeMerge},
				order: [][]string{{"a", "b", "c", "d"}},
			},
			{
				q: "select /*+ LOOKUP_JOIN(xy,uv) */ 1 from xy where x not in (select u from uv)",
				// This should be JoinTypeSemiLookup, but for https://github.com/dolthub/go-mysql-server/issues/1894
				types: []plan.JoinType{plan.JoinTypeLeftOuterLookup},
			},
			{
				q:     "select /*+ ANTI_JOIN(xy,uv) */ 1 from xy where x not in (select u from uv)",
				types: []plan.JoinType{plan.JoinTypeAnti},
			},
			{
				q:     "select /*+ LOOKUP_JOIN(xy,uv) */ 1 from xy where x in (select u from uv)",
				types: []plan.JoinType{plan.JoinTypeSemiLookup},
			},
			{
				q:     "select /*+ SEMI_JOIN(xy,uv) */ 1 from xy where x in (select u from uv)",
				types: []plan.JoinType{plan.JoinTypeSemi},
			},
			{
				q:     "select /*+ LOOKUP_JOIN(s,uv) */ 1 from xy s where x in (select u from uv)",
				types: []plan.JoinType{plan.JoinTypeSemiLookup},
			},
			{
				q:     "select /*+ SEMI_JOIN(s,uv) */ 1 from xy s where x in (select u from uv)",
				types: []plan.JoinType{plan.JoinTypeSemi},
			},
		},
	},
	{
		// This is a regression test for https://github.com/dolthub/go-mysql-server/pull/1889.
		// We should always prefer a more specific index over a less specific index for lookups.
		name: "lookup join multiple indexes",
		setup: []string{
			"create table lhs (a int, b int, c int);",
			"create table rhs (a int, b int, c int, d int, index a_idx(a), index abcd_idx(a,b,c,d));",
			"insert into lhs values (0, 0, 0), (0, 0, 1), (0, 1, 1), (1, 1, 1);",
			"insert into rhs values " +
				"(0, 0, 0, 0)," +
				"(0, 0, 0, 1)," +
				"(0, 0, 1, 0)," +
				"(0, 0, 1, 1)," +
				"(0, 1, 0, 0)," +
				"(0, 1, 0, 1)," +
				"(0, 1, 1, 0)," +
				"(0, 1, 1, 1)," +
				"(1, 0, 0, 0)," +
				"(1, 0, 0, 1)," +
				"(1, 0, 1, 0)," +
				"(1, 0, 1, 1)," +
				"(1, 1, 0, 0)," +
				"(1, 1, 0, 1)," +
				"(1, 1, 1, 0)," +
				"(1, 1, 1, 1);",
		},
		tests: []JoinPlanTest{
			{
				q:       "select /*+ LOOKUP_JOIN(lhs, rhs) */ rhs.* from lhs left join rhs on lhs.a = rhs.a and lhs.b = rhs.b and lhs.c = rhs.c",
				types:   []plan.JoinType{plan.JoinTypeLeftOuterLookup},
				indexes: []string{"abcd_idx"},
				exp: []sql.Row{
					{0, 0, 0, 0},
					{0, 0, 0, 1},
					{0, 0, 1, 0},
					{0, 0, 1, 1},
					{0, 1, 1, 0},
					{0, 1, 1, 1},
					{1, 1, 1, 0},
					{1, 1, 1, 1},
				},
			},
		},
	},
	{
		name: "indexed range join",
		setup: []string{
			"create table vals (val int unique key);",
			"create table ranges (min int unique key, max int, unique key(min,max));",
			"insert into vals values (null), (0), (1), (2), (3), (4), (5), (6);",
			"insert into ranges values (null,1), (0,2), (1,3), (2,4), (3,5), (4,6);",
		},
		tests: []JoinPlanTest{
			{
				q:     "select * from vals join ranges on val between min and max",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
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
				q:     "select * from vals join ranges on val > min and val < max",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
					{1, 0, 2},
					{2, 1, 3},
					{3, 2, 4},
					{4, 3, 5},
					{5, 4, 6},
				},
			},
			{
				q:     "select * from vals join ranges on val >= min and val < max",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
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
				q:     "select * from vals join ranges on val > min and val <= max",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
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
				q:     "select * from vals join ranges on val >= min and val <= max",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
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
				q:     "select * from vals join ranges on val >= min and val <= max where min >= 2",
				types: []plan.JoinType{plan.JoinTypeInner},
				exp: []sql.Row{
					{2, 2, 4},
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
				q:     "select * from vals join ranges on val between min and max where min >= 2 and max <= 5",
				types: []plan.JoinType{plan.JoinTypeInner},
				exp: []sql.Row{
					{2, 2, 4},
					{3, 2, 4},
					{3, 3, 5},
					{4, 2, 4},
					{4, 3, 5},
					{5, 3, 5},
				},
			},
			{
				q:     "select * from vals join (select max, min from ranges) ranges on val between min and max where min >= 2 and max <= 5",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
					{2, 4, 2},
					{3, 4, 2},
					{3, 5, 3},
					{4, 4, 2},
					{4, 5, 3},
					{5, 5, 3},
				},
			},
			{
				q:     "select * from vals join (select * from ranges where min >= 2 and max <= 5) ranges on val between min and max",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
					{2, 2, 4},
					{3, 2, 4},
					{3, 3, 5},
					{4, 2, 4},
					{4, 3, 5},
					{5, 3, 5},
				},
			},
			{
				q:     "select * from vals join (select * from ranges where min >= 2 and max <= 5 limit 1) ranges on val between min and max",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
					{2, 2, 4},
					{3, 2, 4},
					{4, 2, 4},
				},
			},
			{
				q:     "select * from vals join (select * from ranges where min >= 2 and max <= 5) ranges on val between min and max limit 1",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
					{2, 2, 4},
				},
			},
			{
				q:     "select * from vals join (select * from ranges where min >= 2 and max <= 5 order by min, max asc) ranges on val between min and max",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
					{2, 2, 4},
					{3, 2, 4},
					{3, 3, 5},
					{4, 2, 4},
					{4, 3, 5},
					{5, 3, 5},
				},
			},
			{
				q:     "select * from vals join (select distinct * from ranges where min >= 2 and max <= 5) ranges on val between min and max",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
					{2, 2, 4},
					{3, 2, 4},
					{4, 2, 4},
					{3, 3, 5},
					{4, 3, 5},
					{5, 3, 5},
				},
			},
			{
				q:     "select * from vals where exists (select * from vals join ranges on val between min and max where min >= 2 and max <= 5)",
				types: []plan.JoinType{plan.JoinTypeCross, plan.JoinTypeInner},
				exp: []sql.Row{
					{nil},
					{0},
					{1},
					{2},
					{3},
					{4},
					{5},
					{6},
				},
			},
			{
				q:     "select * from vals where exists (select * from ranges where val between min and max limit 1);",
				types: []plan.JoinType{plan.JoinTypeSemi},
				exp: []sql.Row{
					{0},
					{1},
					{2},
					{3},
					{4},
					{5},
					{6},
				},
			},
			{
				q:     "select * from vals where exists (select distinct val from ranges where val between min and max);",
				types: []plan.JoinType{plan.JoinTypeSemi},
				exp: []sql.Row{
					{0},
					{1},
					{2},
					{3},
					{4},
					{5},
					{6},
				},
			},
			{
				q:     "select * from vals where exists (select * from ranges where val between min and max order by 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemi},
				exp: []sql.Row{
					{0},
					{1},
					{2},
					{3},
					{4},
					{5},
					{6},
				},
			},
			{
				q:     "select * from vals where exists (select * from ranges where val between min and max limit 1 offset 1);",
				types: []plan.JoinType{}, // This expression cannot be optimized into a join.
				exp: []sql.Row{
					{1},
					{2},
					{3},
					{4},
					{5},
				},
			},
			{
				q:     "select * from vals where exists (select * from ranges where val between min and max having val > 1);",
				types: []plan.JoinType{},
				exp: []sql.Row{
					{2},
					{3},
					{4},
					{5},
					{6},
				},
			},
		},
	},
	{
		name: "keyless range join",
		setup: []string{
			"create table vals (val int)",
			"create table ranges (min int, max int)",
			"insert into vals values (null), (0), (1), (2), (3), (4), (5), (6)",
			"insert into ranges values (null,1), (0,2), (1,3), (2,4), (3,5), (4,6)",
		},
		tests: []JoinPlanTest{
			{
				q:     "select * from vals join ranges on val between min and max",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
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
				q:     "select * from vals join ranges on val > min and val < max",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
					{1, 0, 2},
					{2, 1, 3},
					{3, 2, 4},
					{4, 3, 5},
					{5, 4, 6},
				},
			},
			{
				q:     "select * from vals join ranges on min < val and max > val",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
					{1, 0, 2},
					{2, 1, 3},
					{3, 2, 4},
					{4, 3, 5},
					{5, 4, 6},
				},
			},
			{
				q:     "select * from vals join ranges on val >= min and val < max",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
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
				q:     "select * from vals join ranges on val > min and val <= max",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
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
				q:     "select * from vals join ranges on val >= min and val <= max",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
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
				q:     "select * from vals left join ranges on val > min and val < max",
				types: []plan.JoinType{plan.JoinTypeLeftOuterRangeHeap},
				exp: []sql.Row{
					{nil, nil, nil},
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
				q:     "select * from ranges l join ranges r on l.min > r.min and l.min < r.max",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
					{1, 3, 0, 2},
					{2, 4, 1, 3},
					{3, 5, 2, 4},
					{4, 6, 3, 5},
				},
			},
			{
				q:     "select * from vals left join ranges r1 on val > r1.min and val < r1.max left join ranges r2 on r1.min > r2.min and r1.min < r2.max",
				types: []plan.JoinType{plan.JoinTypeLeftOuterRangeHeap, plan.JoinTypeLeftOuterRangeHeap},
				exp: []sql.Row{
					{nil, nil, nil, nil, nil},
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
				q:     "select * from (select vals.val * 2 as val from vals) as newVals join (select ranges.min * 2 as min, ranges.max * 2 as max from ranges) as newRanges on val > min and val < max;",
				types: []plan.JoinType{plan.JoinTypeRangeHeap},
				exp: []sql.Row{
					{2, 0, 4},
					{4, 2, 6},
					{6, 4, 8},
					{8, 6, 10},
					{10, 8, 12},
				},
			},
			{
				// This tests that the RangeHeapJoin node functions correctly even if its rows are iterated over multiple times.
				q:     "select * from (select 1 union select 2) as l left join (select * from vals join ranges on val > min and val < max) as r on max = max",
				types: []plan.JoinType{plan.JoinTypeLeftOuter, plan.JoinTypeRangeHeap},
				exp: []sql.Row{
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
				q:     "select * from vals left join (select * from ranges where 0) as newRanges on val > min and val < max;",
				types: []plan.JoinType{plan.JoinTypeLeftOuterRangeHeap},
				exp: []sql.Row{
					{nil, nil, nil},
					{0, nil, nil},
					{1, nil, nil},
					{2, nil, nil},
					{3, nil, nil},
					{4, nil, nil},
					{5, nil, nil},
					{6, nil, nil},
				},
			},
		},
	},
	{
		name: "range join vs good lookup join regression test",
		setup: []string{
			"create table vals (val int, filter1 int, filter2 int, filter3 int)",
			"create table ranges (min int, max int, filter1 int, filter2 int, filter3 int, key filters (filter1, filter2, filter3))",
			"insert into vals values (0, 0, 0, 0), " +
				"(1, 0, 0, 0), " +
				"(2, 0, 0, 0), " +
				"(3, 0, 0, 0), " +
				"(4, 0, 0, 0), " +
				"(5, 0, 0, 0), " +
				"(6, 0, 0, 0), " +
				"(0, 0, 0, 1), " +
				"(1, 0, 0, 1), " +
				"(2, 0, 0, 1), " +
				"(3, 0, 0, 1), " +
				"(4, 0, 0, 1), " +
				"(5, 0, 0, 1), " +
				"(6, 0, 0, 1), " +
				"(0, 0, 1, 0), " +
				"(1, 0, 1, 0), " +
				"(2, 0, 1, 0), " +
				"(3, 0, 1, 0), " +
				"(4, 0, 1, 0), " +
				"(5, 0, 1, 0), " +
				"(6, 0, 1, 0), " +
				"(0, 0, 1, 1), " +
				"(1, 0, 1, 1), " +
				"(2, 0, 1, 1), " +
				"(3, 0, 1, 1), " +
				"(4, 0, 1, 1), " +
				"(5, 0, 1, 1), " +
				"(6, 0, 1, 1), " +
				"(0, 1, 0, 0), " +
				"(1, 1, 0, 0), " +
				"(2, 1, 0, 0), " +
				"(3, 1, 0, 0), " +
				"(4, 1, 0, 0), " +
				"(5, 1, 0, 0), " +
				"(6, 1, 0, 0), " +
				"(0, 1, 0, 1), " +
				"(1, 1, 0, 1), " +
				"(2, 1, 0, 1), " +
				"(3, 1, 0, 1), " +
				"(4, 1, 0, 1), " +
				"(5, 1, 0, 1), " +
				"(6, 1, 0, 1), " +
				"(0, 1, 1, 0), " +
				"(1, 1, 1, 0), " +
				"(2, 1, 1, 0), " +
				"(3, 1, 1, 0), " +
				"(4, 1, 1, 0), " +
				"(5, 1, 1, 0), " +
				"(6, 1, 1, 0), " +
				"(0, 1, 1, 1), " +
				"(1, 1, 1, 1), " +
				"(2, 1, 1, 1), " +
				"(3, 1, 1, 1), " +
				"(4, 1, 1, 1), " +
				"(5, 1, 1, 1), " +
				"(6, 1, 1, 1);",
			"insert into ranges values " +
				"(0, 2, 0, 0, 0), " +
				"(1, 3, 0, 0, 0), " +
				"(2, 4, 0, 0, 0), " +
				"(3, 5, 0, 0, 0), " +
				"(4, 6, 0, 0, 0), " +
				"(0, 2, 0, 0, 1), " +
				"(1, 3, 0, 0, 1), " +
				"(2, 4, 0, 0, 1), " +
				"(3, 5, 0, 0, 1), " +
				"(4, 6, 0, 0, 1), " +
				"(0, 2, 0, 1, 0), " +
				"(1, 3, 0, 1, 0), " +
				"(2, 4, 0, 1, 0), " +
				"(3, 5, 0, 1, 0), " +
				"(4, 6, 0, 1, 0), " +
				"(0, 2, 0, 1, 1), " +
				"(1, 3, 0, 1, 1), " +
				"(2, 4, 0, 1, 1), " +
				"(3, 5, 0, 1, 1), " +
				"(4, 6, 0, 1, 1), " +
				"(0, 2, 1, 0, 0), " +
				"(1, 3, 1, 0, 0), " +
				"(2, 4, 1, 0, 0), " +
				"(3, 5, 1, 0, 0), " +
				"(4, 6, 1, 0, 0), " +
				"(0, 2, 1, 0, 1), " +
				"(1, 3, 1, 0, 1), " +
				"(2, 4, 1, 0, 1), " +
				"(3, 5, 1, 0, 1), " +
				"(4, 6, 1, 0, 1), " +
				"(0, 2, 1, 1, 0), " +
				"(1, 3, 1, 1, 0), " +
				"(2, 4, 1, 1, 0), " +
				"(3, 5, 1, 1, 0), " +
				"(4, 6, 1, 1, 0), " +
				"(0, 2, 1, 1, 1), " +
				"(1, 3, 1, 1, 1), " +
				"(2, 4, 1, 1, 1), " +
				"(3, 5, 1, 1, 1), " +
				"(4, 6, 1, 1, 1); ",
		},
		tests: []JoinPlanTest{
			{
				// Test that a RangeHeapJoin won't be chosen over a LookupJoin with a multiple-column index.
				q:     "select val, min, max, vals.filter1, vals.filter2, vals.filter3 from vals join ranges on val > min and val < max and vals.filter1 = ranges.filter1 and vals.filter2 = ranges.filter2 and vals.filter3 = ranges.filter3",
				types: []plan.JoinType{plan.JoinTypeLookup},
				exp: []sql.Row{
					{1, 0, 2, 0, 0, 0},
					{2, 1, 3, 0, 0, 0},
					{3, 2, 4, 0, 0, 0},
					{4, 3, 5, 0, 0, 0},
					{5, 4, 6, 0, 0, 0},
					{1, 0, 2, 0, 0, 1},
					{2, 1, 3, 0, 0, 1},
					{3, 2, 4, 0, 0, 1},
					{4, 3, 5, 0, 0, 1},
					{5, 4, 6, 0, 0, 1},
					{1, 0, 2, 0, 1, 0},
					{2, 1, 3, 0, 1, 0},
					{3, 2, 4, 0, 1, 0},
					{4, 3, 5, 0, 1, 0},
					{5, 4, 6, 0, 1, 0},
					{1, 0, 2, 0, 1, 1},
					{2, 1, 3, 0, 1, 1},
					{3, 2, 4, 0, 1, 1},
					{4, 3, 5, 0, 1, 1},
					{5, 4, 6, 0, 1, 1},
					{1, 0, 2, 1, 0, 0},
					{2, 1, 3, 1, 0, 0},
					{3, 2, 4, 1, 0, 0},
					{4, 3, 5, 1, 0, 0},
					{5, 4, 6, 1, 0, 0},
					{1, 0, 2, 1, 0, 1},
					{2, 1, 3, 1, 0, 1},
					{3, 2, 4, 1, 0, 1},
					{4, 3, 5, 1, 0, 1},
					{5, 4, 6, 1, 0, 1},
					{1, 0, 2, 1, 1, 0},
					{2, 1, 3, 1, 1, 0},
					{3, 2, 4, 1, 1, 0},
					{4, 3, 5, 1, 1, 0},
					{5, 4, 6, 1, 1, 0},
					{1, 0, 2, 1, 1, 1},
					{2, 1, 3, 1, 1, 1},
					{3, 2, 4, 1, 1, 1},
					{4, 3, 5, 1, 1, 1},
					{5, 4, 6, 1, 1, 1},
				},
			},
		},
	},
	{
		name: "straight_join is inner join",
		setup: []string{
			"create table t1 (i int)",
			"create table t2 (j int)",
			"insert into t1 values (1), (2), (3)",
			"insert into t2 values (2), (3), (4)",
		},
		tests: []JoinPlanTest{
			{
				q:     "select * from t1 straight_join t2 on i = j",
				types: []plan.JoinType{plan.JoinTypeInner},
				exp: []sql.Row{
					{2, 2},
					{3, 3},
				},
			},
		},
	},
}

func TestJoinPlanning(t *testing.T, harness Harness) {
	runJoinPlanningTests(t, harness, JoinPlanningTests)
}

func runJoinPlanningTests(t *testing.T, harness Harness, tests []joinPlanScript) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if sh, ok := harness.(SkippingHarness); ok {
				if sh.SkipQueryTest(tt.name) {
					t.Skip(tt.name)
				}
			}
			harness.Setup([]setup.SetupScript{setup.MydbData[0], tt.setup})
			e := mustNewEngine(t, harness)
			defer e.Close()
			for _, tt := range tt.tests {
				if tt.types != nil {
					evalJoinTypeTest(t, harness, e, tt.q, tt.types, tt.skipOld)
				}
				if tt.indexes != nil {
					evalIndexTest(t, harness, e, tt.q, tt.indexes, tt.skipOld)
				}
				if tt.mergeCompares != nil {
					evalMergeCmpTest(t, harness, e, tt)
				}
				if tt.exp != nil {
					evalJoinCorrectness(t, harness, e, tt.q, tt.q, tt.exp, tt.skipOld)
				}
				if tt.order != nil {
					evalJoinOrder(t, harness, e, tt.q, tt.order, tt.skipOld)
				}
			}
		})
	}
}
func evalJoinTypeTest(t *testing.T, harness Harness, e QueryEngine, query string, types []plan.JoinType, skipOld bool) {
	t.Run(query+" join types", func(t *testing.T) {
		if skipOld {
			t.Skip()
		}

		ctx := NewContext(harness)
		ctx = ctx.WithQuery(query)

		a, err := analyzeQuery(ctx, e, query)
		require.NoError(t, err)

		jts := collectJoinTypes(a)
		var exp []string
		for _, t := range types {
			exp = append(exp, t.String())
		}
		var cmp []string
		for _, t := range jts {
			cmp = append(cmp, t.String())
		}
		require.Equal(t, exp, cmp, fmt.Sprintf("unexpected plan:\n%s", sql.DebugString(a)))
	})
}

func analyzeQuery(ctx *sql.Context, e QueryEngine, query string) (sql.Node, error) {
	parsed, qFlags, err := planbuilder.Parse(ctx, e.EngineAnalyzer().Catalog, query)
	if err != nil {
		return nil, err
	}

	return e.EngineAnalyzer().Analyze(ctx, parsed, nil, qFlags)
}

func evalMergeCmpTest(t *testing.T, harness Harness, e QueryEngine, tt JoinPlanTest) {
	hasMergeJoin := false
	for _, joinType := range tt.types {
		if joinType.IsMerge() {
			hasMergeJoin = true
		}
	}
	if !hasMergeJoin {
		return
	}
	t.Run(tt.q+"merge join compare", func(t *testing.T) {
		if tt.skipOld {
			t.Skip()
		}

		ctx := NewContext(harness)
		ctx = ctx.WithQuery(tt.q)

		a, err := analyzeQuery(ctx, e, tt.q)
		require.NoError(t, err)

		// consider making this a string too
		compares := collectMergeCompares(a)
		var cmp []string
		for _, i := range compares {
			cmp = append(cmp, i.String())
		}
		require.Equal(t, tt.mergeCompares, cmp, fmt.Sprintf("unexpected plan:\n%s", sql.DebugString(a)))
	})
}

func evalIndexTest(t *testing.T, harness Harness, e QueryEngine, q string, indexes []string, skip bool) {
	t.Run(q+" join indexes", func(t *testing.T) {
		if skip {
			t.Skip()
		}

		ctx := NewContext(harness)
		ctx = ctx.WithQuery(q)

		a, err := analyzeQuery(ctx, e, q)
		require.NoError(t, err)

		idxs := collectIndexes(a)
		var exp []string
		for _, i := range indexes {
			exp = append(exp, i)
		}
		var cmp []string
		for _, i := range idxs {
			cmp = append(cmp, strings.ToLower(i.ID()))
		}
		require.Equal(t, exp, cmp, fmt.Sprintf("unexpected plan:\n%s", sql.DebugString(a)))
	})
}

func evalJoinCorrectness(t *testing.T, harness Harness, e QueryEngine, name, q string, exp []sql.Row, skipOld bool) {
	t.Run(name, func(t *testing.T) {
		ctx := NewContext(harness)
		ctx = ctx.WithQuery(q)

		sch, iter, _, err := e.QueryWithBindings(ctx, q, nil, nil, nil)
		require.NoError(t, err, "Unexpected error for query %s: %s", q, err)

		rows, err := sql.RowIterToRows(ctx, iter)
		require.NoError(t, err, "Unexpected error for query %s: %s", q, err)

		if exp != nil {
			CheckResults(ctx, t, harness, exp, nil, sch, rows, q, e)
		}

		require.Equal(t, 0, ctx.Memory.NumCaches())
		validateEngine(t, ctx, harness, e)
	})
}

func collectJoinTypes(n sql.Node) []plan.JoinType {
	var types []plan.JoinType
	transform.Inspect(n, func(n sql.Node) bool {
		if n == nil {
			return true
		}
		j, ok := n.(*plan.JoinNode)
		if ok {
			types = append(types, j.Op)
		}

		if ex, ok := n.(sql.Expressioner); ok {
			for _, e := range ex.Expressions() {
				transform.InspectExpr(e, func(e sql.Expression) bool {
					sq, ok := e.(*plan.Subquery)
					if !ok {
						return false
					}
					types = append(types, collectJoinTypes(sq.Query)...)
					return false
				})
			}
		}
		return true
	})
	return types
}

func collectMergeCompares(n sql.Node) []sql.Expression {
	var compares []sql.Expression
	transform.Inspect(n, func(n sql.Node) bool {
		if n == nil {
			return true
		}

		if ex, ok := n.(sql.Expressioner); ok {
			for _, e := range ex.Expressions() {
				transform.InspectExpr(e, func(e sql.Expression) bool {
					sq, ok := e.(*plan.Subquery)
					if !ok {
						return false
					}
					compares = append(compares, collectMergeCompares(sq.Query)...)
					return false
				})
			}
		}

		join, ok := n.(*plan.JoinNode)
		if !ok {
			return true
		}
		if !join.Op.IsMerge() {
			return true
		}

		compares = append(compares, expression.SplitConjunction(join.JoinCond())[0])
		return true
	})
	return compares
}

func collectIndexes(n sql.Node) []sql.Index {
	var indexes []sql.Index
	transform.Inspect(n, func(n sql.Node) bool {
		if n == nil {
			return true
		}
		access, ok := n.(*plan.IndexedTableAccess)
		if ok {
			indexes = append(indexes, access.Index())
			return true
		}

		if ex, ok := n.(sql.Expressioner); ok {
			for _, e := range ex.Expressions() {
				transform.InspectExpr(e, func(e sql.Expression) bool {
					sq, ok := e.(*plan.Subquery)
					if !ok {
						return false
					}
					indexes = append(indexes, collectIndexes(sq.Query)...)
					return false
				})
			}
		}
		return true
	})
	return indexes
}

func evalJoinOrder(t *testing.T, harness Harness, e QueryEngine, q string, exp [][]string, skipOld bool) {
	t.Run(q+" join order", func(t *testing.T) {
		ctx := NewContext(harness)
		ctx = ctx.WithQuery(q)

		a, err := analyzeQuery(ctx, e, q)
		require.NoError(t, err)

		cmp := collectJoinOrder(a)
		for _, expCand := range exp {
			if assert.ObjectsAreEqual(expCand, cmp) {
				return
			}
		}
		assert.Failf(t, "expected order %s found '%s'\ndetail:\n%s", fmt.Sprintf("%#v", exp), strings.Join(cmp, ","), sql.DebugString(a))
	})
}

func collectJoinOrder(n sql.Node) []string {
	order := []string{}

	switch n := n.(type) {
	case *plan.JoinNode:
		order = append(order, collectJoinOrder(n.Left())...)
		order = append(order, collectJoinOrder(n.Right())...)
	case plan.TableIdNode:
		order = append(order, n.Name())
	default:
		children := n.Children()
		for _, c := range children {
			order = append(order, collectJoinOrder(c)...)
		}
	}

	return order
}
