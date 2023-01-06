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
	"testing"

	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

type JoinOpTest struct {
	q     string
	types []plan.JoinType
	exp   []sql.Row
	skip  bool
}

var JoinOpTests = []struct {
	name  string
	setup []string
	tests []JoinOpTest
}{
	{
		name: "merge join unary index",
		setup: []string{
			"CREATE table xy (x int primary key, y int, index y_idx(y));",
			"create table rs (r int primary key, s int, index s_idx(s));",
			"CREATE table uv (u int primary key, v int);",
			"CREATE table ab (a int primary key, b int);",
			"insert into xy values (1,0), (2,1), (0,2), (3,3);",
			"insert into rs values (0,0), (1,0), (2,0), (4,4), (5,4);",
			"insert into uv values (0,1), (1,1), (2,2), (3,2);",
			"insert into ab values (0,2), (1,2), (2,2), (3,1);",
			"update information_schema.statistics set cardinality = 1000000000 where table_name = 'rs';",
			"update information_schema.statistics set cardinality = 1000000000 where table_name = 'ab'",
		},
		tests: []JoinOpTest{
			{
				q:     "select u,a,y from uv join (select /*+ JOIN_ORDER(ab, xy) */ * from ab join xy on y = a) r on u = r.a order by 1",
				types: []plan.JoinType{plan.JoinTypeLookup, plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 0, 0}, {1, 1, 1}, {2, 2, 2}, {3, 3, 3}},
			},
			{
				q:     "select /*+ JOIN_ORDER(ab, xy) */ * from ab join xy on y = a order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 2, 1, 0}, {1, 2, 2, 1}, {2, 2, 0, 2}, {3, 1, 3, 3}},
			},
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) */ * from rs left join xy on y = s order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeLeftOuterMerge},
				exp:   []sql.Row{{0, 0, 1, 0}, {1, 0, 1, 0}, {2, 0, 1, 0}, {4, 4, nil, nil}, {5, 4, nil, nil}},
			},
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on y = r order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 0, 1, 0}, {1, 0, 2, 1}, {2, 0, 0, 2}},
			},
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on r = y order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 0, 1, 0}, {1, 0, 2, 1}, {2, 0, 0, 2}},
			},
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on y = s order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 0, 1, 0}, {1, 0, 1, 0}, {2, 0, 1, 0}},
			},
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on y+10 = s order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeHash},
				exp:   []sql.Row{},
			},
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on 10 = s+y order by 1, 3",
				types: []plan.JoinType{plan.JoinTypeInner},
				exp:   []sql.Row{},
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
			"update information_schema.statistics set cardinality = 1000000000 where table_name = 'rs';",
		},
		tests: []JoinOpTest{
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on y = s order by 1,3",
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
			"update information_schema.statistics set cardinality = 10 where table_name = 'xy';",
			"update information_schema.statistics set cardinality = 1000000000 where table_name = 'rs';",
		},
		tests: []JoinOpTest{
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on y = s order by 1,3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{},
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
			"update information_schema.statistics set cardinality = 1000000000 where table_name = 'rs';",
		},
		tests: []JoinOpTest{
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on y = s order by 1,3",
				types: []plan.JoinType{plan.JoinTypeMerge},
				exp:   []sql.Row{{0, 0, 1, 0}, {0, 0, 4, 0}, {3, 0, 1, 0}, {3, 0, 4, 0}, {4, 8, 0, 8}, {5, 4, 5, 4}},
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
			"update information_schema.statistics set cardinality = 1000000000 where table_name = 'rs';",
		},
		tests: []JoinOpTest{
			{
				q:     "select /*+ JOIN_ORDER(rs, xy) */ * from rs join xy on y = s order by 1,3",
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
		},
		tests: []JoinOpTest{
			{
				q:     "select * from xy where y+1 not in (select u from uv);",
				types: []plan.JoinType{plan.JoinTypeAntiLookup},
				exp:   []sql.Row{{3, 3}},
			},
			{
				q:     "select * from xy where x not in (select u from uv where u not in (select a from ab where a not in (select r from rs where r = 1))) order by 1;",
				types: []plan.JoinType{plan.JoinTypeAnti, plan.JoinTypeAnti, plan.JoinTypeAnti},
				exp:   []sql.Row{{0, 2}, {2, 1}, {3, 3}},
			},
			{
				q:     "select * from xy where x != (select r from rs where r = 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeAnti},
				exp:   []sql.Row{{0, 2}, {2, 1}, {3, 3}},
			},
			{
				// anti join will be cross-join-right, be passed non-nil parent row
				q:     "select x,a from ab, (select * from xy where x != (select r from rs where r = 1) order by 1) sq where x = 2 and b = 2 order by 1,2;",
				types: []plan.JoinType{plan.JoinTypeCross, plan.JoinTypeAnti},
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
				types: []plan.JoinType{plan.JoinTypeCross, plan.JoinTypeAnti},
				exp:   []sql.Row{{1, 1}, {2, 2}, {3, 2}},
			},
			{
				// cast prevents scope merging
				q:     "select * from xy where x != (select cast(r as signed) from rs where r = 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeAnti},
				exp:   []sql.Row{{0, 2}, {2, 1}, {3, 3}},
			},
			{
				// order by will be discarded
				q:     "select * from xy where x != (select r from rs where r = 1 order by 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeAnti},
				exp:   []sql.Row{{0, 2}, {2, 1}, {3, 3}},
			},
			{
				// limit prevents scope merging
				q:     "select * from xy where x != (select r from rs where r = 1 limit 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeAnti},
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
				types: []plan.JoinType{plan.JoinTypeCross, plan.JoinTypeRightSemiLookup},
				exp:   []sql.Row{{1, 0}, {1, 1}, {1, 2}, {1, 3}},
			},
			//{
			// scope and parent row are non-nil
			// TODO: subquery alias unable to track parent row from a different scope
			//				q: `
			//select * from uv where u > (
			//  select x from ab, (
			//    select x from xy where x = (
			//      select r from rs where r = 1
			//    ) order by 1
			//  ) sq
			//  order by 1 limit 1
			//)
			//order by 1;`,
			//types: []plan.JoinType{plan.JoinTypeCross, plan.JoinTypeRightSemiLookup},
			//exp:   []sql.Row{{2, 2}, {3, 2}},
			//},
			{
				q:     "select * from xy where y-1 in (select cast(u as signed) from uv) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemi},
				exp:   []sql.Row{{0, 2}, {2, 1}, {3, 3}},
			},
			{
				q:     "select * from xy where y-1 in (select u from uv order by 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemiLookup},
				exp:   []sql.Row{{0, 2}, {2, 1}, {3, 3}},
			},
			{
				q:     "select * from xy where y-1 in (select u from uv order by 1 limit 1) order by 1;",
				types: []plan.JoinType{plan.JoinTypeSemi},
				exp:   []sql.Row{{2, 1}},
			},
			{
				q:     "select * from xy where x in (select u from uv join ab on u = a and a = 2) order by 1;",
				types: []plan.JoinType{plan.JoinTypeRightSemiLookup, plan.JoinTypeLookup},
				exp:   []sql.Row{{2, 1}},
			},
			{
				q:     "select * from xy where x = (select u from uv join ab on u = a and a = 2) order by 1;",
				types: []plan.JoinType{plan.JoinTypeRightSemiLookup, plan.JoinTypeLookup},
				exp:   []sql.Row{{2, 1}},
			},
			{
				// group by doesn't transform
				q:     "select * from xy where y-1 in (select u from uv group by v having v = 2 order by 1) order by 1;",
				types: nil,
				exp:   []sql.Row{{3, 3}},
			},
			{
				// window doesn't transform
				q:     "select * from xy where y-1 in (select row_number() over (order by v) from uv) order by 1;",
				types: nil,
				exp:   []sql.Row{{0, 2}, {3, 3}},
			},
		},
	},
}

func TestJoinOps(t *testing.T, harness Harness) {
	for _, tt := range JoinOpTests {
		t.Run(tt.name, func(t *testing.T) {
			e := mustNewEngine(t, harness)
			defer e.Close()
			for _, statement := range tt.setup {
				if sh, ok := harness.(SkippingHarness); ok {
					if sh.SkipQueryTest(statement) {
						t.Skip()
					}
				}
				ctx := NewContext(harness)
				RunQueryWithContext(t, e, harness, ctx, statement)
			}
			for _, tt := range tt.tests {
				evalJoinTypeTest(t, harness, e, tt)
				evalJoinCorrectnessTest(t, harness, e, tt)
			}
		})
	}
}

func evalJoinTypeTest(t *testing.T, harness Harness, e *sqle.Engine, tt JoinOpTest) {
	t.Run(tt.q+" join types", func(t *testing.T) {
		if tt.skip {
			t.Skip()
		}

		ctx := NewContext(harness)
		ctx = ctx.WithQuery(tt.q)

		a, err := e.AnalyzeQuery(ctx, tt.q)
		require.NoError(t, err)

		jts := collectJoinTypes(a)
		require.Equal(t, tt.types, jts)
	})
}

func evalJoinCorrectnessTest(t *testing.T, harness Harness, e *sqle.Engine, tt JoinOpTest) {
	t.Run(tt.q, func(t *testing.T) {
		if tt.skip {
			t.Skip()
		}

		ctx := NewContext(harness)
		ctx = ctx.WithQuery(tt.q)

		sch, iter, err := e.QueryWithBindings(ctx, tt.q, nil)
		require.NoError(t, err, "Unexpected error for query %s: %s", tt.q, err)

		rows, err := sql.RowIterToRows(ctx, sch, iter)
		require.NoError(t, err, "Unexpected error for query %s: %s", tt.q, err)

		if tt.exp != nil {
			checkResults(t, tt.exp, nil, sch, rows, tt.q)
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

func TestJoinOpsPrepared(t *testing.T, harness Harness) {
	for _, tt := range JoinOpTests {
		t.Run(tt.name, func(t *testing.T) {
			e := mustNewEngine(t, harness)
			defer e.Close()
			for _, statement := range tt.setup {
				if sh, ok := harness.(SkippingHarness); ok {
					if sh.SkipQueryTest(statement) {
						t.Skip()
					}
				}
				ctx := NewContext(harness)
				RunQueryWithContext(t, e, harness, ctx, statement)
			}
			for _, tt := range tt.tests {
				evalJoinTypeTestPrepared(t, harness, e, tt)
				evalJoinCorrectnessTestPrepared(t, harness, e, tt)
			}
		})
	}
}

func evalJoinTypeTestPrepared(t *testing.T, harness Harness, e *sqle.Engine, tt JoinOpTest) {
	t.Run(tt.q+" join types", func(t *testing.T) {
		if tt.skip {
			t.Skip()
		}

		ctx := NewContext(harness)
		ctx = ctx.WithQuery(tt.q)

		bindings, err := injectBindVarsAndPrepare(t, ctx, e, tt.q)
		require.NoError(t, err)

		p, ok := e.PreparedDataCache.GetCachedStmt(ctx.Session.ID(), tt.q)
		require.True(t, ok, "prepared statement not found")

		if len(bindings) > 0 {
			var usedBindings map[string]bool
			p, usedBindings, err = plan.ApplyBindings(p, bindings)
			require.NoError(t, err)
			for binding := range bindings {
				require.True(t, usedBindings[binding], "unused binding %s", binding)
			}
		}

		a, _, err := e.Analyzer.AnalyzePrepared(ctx, p, nil)
		require.NoError(t, err)

		jts := collectJoinTypes(a)
		require.Equal(t, tt.types, jts)
	})
}

func evalJoinCorrectnessTestPrepared(t *testing.T, harness Harness, e *sqle.Engine, tt JoinOpTest) {
	t.Run(tt.q, func(t *testing.T) {
		if tt.skip {
			t.Skip()
		}

		ctx := NewContext(harness)
		ctx = ctx.WithQuery(tt.q)

		bindings, err := injectBindVarsAndPrepare(t, ctx, e, tt.q)
		require.NoError(t, err)

		sch, iter, err := e.QueryWithBindings(ctx, tt.q, bindings)
		require.NoError(t, err, "Unexpected error for query %s: %s", tt.q, err)

		rows, err := sql.RowIterToRows(ctx, sch, iter)
		require.NoError(t, err, "Unexpected error for query %s: %s", tt.q, err)

		if tt.exp != nil {
			checkResults(t, tt.exp, nil, sch, rows, tt.q)
		}

		require.Equal(t, 0, ctx.Memory.NumCaches())
		validateEngine(t, ctx, harness, e)
	})
}
