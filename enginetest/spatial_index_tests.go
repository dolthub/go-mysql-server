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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type SpatialIndexPlanTestAssertion struct {
	q        string
	skip     bool
	skipPrep bool
	noIdx    bool
	exp      []sql.UntypedSqlRow
}

type SpatialIndexPlanTest struct {
	name  string
	setup []string
	tests []SpatialIndexPlanTestAssertion
}

var SpatialIndexTests = []SpatialIndexPlanTest{
	{
		name: "filter point table with st_intersects",
		setup: []string{
			"create table point_tbl(p point not null srid 0, spatial index (p))",
			"insert into point_tbl values (point(0,0)), (point(1,1)), (point(2,2))",
		},
		tests: []SpatialIndexPlanTestAssertion{
			{
				q: "select p from point_tbl where st_intersects(p, point(0,0))",
				exp: []sql.UntypedSqlRow{
					{types.Point{}},
				},
			},
		},
	},
	{
		name: "filter point table with st_intersects with Equals",
		setup: []string{
			"create table point_tbl(p point not null srid 0, spatial index (p))",
			"insert into point_tbl values (point(0,0)), (point(1,1)), (point(2,2))",
		},
		tests: []SpatialIndexPlanTestAssertion{
			{
				noIdx: true, // this should take advantage of indexes
				q:     "select p from point_tbl where st_intersects(p, point(0,0)) = true",
				exp: []sql.UntypedSqlRow{
					{types.Point{}},
				},
			},
			{
				noIdx: true,
				q:     "select st_aswkt(p) from point_tbl where st_intersects(p, point(0,0)) = false order by st_x(p), st_y(p)",
				exp: []sql.UntypedSqlRow{
					{"POINT(1 1)"},
					{"POINT(2 2)"},
				},
			},
		},
	},
	{
		name: "filter point table with st_intersects with ANDs and ORs",
		setup: []string{
			"create table point_tbl(p point not null srid 0, spatial index (p))",
			"insert into point_tbl values (point(0,0)), (point(1,1)), (point(2,2))",
			"create table point_tbl_pk(pk int primary key, p point not null srid 0, spatial index (p))",
			"insert into point_tbl_pk values (0, point(0,0)), (1, point(1,1)), (2, point(2,2))",
		},
		tests: []SpatialIndexPlanTestAssertion{
			{
				noIdx: true,
				q:     "select p from point_tbl where st_intersects(p, point(0,0)) and st_intersects(p, point(1,1))",
				exp:   []sql.UntypedSqlRow{},
			},
			{
				noIdx: true,
				q:     "select st_aswkt(p) from point_tbl where st_intersects(p, point(0,0)) or st_intersects(p, point(1,1)) order by st_x(p), st_y(p)",
				exp: []sql.UntypedSqlRow{
					{"POINT(0 0)"},
					{"POINT(1 1)"},
				},
			},
			{
				noIdx: false, // still expect index access using primary key
				q:     "select pk, st_aswkt(p) from point_tbl_pk where pk = 0 and st_intersects(p, point(0,0)) order by pk",
				exp: []sql.UntypedSqlRow{
					{0, "POINT(0 0)"},
				},
			},
			{
				noIdx: false, // still expect index access using primary key
				q:     "select pk, st_aswkt(p) from point_tbl_pk where pk = 0 or st_intersects(p, point(1,1)) order by pk",
				exp: []sql.UntypedSqlRow{
					{0, "POINT(0 0)"},
					{1, "POINT(1 1)"},
				},
			},
		},
	},
	{
		name: "filter subquery with st_intersects",
		setup: []string{
			"create table point_tbl(p point not null srid 0, spatial index (p))",
			"insert into point_tbl values (point(0,0)), (point(1,1)), (point(2,2))",
		},
		tests: []SpatialIndexPlanTestAssertion{
			{
				q: "select st_aswkt(p) from (select * from point_tbl) t where st_intersects(p, point(0,0))",
				exp: []sql.UntypedSqlRow{
					{"POINT(0 0)"},
				},
			},
		},
	},
	{
		name: "filter geom table with st_intersects",
		setup: []string{
			"create table geom_tbl(g geometry not null srid 0, spatial index (g))",
			"insert into geom_tbl values (point(0,0))",
			"insert into geom_tbl values (st_geomfromtext('linestring(-1 -1,1 1)'))",
			"insert into geom_tbl values (st_geomfromtext('polygon((2 2,2 -2,-2 -2,-2 2,2 2),(1 1,1 -1,-1 -1,-1 1,1 1))'))",
		},
		tests: []SpatialIndexPlanTestAssertion{
			{
				q: "select st_aswkt(g) from geom_tbl where st_intersects(g, point(0,0)) order by g",
				exp: []sql.UntypedSqlRow{
					{"POINT(0 0)"},
					{"LINESTRING(-1 -1,1 1)"},
				},
			},
			{
				q: "select st_aswkt(g) from geom_tbl where st_intersects(g, linestring(point(-1,1), point(1,-1))) order by g",
				exp: []sql.UntypedSqlRow{
					{"POINT(0 0)"},
					{"LINESTRING(-1 -1,1 1)"},
					{"POLYGON((2 2,2 -2,-2 -2,-2 2,2 2),(1 1,1 -1,-1 -1,-1 1,1 1))"},
				},
			},
		},
	},
	{
		name: "filter complicated geom table with st_intersects",
		setup: []string{
			"create table geom_tbl(g geometry not null srid 0, spatial index (g))",

			"insert into geom_tbl values (point(-2,-2))",
			"insert into geom_tbl values (point(-2,-1))",
			"insert into geom_tbl values (point(-2,0))",
			"insert into geom_tbl values (point(-2,1))",
			"insert into geom_tbl values (point(-2,2))",

			"insert into geom_tbl values (point(-1,-2))",
			"insert into geom_tbl values (point(-1,-1))",
			"insert into geom_tbl values (point(-1,0))",
			"insert into geom_tbl values (point(-1,1))",
			"insert into geom_tbl values (point(-1,2))",

			"insert into geom_tbl values (point(0,-2))",
			"insert into geom_tbl values (point(0,-1))",
			"insert into geom_tbl values (point(0,0))",
			"insert into geom_tbl values (point(0,1))",
			"insert into geom_tbl values (point(0,2))",

			"insert into geom_tbl values (point(1,-2))",
			"insert into geom_tbl values (point(1,-1))",
			"insert into geom_tbl values (point(1,0))",
			"insert into geom_tbl values (point(1,1))",
			"insert into geom_tbl values (point(1,2))",

			"insert into geom_tbl values (point(2,-2))",
			"insert into geom_tbl values (point(2,-1))",
			"insert into geom_tbl values (point(2,0))",
			"insert into geom_tbl values (point(2,1))",
			"insert into geom_tbl values (point(2,2))",
		},
		tests: []SpatialIndexPlanTestAssertion{
			{
				q: "select st_aswkt(g) from geom_tbl where st_intersects(g, point(0,0)) order by g",
				exp: []sql.UntypedSqlRow{
					{"POINT(0 0)"},
				},
			},
			{
				q: "select st_aswkt(g) from geom_tbl where st_intersects(g, linestring(point(-1,1), point(1,-1))) order by st_x(g), st_y(g)",
				exp: []sql.UntypedSqlRow{
					{"POINT(-1 1)"},
					{"POINT(0 0)"},
					{"POINT(1 -1)"},
				},
			},
			{
				q: "select st_aswkt(g) from geom_tbl where st_intersects(g, st_geomfromtext('polygon((1 1,1 -1,-1 -1,-1 1,1 1))')) order by st_x(g), st_y(g)",
				exp: []sql.UntypedSqlRow{
					{"POINT(-1 -1)"},
					{"POINT(-1 0)"},
					{"POINT(-1 1)"},
					{"POINT(0 -1)"},
					{"POINT(0 0)"},
					{"POINT(0 1)"},
					{"POINT(1 -1)"},
					{"POINT(1 0)"},
					{"POINT(1 1)"},
				},
			},
			{
				q: "select st_aswkt(g) from geom_tbl where st_intersects(g, st_geomfromtext('linestring(-2 -2,2 2)')) order by st_x(g), st_y(g)",
				exp: []sql.UntypedSqlRow{
					{"POINT(-2 -2)"},
					{"POINT(-1 -1)"},
					{"POINT(0 0)"},
					{"POINT(1 1)"},
					{"POINT(2 2)"},
				},
			},
			{
				q: "select st_aswkt(g) from geom_tbl where st_intersects(g, st_geomfromtext('multipoint(-2 -2,0 0,2 2)')) order by st_x(g), st_y(g)",
				exp: []sql.UntypedSqlRow{
					{"POINT(-2 -2)"},
					{"POINT(0 0)"},
					{"POINT(2 2)"},
				},
			},
			{
				noIdx: true,
				q:     "select st_aswkt(g) from geom_tbl where not st_intersects(g, st_geomfromtext('multipoint(0 0)')) order by st_x(g), st_y(g)",
				exp: []sql.UntypedSqlRow{
					{"POINT(-2 -2)"},
					{"POINT(-2 -1)"},
					{"POINT(-2 0)"},
					{"POINT(-2 1)"},
					{"POINT(-2 2)"},
					{"POINT(-1 -2)"},
					{"POINT(-1 -1)"},
					{"POINT(-1 0)"},
					{"POINT(-1 1)"},
					{"POINT(-1 2)"},
					{"POINT(0 -2)"},
					{"POINT(0 -1)"},
					{"POINT(0 1)"},
					{"POINT(0 2)"},
					{"POINT(1 -2)"},
					{"POINT(1 -1)"},
					{"POINT(1 0)"},
					{"POINT(1 1)"},
					{"POINT(1 2)"},
					{"POINT(2 -2)"},
					{"POINT(2 -1)"},
					{"POINT(2 0)"},
					{"POINT(2 1)"},
					{"POINT(2 2)"},
				},
			},
		},
	},
	{
		name: "negated filter point table with st_intersects does not use index",
		setup: []string{
			"create table point_tbl(p point not null srid 0, spatial index (p))",
			"insert into point_tbl values (point(0,0)), (point(1,1)), (point(2,2))",
		},
		tests: []SpatialIndexPlanTestAssertion{
			{
				noIdx: true,
				q:     "select st_aswkt(p) from point_tbl where not st_intersects(p, point(0,0)) order by p",
				exp: []sql.UntypedSqlRow{
					{"POINT(2 2)"},
					{"POINT(1 1)"},
				},
			},
		},
	},
	{
		name: "filter join with st_intersects",
		setup: []string{
			"create table t1(g geometry not null srid 0, spatial index (g))",
			"create table t2(g geometry not null srid 0, spatial index (g))",
			"insert into t1 values (point(0,0)), (point(1,1))",
			"insert into t2 values (point(0,0)), (point(1,1))",
		},
		tests: []SpatialIndexPlanTestAssertion{
			{
				q: "select st_aswkt(t1.g), st_aswkt(t2.g) from t1 join t2 where st_intersects(t1.g, point(0,0))",
				exp: []sql.UntypedSqlRow{
					{"POINT(0 0)", "POINT(0 0)"},
					{"POINT(0 0)", "POINT(1 1)"},
				},
			},
			{
				noIdx: true, // TODO: this should be able to take advantage of indexes
				q:     "select st_aswkt(t1.g), st_aswkt(t2.g) from t1 join t2 where st_intersects(t1.g, t2.g)",
				exp: []sql.UntypedSqlRow{
					{"POINT(0 0)", "POINT(0 0)"},
					{"POINT(1 1)", "POINT(1 1)"},
				},
			},
		},
	},
	{
		name: "filter point table with st_within",
		setup: []string{
			"create table point_tbl(p point not null srid 0, spatial index (p))",
			"insert into point_tbl values (point(0,0)), (point(1,1)), (point(2,2))",
			"create table point_pk_tbl(i int primary key, p point not null srid 0, spatial index (p))",
			"insert into point_pk_tbl values (0, point(0,0)), (1, point(1,1)), (2, point(2,2))",
		},
		tests: []SpatialIndexPlanTestAssertion{
			{
				q: "select p from point_tbl where st_within(p, point(0,0))",
				exp: []sql.UntypedSqlRow{
					{types.Point{X: 0, Y: 0}},
				},
			},
			{
				noIdx: true,
				q:     "select p from point_tbl where st_within(p, null)",
				exp:   []sql.UntypedSqlRow{},
			},
			{
				q: "select i, p from point_pk_tbl where st_within(p, point(0,0))",
				exp: []sql.UntypedSqlRow{
					{0, types.Point{X: 0, Y: 0}},
				},
			},
			{
				noIdx: true,
				q:     "select i, p from point_pk_tbl where st_within(p, null)",
				exp:   []sql.UntypedSqlRow{},
			},
		},
	},
}

func TestSpatialIndexPlans(t *testing.T, harness Harness) {
	for _, tt := range SpatialIndexTests {
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
				evalSpatialIndexPlanCorrectness(t, harness, e, tt.q, tt.q, tt.exp, tt.skip)
				if !IsServerEngine(e) {
					evalSpatialIndexPlanTest(t, harness, e, tt.q, tt.skip, tt.noIdx)
				}
			}
		})
	}
}

func evalSpatialIndexPlanTest(t *testing.T, harness Harness, e QueryEngine, query string, skip, noIdx bool) {
	t.Run(query+" index plan", func(t *testing.T) {
		if skip {
			t.Skip()
		}
		ctx := NewContext(harness)
		ctx = ctx.WithQuery(query)

		a, err := analyzeQuery(ctx, e, query)
		require.NoError(t, err)

		hasFilter, hasIndex, hasRightOrder := false, false, false
		transform.Inspect(a, func(n sql.Node) bool {
			if n == nil {
				return false
			}
			if _, ok := n.(*plan.Filter); ok {
				hasFilter = true
			}
			if _, ok := n.(*plan.IndexedTableAccess); ok {
				hasRightOrder = hasFilter
				hasIndex = true
			}
			return true
		})

		require.True(t, hasFilter, "filter node was missing from plan")
		if noIdx {
			require.False(t, hasIndex, "indextableaccess should not be in plan")
		} else {
			require.True(t, hasIndex, "indextableaccess node was missing from plan:\n %s", sql.DebugString(a))
			require.True(t, hasRightOrder, "filter node was not above indextableaccess")
		}
	})
}

func evalSpatialIndexPlanCorrectness(t *testing.T, harness Harness, e QueryEngine, name, q string, exp []sql.UntypedSqlRow, skip bool) {
	t.Run(name, func(t *testing.T) {
		if skip {
			t.Skip()
		}

		ctx := NewContext(harness)
		ctx = ctx.WithQuery(q)

		sch, iter, _, err := e.QueryWithBindings(ctx, q, nil, nil, nil)
		require.NoError(t, err, "Unexpected error for q %s: %s", q, err)

		rows, err := sql.RowIterToRows(ctx, iter)
		require.NoError(t, err, "Unexpected error for q %s: %s", q, err)

		if exp != nil {
			CheckResults(t, harness, exp, nil, sch, sql.RowsToUntyped(rows), q, e)
		}

		require.Equal(t, 0, ctx.Memory.NumCaches())
		validateEngine(t, ctx, harness, e)
	})
}
