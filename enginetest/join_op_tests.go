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

	"github.com/dolthub/go-mysql-server/sql/memo"

	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
)

type JoinOpTests struct {
	Query    string
	Expected []sql.Row
	Skip     bool
}

var biasedCosters = map[string]memo.Coster{
	"inner":   memo.NewInnerBiasedCoster(),
	"lookup":  memo.NewLookupBiasedCoster(),
	"hash":    memo.NewHashBiasedCoster(),
	"merge":   memo.NewMergeBiasedCoster(),
	"partial": memo.NewPartialBiasedCoster(),
}

func TestJoinOps(t *testing.T, harness Harness) {
	for _, tt := range joinOpTests {
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
			for k, c := range biasedCosters {
				e.Analyzer.Coster = c
				for _, tt := range tt.tests {
					evalJoinCorrectness(t, harness, e, fmt.Sprintf("%s join: %s", k, tt.Query), tt.Query, tt.Expected, tt.Skip)
				}
			}
		})
	}
}

func TestJoinOpsPrepared(t *testing.T, harness Harness) {
	for _, tt := range joinOpTests {
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

			for k, c := range biasedCosters {
				e.Analyzer.Coster = c
				for _, tt := range tt.tests {
					evalJoinCorrectnessPrepared(t, harness, e, fmt.Sprintf("%s join: %s", k, tt.Query), tt.Query, tt.Expected, tt.Skip)
				}
			}
		})
	}
}

var joinOpTests = []struct {
	name  string
	setup [][]string
	tests []JoinOpTests
}{
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
				Expected: []sql.Row{{1, 1, 1}},
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
				Expected: []sql.Row{{1, 3}, {2, 2}, {3, 1}},
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
				Expected: []sql.Row{{0}, {2}, {3}, {4}, {5}, {5}, {7}, {8}, {10}},
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
				Expected: []sql.Row{{0, 0}},
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
				// natural join w/ inner join
				Query: "select * from mytable t1 natural join mytable t2 join othertable t3 on t2.i = t3.i2;",
				Expected: []sql.Row{
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
				Expected: []sql.Row{{float64(2)}},
			},
			{
				Query:    "select * from ab left join uv on a = u where exists (select * from uv where false)",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ab left join (select * from uv where false) s on a = u order by 1;",
				Expected: []sql.Row{
					{0, 2, nil, nil},
					{1, 2, nil, nil},
					{2, 2, nil, nil},
					{3, 1, nil, nil},
				},
			},
			{
				Query:    "select * from ab right join (select * from uv where false) s on a = u order by 1;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from mytable where exists (select * from mytable where i = 1) order by 1;",
				Expected: []sql.Row{
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},
			// queries that test subquery hoisting
			{
				// case 1: condition uses columns from both sides
				Query: "/*+case1*/ select * from ab where exists (select * from xy where ab.a = xy.x + 3)",
				Expected: []sql.Row{
					{3, 1},
				},
			},
			{
				// case 1N: NOT EXISTS condition uses columns from both sides
				Query: "/*+case1N*/ select * from ab where not exists (select * from xy where ab.a = xy.x + 3)",
				Expected: []sql.Row{
					{0, 2},
					{1, 2},
					{2, 2},
				},
			},
			{
				// case 2: condition uses columns from left side only
				Query:    "/*+case2*/ select * from ab where exists (select * from xy where a = 1)",
				Expected: []sql.Row{{1, 2}},
			},
			{
				// case 2N: NOT EXISTS condition uses columns from left side only
				Query: "/*+case2N*/ select * from ab where not exists (select * from xy where a = 1)",
				Expected: []sql.Row{
					{0, 2},
					{2, 2},
					{3, 1},
				},
			},
			{
				// case 3: condition uses columns from right side only
				Query: "/*+case3*/ select * from ab where exists (select * from xy where 1 = xy.x)",
				Expected: []sql.Row{
					{0, 2},
					{1, 2},
					{2, 2},
					{3, 1},
				},
			},
			{
				// case 3N: NOT EXISTS condition uses columns from right side only
				Query: "/*+case3N*/ select * from ab where not exists (select * from xy where 10 = xy.x)",
				Expected: []sql.Row{
					{0, 2},
					{1, 2},
					{2, 2},
					{3, 1},
				},
			},
			{
				// case 4a: condition uses no columns from either side, and condition is true
				Query: "/*+case4a*/ select * from ab where exists (select * from xy where 1 = 1)",
				Expected: []sql.Row{
					{0, 2},
					{1, 2},
					{2, 2},
					{3, 1},
				},
			},
			{
				// case 4aN: NOT EXISTS condition uses no columns from either side, and condition is true
				Query:    "/*+case4aN*/ select * from ab where not exists (select * from xy where 1 = 1)",
				Expected: []sql.Row{},
			},
			{
				// case 4b: condition uses no columns from either side, and condition is false
				Query:    "/*+case4b*/ select * from ab where exists (select * from xy where 1 = 0)",
				Expected: []sql.Row{},
			},
			{
				// case 4bN: NOT EXISTS condition uses no columns from either side, and condition is false
				Query:    "/*+case4bN*/ select * from ab where not exists (select * from xy where 1 = 0)",
				Expected: []sql.Row{{0, 2}, {1, 2}, {2, 2}, {3, 1}},
			},
			{
				// test more complex scopes
				Query: "select x, 1 in (select a from ab where exists (select * from uv where a = u)) s from xy",
				Expected: []sql.Row{
					{0, true},
					{1, true},
					{2, true},
					{3, true},
				},
			},
			{
				Query:    `select a.i,a.f, b.i2 from niltable a left join niltable b on a.i = b.i2`,
				Expected: []sql.Row{{1, nil, nil}, {2, nil, 2}, {3, nil, nil}, {4, 4.0, 4}, {5, 5.0, nil}, {6, 6.0, 6}},
			},
			{
				Query: `SELECT i, s, i2, s2 FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 <=> s)`,
				Expected: []sql.Row{
					{1, "first row", 1, "third"},
					{2, "second row", 2, "second"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query: `SELECT i, s, i2, s2 FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND NOT (s2 = s)`,
				Expected: []sql.Row{
					{1, "first row", 1, "third"},
					{2, "second row", 2, "second"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query: `SELECT i, s, i2, s2 FROM MYTABLE JOIN OTHERTABLE ON i = i2 AND CONCAT(s, s2) IS NOT NULL`,
				Expected: []sql.Row{
					{1, "first row", 1, "third"},
					{2, "second row", 2, "second"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query: `SELECT * FROM mytable mt JOIN othertable ot ON ot.i2 = (SELECT i2 FROM othertable WHERE s2 = "second") AND mt.i = ot.i2 JOIN mytable mt2 ON mt.i = mt2.i`,
				Expected: []sql.Row{
					{2, "second row", "second", 2, 2, "second row"},
				},
			},
			{
				Query:    "SELECT l.i, r.i2 FROM niltable l INNER JOIN niltable r ON l.i2 = r.i2 ORDER BY 1",
				Expected: []sql.Row{{2, 2}, {4, 4}, {6, 6}},
			},
			{
				Query:    "SELECT l.i, r.i2 FROM niltable l INNER JOIN niltable r ON l.i2 != r.i2 ORDER BY 1, 2",
				Expected: []sql.Row{{2, 4}, {2, 6}, {4, 2}, {4, 6}, {6, 2}, {6, 4}},
			},
			{
				Query:    "SELECT l.i, r.i2 FROM niltable l INNER JOIN niltable r ON l.i2 <=> r.i2 ORDER BY 1 ASC",
				Expected: []sql.Row{{1, nil}, {1, nil}, {1, nil}, {2, 2}, {3, nil}, {3, nil}, {3, nil}, {4, 4}, {5, nil}, {5, nil}, {5, nil}, {6, 6}},
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
				Expected: []sql.Row{
					{"third", 1, 1},
					{"second", 2, 2},
					{"first", 3, 3},
					{"not found", 4, nil},
				},
			},
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{"testing", 4},
				},
			},

			{
				Query: "SELECT substring(mytable.s, 1, 5) AS s FROM mytable INNER JOIN othertable ON (substring(mytable.s, 1, 5) = SUBSTRING(othertable.s2, 1, 5)) GROUP BY 1",
				Expected: []sql.Row{
					{"third"},
					{"secon"},
					{"first"},
				},
			},
			{
				Query: "SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "SELECT /*+ JOIN_ORDER(t1,t2) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "SELECT /*+ JOIN_ORDER(t2,t1) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "SELECT /*+ JOIN_ORDER(t1) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "SELECT /*+ JOIN_ORDER(t1, mytable) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "SELECT /*+ JOIN_ORDER(t1, not_exist) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "SELECT /*+ NOTHING(abc) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "SELECT /*+ JOIN_ORDER( */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "select mytable.i as i2, othertable.i2 as i from mytable join othertable on i = i2 order by 1",
				Expected: []sql.Row{
					{1, 1},
					{2, 2},
					{3, 3},
				},
			},
			{
				Query: "SELECT i, s, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR s = s2 order by 1",
				Expected: []sql.Row{
					{1, "first row", 1, "third"},
					{2, "second row", 2, "second"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query: "SELECT i, s, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 order by 1, 3",
				Expected: []sql.Row{
					{1, "first row", 1, "third"},
					{1, "first row", 3, "first"},
					{2, "second row", 2, "second"},
					{3, "third row", 1, "third"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query: "SELECT i, s, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 OR SUBSTRING_INDEX(s, ' ', 2) = s2 order by 1, 3",
				Expected: []sql.Row{
					{1, "first row", 1, "third"},
					{1, "first row", 3, "first"},
					{2, "second row", 2, "second"},
					{3, "third row", 1, "third"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query: "SELECT i, s, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 2) = s2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 order by 1, 3",
				Expected: []sql.Row{
					{1, "first row", 1, "third"},
					{1, "first row", 3, "first"},
					{2, "second row", 2, "second"},
					{3, "third row", 1, "third"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query: "SELECT i, s, i2, s2 FROM mytable INNER JOIN othertable ON SUBSTRING_INDEX(s, ' ', 2) = s2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 OR i = i2 order by 1, 3",
				Expected: []sql.Row{
					{1, "first row", 1, "third"},
					{1, "first row", 3, "first"},
					{2, "second row", 2, "second"},
					{3, "third row", 1, "third"},
					{3, "third row", 3, "first"},
				},
			},
			{
				Query:    "SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 3",
				Expected: []sql.Row{},
			},
			{
				Query: "SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i",
				Expected: []sql.Row{
					{int64(1), int64(1), "third"},
					{int64(2), int64(2), "second"},
					{int64(3), int64(3), "first"},
				},
			},
			{
				Query: "SELECT i, i2, s2 FROM mytable as OTHERTABLE INNER JOIN othertable as MYTABLE ON i = i2 ORDER BY i",
				Expected: []sql.Row{
					{int64(1), int64(1), "third"},
					{int64(2), int64(2), "second"},
					{int64(3), int64(3), "first"},
				},
			},
			{
				Query: "SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i",
				Expected: []sql.Row{
					{"third", int64(1), int64(1)},
					{"second", int64(2), int64(2)},
					{"first", int64(3), int64(3)},
				},
			},
			{
				Query: "SELECT i, i2, s2 FROM othertable JOIN mytable  ON i = i2 ORDER BY i",
				Expected: []sql.Row{
					{int64(1), int64(1), "third"},
					{int64(2), int64(2), "second"},
					{int64(3), int64(3), "first"},
				},
			},
			{
				Query: "SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2 ORDER BY i",
				Expected: []sql.Row{
					{"third", int64(1), int64(1)},
					{"second", int64(2), int64(2)},
					{"first", int64(3), int64(3)},
				},
			},
			{
				Query: "SELECT s FROM mytable INNER JOIN othertable " +
					"ON substring(s2, 1, 2) != '' AND i = i2 ORDER BY 1",
				Expected: []sql.Row{
					{"first row"},
					{"second row"},
					{"third row"},
				},
			},
			{
				Query: `SELECT i FROM mytable NATURAL JOIN tabletest`,
				Expected: []sql.Row{
					{int64(1)},
					{int64(2)},
					{int64(3)},
				},
			},
			{
				Query: `SELECT i FROM mytable AS t NATURAL JOIN tabletest AS test`,
				Expected: []sql.Row{
					{int64(1)},
					{int64(2)},
					{int64(3)},
				},
			},
			{
				Query: `SELECT t.i, test.s FROM mytable AS t NATURAL JOIN tabletest AS test`,
				Expected: []sql.Row{
					{int64(1), "first row"},
					{int64(2), "second row"},
					{int64(3), "third row"},
				},
			},
			{
				Query: `SELECT * FROM tabletest, mytable mt INNER JOIN othertable ot ON mt.i = ot.i2`,
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{int64(3), "third row", "first", int64(3)},
				},
			},
			{
				Query: `SELECT * FROM othertable ot INNER JOIN mytable mt ON mt.i = ot.i2 AND mt.i > 2`,
				Expected: []sql.Row{
					{"first", int64(3), int64(3), "third row"},
				},
			},
			{
				Query: "SELECT i, i2, s2 FROM mytable LEFT JOIN othertable ON i = i2 - 1",
				Expected: []sql.Row{
					{int64(1), int64(2), "second"},
					{int64(2), int64(3), "first"},
					{int64(3), nil, nil},
				},
			},
			{
				Query: "SELECT i, i2, s2 FROM mytable RIGHT JOIN othertable ON i = i2 - 1",
				Expected: []sql.Row{
					{nil, int64(1), "third"},
					{int64(1), int64(2), "second"},
					{int64(2), int64(3), "first"},
				},
			},
			{
				Query: "SELECT i, i2, s2 FROM mytable LEFT OUTER JOIN othertable ON i = i2 - 1",
				Expected: []sql.Row{
					{int64(1), int64(2), "second"},
					{int64(2), int64(3), "first"},
					{int64(3), nil, nil},
				},
			},
			{
				Query: "SELECT i, i2, s2 FROM mytable RIGHT OUTER JOIN othertable ON i = i2 - 1",
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{1, 1, "third", 1, "third"},
					{2, 2, "second", 2, "second"},
					{3, 3, "first", 3, "first"},
				},
			},
			{
				Query: "SELECT one_pk.c5,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{4, 0, 0},
					{4, 0, 1},
					{14, 1, 0},
					{14, 1, 1},
				},
			},
			{
				Query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{4, 0, 0},
					{4, 0, 1},
					{14, 1, 0},
					{14, 1, 1},
				},
			},
			{
				Query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{4, 0, 0},
					{4, 0, 1},
					{14, 1, 0},
					{14, 1, 1},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE pk=1 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{1, 0, 1},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{0, 0, 0},
					{1, 1, 1},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{0, 0, 0},
					{1, 1, 1},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=tpk.pk1 AND pk=tpk.pk2 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{0, 0, 0},
					{1, 1, 1},
				},
			},
			{
				Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk-1=tpk.pk2
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1
						ORDER BY 1`,
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{1, 1, 0, 0, 1},
				},
			},
			{
				Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk
						JOIN two_pk tpk ON pk=tpk.pk1 AND pk-1=tpk.pk2
						JOIN two_pk tpk2 ON pk-1=TPK2.pk1 AND pk=tpk2.pk2
						ORDER BY 1`,
				Expected: []sql.Row{
					{1, 1, 0, 0, 1},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{0, 0, 0},
					{1, 1, 1},
					{2, nil, nil},
					{3, nil, nil},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{nil, 0, 1},
					{nil, 1, 0},
					{0, 0, 0},
					{1, 1, 1},
				},
			},
			{
				Query: "SELECT i,pk1,pk2 FROM mytable JOIN two_pk ON i-1=pk1 AND i-2=pk2 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{int64(2), 1, 0},
				},
			},
			{
				Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{0, 0, 0, 0},
					{0, 1, 1, 0},
					{1, 0, 0, 1},
					{1, 1, 1, 1},
				},
			},
			{
				Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{0, 0, 0, 0},
					{0, 1, 0, 1},
					{1, 0, 1, 0},
					{1, 1, 1, 1},
				},
			},
			{
				Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{0, 0, 0, 0},
					{0, 1, 0, 1},
					{1, 0, 1, 0},
					{1, 1, 1, 1},
				},
			},
			{
				Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON b.pk1=a.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{0, 0, 0, 0},
					{0, 1, 0, 1},
					{1, 0, 1, 0},
					{1, 1, 1, 1},
				},
			},
			{
				Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1+1=b.pk1 AND a.pk2+1=b.pk2 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{0, 0, 1, 1},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{0, nil, nil},
					{1, nil, nil},
					{2, int64(2), nil},
					{3, nil, nil},
				},
			},
			{
				Query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i2 ORDER BY 2,3",
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{0, nil, nil},
					{1, nil, nil},
					{2, nil, nil},
					{3, nil, nil},
				},
			},
			{
				Query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i2 and pk > 0 ORDER BY 2,3", // > 0 clause in join condition is ignored
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{0, nil, nil},
					{1, 1, nil},
				},
			},
			{
				Query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3",
				Expected: []sql.Row{
					{nil, nil, 5.0},
					{nil, int64(4), 4.0},
					{nil, int64(6), 6.0},
				},
			},
			{
				Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1 ORDER BY 1",
				Expected: []sql.Row{
					{2, 2, nil},
					{3, 3, nil},
				},
			},
			{
				Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE c1 > 10 ORDER BY 1",
				Expected: []sql.Row{
					{2, 2, nil},
					{3, 3, nil},
				},
			},
			{
				Query: "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3",
				Expected: []sql.Row{
					{nil, 4, 4.0},
					{nil, 5, 5.0},
					{nil, 6, 6.0},
				},
			},
			{
				Query: "SELECT t1.i,t1.i2 FROM niltable t1 LEFT JOIN niltable t2 ON t1.i=t2.i2 WHERE t2.f IS NULL ORDER BY 1,2",
				Expected: []sql.Row{
					{1, nil},
					{2, 2},
					{3, nil},
					{5, nil},
				},
			},
			{
				Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i2 > 1 ORDER BY 1",
				Expected: []sql.Row{
					{2, 2, nil},
				},
			},
			{
				Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i > 1 ORDER BY 1",
				Expected: []sql.Row{
					{2, 2, nil},
					{3, 3, nil},
				},
			},
			{
				Query: "SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i2 IS NOT NULL ORDER BY 1",
				Expected: []sql.Row{
					{2, int64(2), nil},
				},
			},
			{
				Query: "SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i2 WHERE pk > 1 ORDER BY 1",
				Expected: []sql.Row{
					{2, int64(2), nil},
					{3, nil, nil},
				},
			},
			{
				Query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i2 WHERE pk > 0 ORDER BY 2,3",
				Expected: []sql.Row{
					{2, int64(2), nil},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2,one_pk.c1 AS foo, two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 ORDER BY 1,2,3",
				Expected: []sql.Row{
					{0, 0, 0, 0, 0},
					{1, 0, 1, 10, 10},
					{2, 1, 0, 20, 20},
					{3, 1, 1, 30, 30},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2,one_pk.c1 AS foo,two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE one_pk.c1=10",
				Expected: []sql.Row{
					{1, 0, 1, 10, 10},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk1-pk>0 AND pk2<1",
				Expected: []sql.Row{
					{0, 1, 0},
				},
			},
			{
				Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ORDER BY 1,2,3",
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{0, 0},
					{1, 1},
					{2, 2},
					{3, 3},
				},
			},
			{
				Query: "SELECT a.pk,b.pk FROM one_pk a, one_pk b WHERE a.pk = b.pk order by a.pk",
				Expected: []sql.Row{
					{0, 0},
					{1, 1},
					{2, 2},
					{3, 3},
				},
			},
			{
				Query: "SELECT one_pk.pk,b.pk FROM one_pk JOIN one_pk b ON one_pk.pk = b.pk order by one_pk.pk",
				Expected: []sql.Row{
					{0, 0},
					{1, 1},
					{2, 2},
					{3, 3},
				},
			},
			{
				Query: "SELECT one_pk.pk,b.pk FROM one_pk, one_pk b WHERE one_pk.pk = b.pk order by one_pk.pk",
				Expected: []sql.Row{
					{0, 0},
					{1, 1},
					{2, 2},
					{3, 3},
				},
			},
			{
				Query: "select sum(x.i) + y.i from mytable as x, mytable as y where x.i = y.i GROUP BY x.i",
				Expected: []sql.Row{
					{int64(2)},
					{int64(4)},
					{int64(6)},
				},
			},
			{
				Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
				Expected: []sql.Row{
					{0, 0, 0, 0, 0},
					{1, 1, 1, 1, 1},
				},
			},
			{
				Query: `SELECT pk,nt.i,nt2.i FROM one_pk
						RIGHT JOIN niltable nt ON pk=nt.i
						RIGHT JOIN niltable nt2 ON pk=nt2.i - 1
						ORDER BY 3`,
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{1, 1, 4},
					{1, 1, 4},
				},
			},
			{
				Query: `SELECT pk,pk2,
							(SELECT opk.c5 FROM one_pk opk JOIN two_pk tpk ON opk.c5=tpk.c5 ORDER BY 1 LIMIT 1)
							FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
				Expected: []sql.Row{
					{1, 1, 4},
					{1, 1, 4},
				},
			},
			{
				Query: `SELECT /*+ JOIN_ORDER(mytable, othertable) */ s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b where a.i = b.i`,
				Expected: []sql.Row{
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b where a.i = b.i OR a.i = 1`,
				Expected: []sql.Row{
					{1, "first row"},
					{1, "first row"},
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b where NOT(a.i = b.i OR a.s = b.i)`,
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b where a.i in (2, 432, 7)`,
				Expected: []sql.Row{
					{2, "second row"},
					{2, "second row"},
					{2, "second row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i AND c.i = d.i AND c.i = 2`,
				Expected: []sql.Row{
					{2, "second row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
				Expected: []sql.Row{
					{2, "second row"},
					{2, "second row"},
					{2, "second row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a, mytable b, mytable c, mytable d where a.i = b.i AND b.s = c.s`,
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i = b.i OR a.i = 1`,
				Expected: []sql.Row{
					{1, "first row"},
					{1, "first row"},
					{1, "first row"},
					{2, "second row"},
					{3, "third row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b where a.i >= b.i`,
				Expected: []sql.Row{
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
				Expected: []sql.Row{},
			},
			{
				Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.i = c.i AND c.i = d.i AND c.i = 2`,
				Expected: []sql.Row{
					{2, "second row"},
				},
			},
			{
				Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.i = c.i AND (c.i = d.s OR c.i = 2)`,
				Expected: []sql.Row{
					{2, "second row"},
					{2, "second row"},
					{2, "second row"}},
			},
			{
				Query: `SELECT a.* FROM mytable a CROSS JOIN mytable b CROSS JOIN mytable c CROSS JOIN mytable d where a.i = b.i AND b.s = c.s`,
				Expected: []sql.Row{
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
				Expected: []sql.Row{{1, "first row"}},
			},
			{
				Query: `SELECT * FROM ab WHERE (
			EXISTS (SELECT * FROM ab Alias1 JOIN ab Alias2 WHERE Alias1.a = (ab.a + 1))
			AND EXISTS (SELECT * FROM xy Alias1 JOIN xy Alias2 WHERE Alias1.x = (ab.a + 2)));`,
				Expected: []sql.Row{
					{0, 2},
					{1, 2}},
			},
			{
				// verify that duplicate aliases in different subqueries are allowed
				Query: `SELECT * FROM mytable Alias0 WHERE (
				      	EXISTS (SELECT * FROM mytable Alias WHERE Alias.i = Alias0.i + 1)
				      	AND EXISTS (SELECT * FROM othertable Alias WHERE Alias.i2 = Alias0.i + 2));`,
				Expected: []sql.Row{{1, "first row"}},
			},
			{
				Query: `SELECT * FROM mytable
						WHERE
  							i = (SELECT i2 FROM othertable alias1 WHERE i2 = 2) AND
  							i+1 = (SELECT i2 FROM othertable alias1 WHERE i2 = 3);`,
				Expected: []sql.Row{{2, "second row"}},
			},
			{
				Query: `SELECT * FROM mytable WHERE (
      					EXISTS (SELECT * FROM mytable Alias1 join mytable Alias2 WHERE Alias1.i = (mytable.i + 1))
      					AND EXISTS (SELECT * FROM othertable Alias1 join othertable Alias2 WHERE Alias1.i2 = (mytable.i + 2)))`,
				Expected: []sql.Row{{1, "first row"}},
			},
		},
	},
}
