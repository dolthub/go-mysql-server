// Copyright 2026 Dolthub, Inc.
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

import (
	"github.com/dolthub/go-mysql-server/sql"
)

// JoinsScriptTests contains self-contained joins script tests.
var JoinsScriptTests = []ScriptTest{
	{
		Name: "outer join finish unmatched right side",
		SetUpScript: []string{
			`
CREATE TABLE teams (
  team VARCHAR(100),
  namespace VARCHAR(100)
);`,
			"INSERT INTO teams(team, namespace) VALUES ('sam', 'sam1');",
			"INSERT INTO teams(team, namespace) VALUES ('sam', 'sam2');",
			"INSERT INTO teams(team, namespace) VALUES ('janos', 'janos1');",
			`CREATE TABLE traces (
  namespace VARCHAR(100),
  value INT
);`,
			"INSERT INTO traces(namespace, value) VALUES ('janos1', '400');",
			"INSERT INTO traces(namespace, value) VALUES ('0', '500');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT  team,  sum(value) FROM traces FULL OUTER JOIN teams ON teams.namespace = traces.namespace GROUP BY team;",
				Expected: []sql.Row{{"sam", nil}, {"janos", float64(400)}, {nil, float64(500)}},
			},
			{
				Query:    "SELECT  team,  sum(value) FROM teams FULL OUTER JOIN traces ON teams.namespace = traces.namespace GROUP BY team;",
				Expected: []sql.Row{{"sam", nil}, {"janos", float64(400)}, {nil, float64(500)}},
			},
		},
	},
	{
		Name: "filter pushdown through join uppercase name",
		SetUpScript: []string{
			"create table A (A int primary key);",
			"insert into A values (0),(1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "select /*+ JOIN_ORDER(A, b) */ * from A join A b where a.A = 1 and b.A = 1",
				ExpectedIndexes: []string{"primary", "primary"},
			},
		},
	},
	{
		Name: "3 tables, linear join",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select ya from a join b on ya - 1= xb join c on xc = zb - 2",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		Name: "3 tables, v join",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select za from a join b on ya - 1 = xb join c on xa = xc",
				Expected: []sql.Row{{3}},
			},
		},
	},
	{
		Name: "3 tables, linear join, indexes on A,C",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a join b on xa = yb - 1 join c on yb - 1 = xc",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "4 tables, linear join",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a join b on ya - 1 = xb join c on xb = xc join d on xc = xd",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "4 tables, linear join, index on D",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a join b on ya = yb join c on yb = yc join d on yc - 1 = xd",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "4 tables, left join, indexes on all tables",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a left join b on ya = yb left join c on yb = yc left join d on yc - 1 = xd",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "4 tables, linear join, index on B, D",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a join b on ya - 1 = xb join c on yc = za - 1 join d on yc - 1 = xd",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "4 tables, all joined to A",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from a join b on xa = xb join c on ya - 1 = xc join d on za - 2 = xd",
				Expected: []sql.Row{{1}},
			},
		},
	},
	// {
	// 	Name: "4 tables, all joined to D",
	// 	SetUpScript: []string{
	// 		"create table a (xa int primary key, ya int, za int)",
	// 		"create table b (xb int primary key, yb int, zb int)",
	// 		"create table c (xc int primary key, yc int, zc int)",
	// 		"create table d (xd int primary key, yd int, zd int)",
	// 		"insert into a values (1,2,3)",
	// 		"insert into b values (1,2,3)",
	// 		"insert into c values (1,2,3)",
	// 		"insert into d values (1,2,3)",
	// 	},
	// 	Assertions: []ScriptTestAssertion{
	// 		{
	// 			// gives an error in mysql, a needs an alias
	// 			Query: "select xa from d join a on yd = xa join c on yd = xc join a on xa = yd",
	// 			Expected: []sql.Row{{1}},
	// 		},
	// 	},
	// },
	{
		Name: "4 tables, all joined to D",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select xa from d join a on yd - 1 = xa join c on zd - 2 = xc join b on xb = zd - 2",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "5 tables, complex join conditions",
		SetUpScript: []string{
			"create table a (xa int primary key, ya int, za int)",
			"create table b (xb int primary key, yb int, zb int)",
			"create table c (xc int primary key, yc int, zc int)",
			"create table d (xd int primary key, yd int, zd int)",
			"create table e (xe int, ye int, ze int, primary key(xe, ye))",
			"insert into a values (1,2,3)",
			"insert into b values (1,2,3)",
			"insert into c values (1,2,3)",
			"insert into d values (1,2,3)",
			"insert into e values (1,2,3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select xa from a
									join b on ya - 1 = xb
									join c on xc = za - 2
									join d on xd = yb - 1
									join e on xe = zb - 2 and ye = yc`,
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "Indexed Join On Keyless Table",
		SetUpScript: []string{
			"create table l (pk int primary key, c0 int, c1 int);",
			"create table r (c0 int, c1 int, third int);",
			"create index r_c0 on r (c0);",
			"create index r_c1 on r (c1);",
			"create index r_third on r (third);",
			"insert into l values (0, 0, 0), (1, 0, 1), (2, 1, 0), (3, 0, 2), (4, 2, 0), (5, 1, 2), (6, 2, 1), (7, 2, 2);",
			"insert into l values (256, 1024, 4096);",
			"insert into r values (1, 1, -1), (2, 2, -1), (2, 2, -1);",
			"insert into r values (-1, -1, 256);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select pk, l.c0, l.c1 from l join r on l.c0 = r.c0 or l.c1 = r.c1 order by 1, 2, 3;",
				Expected: []sql.Row{
					{1, 0, 1},
					{2, 1, 0},
					{3, 0, 2},
					{3, 0, 2},
					{4, 2, 0},
					{4, 2, 0},
					{5, 1, 2},
					{5, 1, 2},
					{5, 1, 2},
					{6, 2, 1},
					{6, 2, 1},
					{6, 2, 1},
					{7, 2, 2},
					{7, 2, 2},
				},
			},
			{
				Query: "select pk, l.c0, l.c1 from l join r on l.c0 = r.c0 or l.c1 = r.c1 or l.pk = r.third order by 1, 2, 3;",
				Expected: []sql.Row{
					{1, 0, 1},
					{2, 1, 0},
					{3, 0, 2},
					{3, 0, 2},
					{4, 2, 0},
					{4, 2, 0},
					{5, 1, 2},
					{5, 1, 2},
					{5, 1, 2},
					{6, 2, 1},
					{6, 2, 1},
					{6, 2, 1},
					{7, 2, 2},
					{7, 2, 2},
					{256, 1024, 4096},
				},
			},
			{
				Query: "select pk, l.c0, l.c1 from l join r on l.c0 = r.c0 or l.c1 < 4 and l.c1 = r.c1 or l.c1 >= 4 and l.c1 = r.c1 order by 1, 2, 3;",
				Expected: []sql.Row{
					{1, 0, 1},
					{2, 1, 0},
					{3, 0, 2},
					{3, 0, 2},
					{4, 2, 0},
					{4, 2, 0},
					{5, 1, 2},
					{5, 1, 2},
					{5, 1, 2},
					{6, 2, 1},
					{6, 2, 1},
					{6, 2, 1},
					{7, 2, 2},
					{7, 2, 2},
				},
			},
		},
	},
	{
		Name: "JOIN on non-index-prefix columns do not panic (Dolt Issue #2366)",
		SetUpScript: []string{
			"CREATE TABLE `player_season_stat_totals` (`player_id` int NOT NULL, `team_id` int NOT NULL, `season_id` int NOT NULL, `minutes` int, `games_started` int, `games_played` int, `2pm` int, `2pa` int, `3pm` int, `3pa` int, `ftm` int, `fta` int, `ast` int, `stl` int, `blk` int, `tov` int, `pts` int, `orb` int, `drb` int, `trb` int, `pf` int, `season_type_id` int NOT NULL, `league_id` int NOT NULL DEFAULT 0, PRIMARY KEY (`player_id`,`team_id`,`season_id`,`season_type_id`,`league_id`));",
			"CREATE TABLE `team_seasons` (`team_id` int NOT NULL, `league_id` int NOT NULL, `season_id` int NOT NULL, `prefix` varchar(100), `nickname` varchar(100), `abbreviation` varchar(100), `city` varchar(100), `state` varchar(100), `country` varchar(100), PRIMARY KEY (`team_id`,`league_id`,`season_id`));",
			"INSERT INTO player_season_stat_totals VALUES (1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1);",
			"INSERT INTO team_seasons VALUES (1,1,1,'','','','','','');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT stats.* FROM player_season_stat_totals stats LEFT JOIN team_seasons ON team_seasons.team_id = stats.team_id AND team_seasons.season_id = stats.season_id;",
				Expected: []sql.Row{{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/3065
		Name: "join index lookups do not handle filters",
		SetUpScript: []string{
			"create table a (x int primary key)",
			"create table b (y int primary key, x int, index idx_x(x))",
			"create table c (z int primary key, x int, y int, index idx_x(x))",
			"insert into a values (0),(1),(2),(3)",
			"insert into b values (0,1), (1,1), (2,2), (3,2)",
			"insert into c values (0,1,0), (1,1,0), (2,2,1), (3,2,1)",
		},
		Query: "select a.* from a join b on a.x = b.x join c where c.x = a.x and b.x = 1",
		Expected: []sql.Row{
			{1},
			{1},
			{1},
			{1},
		},
	},
	{
		Name:    "hash lookup for joins works with binary",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table uv (u int primary key, v int);",
			"create table xy (x int primary key, y int);",
			"insert into uv values (0,0), (1,1), (2,2);",
			"insert into xy values (0,0), (1,1), (2,2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select uv.u from uv join xy on binary xy.x = binary uv.u;",
				Expected: []sql.Row{
					{0},
					{1},
					{2},
				},
			},
		},
	},
	{
		Name: "subquery with range heap join",
		SetUpScript: []string{
			"create table a (i int primary key, start int, end int, name varchar(32));",
			"insert into a values (1, 603000, 605001, 'test');",
			"create table b (i int primary key);",
			"insert into b values (600000), (605000), (608000);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select a.i from (select 'test' as name) sq join a on sq.name = a.name join b on b.i between a.start and a.end;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from (select 'test' as name, 1 as x, 2 as y, 3 as z) sq join a on sq.name = a.name join b on b.i between a.start and a.end;",
				Expected: []sql.Row{
					{"test", 1, 2, 3, 1, 603000, 605001, "test", 605000},
				},
			},
		},
	},
	{
		Name: "many joins with chain of ANDs",
		SetUpScript: []string{
			"create table t1  (a1  int primary key, b1  int);",
			"create table t2  (a2  int primary key, b2  int);",
			"create table t3  (a3  int primary key, b3  int);",
			"create table t4  (a4  int primary key, b4  int);",
			"create table t5  (a5  int primary key, b5  int);",
			"create table t6  (a6  int primary key, b6  int);",
			"create table t7  (a7  int primary key, b7  int);",
			"create table t8  (a8  int primary key, b8  int);",
			"create table t9  (a9  int primary key, b9  int);",
			"create table t10 (a10 int primary key, b10 int);",
			"insert into t1 values  (1, 1);",
			"insert into t2 values  (1, 1);",
			"insert into t3 values  (1, 1);",
			"insert into t4 values  (1, 1);",
			"insert into t5 values  (1, 1);",
			"insert into t6 values  (1, 1);",
			"insert into t7 values  (1, 1);",
			"insert into t8 values  (1, 1);",
			"insert into t9 values  (1, 1);",
			"insert into t10 values (1, 1);",
			"insert into t1 values  (2, 2);",
			"insert into t2 values  (2, 2);",
			"insert into t3 values  (2, 2);",
			"insert into t4 values  (2, 2);",
			"insert into t5 values  (2, 2);",
			"insert into t6 values  (2, 2);",
			"insert into t7 values  (2, 2);",
			"insert into t8 values  (2, 2);",
			"insert into t9 values  (2, 2);",
			"insert into t10 values (2, 2);",
			"insert into t1 values  (3, 3);",
			"insert into t2 values  (3, 3);",
			"insert into t3 values  (3, 3);",
			"insert into t4 values  (3, 3);",
			"insert into t5 values  (3, 3);",
			"insert into t6 values  (3, 3);",
			"insert into t7 values  (3, 3);",
			"insert into t8 values  (3, 3);",
			"insert into t9 values  (3, 3);",
			"insert into t10 values (3, 3);",
			"insert into t1 values  (4, 4);",
			"insert into t2 values  (4, 4);",
			"insert into t3 values  (4, 4);",
			"insert into t4 values  (4, 4);",
			"insert into t5 values  (4, 4);",
			"insert into t6 values  (4, 4);",
			"insert into t7 values  (4, 4);",
			"insert into t8 values  (4, 4);",
			"insert into t9 values  (4, 4);",
			"insert into t10 values (4, 4);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
select 
    a1, a2, a3, a4, a5, a6, a7, a8, a9, a10
from
    t1, t2, t3, t4, t5, t6, t7, t8, t9, t10
where
      1 = a3  and
     b9 = a3  and
     b2 = a9  and
    b10 = a2  and
     b5 = a10 and
     b7 = a5  and
     b4 = a7  and
     b1 = a4  and
     b8 = a1  and
     b6 = a8
;
`,
				Expected: []sql.Row{
					{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
				},
			},
		},
	},
	{
		Name:    "test parenthesized tables",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t1 (i int);",
			"insert into t1 values (1), (2), (3);",
			"create table t2 (j int);",
			"insert into t2 values (1), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from (t1)",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select * from (((((t1)))))",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select * from (((((t1 as t11)))))",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select * from (t1) join t2 where t1.i = t2.j",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
				},
			},
			{
				Query: "select * from t1 join (t2) where t1.i = t2.j",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
				},
			},
			{
				Query: "select * from (t1) join (t2) where t1.i = t2.j",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
				},
			},
			{
				Query: "select * from ((((t1)))) join ((((t2)))) where t1.i = t2.j",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
				},
			},
			{
				Query: "select * from (t1 as t11) join (t2 as t22) where t11.i = t22.j",
				Expected: []sql.Row{
					{1, 1},
					{3, 3},
				},
			},
		},
	},
}
