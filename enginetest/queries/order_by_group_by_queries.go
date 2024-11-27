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

package queries

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer/analyzererrors"
)

var OrderByGroupByScriptTests = []ScriptTest{
	{
		Name: "Basic order by/group by cases",
		SetUpScript: []string{
			"use mydb;",
			"create table members (id bigint primary key, team text);",
			"insert into members values (3,'red'), (4,'red'),(5,'orange'),(6,'orange'),(7,'orange'),(8,'purple');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select team as f from members order by id, f",
				Expected: []sql.UntypedSqlRow{{"red"}, {"red"}, {"orange"}, {"orange"}, {"orange"}, {"purple"}},
			},
			{
				Query: "SELECT team, COUNT(*) FROM members GROUP BY team ORDER BY 2",
				Expected: []sql.UntypedSqlRow{
					{"purple", int64(1)},
					{"red", int64(2)},
					{"orange", int64(3)},
				},
			},
			{
				Query: "SELECT team, COUNT(*) FROM members GROUP BY 1 ORDER BY 2",
				Expected: []sql.UntypedSqlRow{
					{"purple", int64(1)},
					{"red", int64(2)},
					{"orange", int64(3)},
				},
			},
			{
				Query:       "SELECT team, COUNT(*) FROM members GROUP BY team ORDER BY columndoesnotexist",
				ExpectedErr: sql.ErrColumnNotFound,
			},
			{
				Query:    "SELECT DISTINCT BINARY t1.id as id FROM members AS t1 JOIN members AS t2 ON t1.id = t2.id WHERE t1.id > 0 ORDER BY BINARY t1.id",
				Expected: []sql.UntypedSqlRow{{[]uint8{0x33}}, {[]uint8{0x34}}, {[]uint8{0x35}}, {[]uint8{0x36}}, {[]uint8{0x37}}, {[]uint8{0x38}}},
			},
			{
				Query:    "SELECT DISTINCT BINARY t1.id as id FROM members AS t1 JOIN members AS t2 ON t1.id = t2.id WHERE t1.id > 0 ORDER BY t1.id",
				Expected: []sql.UntypedSqlRow{{[]uint8{0x33}}, {[]uint8{0x34}}, {[]uint8{0x35}}, {[]uint8{0x36}}, {[]uint8{0x37}}, {[]uint8{0x38}}},
			},
			{
				Query:    "SELECT DISTINCT t1.id as id FROM members AS t1 JOIN members AS t2 ON t1.id = t2.id WHERE t2.id > 0 ORDER BY t1.id",
				Expected: []sql.UntypedSqlRow{{3}, {4}, {5}, {6}, {7}, {8}},
			},
			{
				// aliases from outer scopes can be used in a subquery's having clause.
				// https://github.com/dolthub/dolt/issues/4723
				Query:    "SELECT id as alias1, (SELECT alias1+1 group by alias1 having alias1 > 0) FROM members where id < 6;",
				Expected: []sql.UntypedSqlRow{{3, 4}, {4, 5}, {5, 6}},
			},
			{
				// columns from outer scopes can be used in a subquery's having clause.
				// https://github.com/dolthub/dolt/issues/4723
				Query:    "SELECT id, (SELECT UPPER(team) having id > 3) as upper_team FROM members where id < 6;",
				Expected: []sql.UntypedSqlRow{{3, nil}, {4, "RED"}, {5, "ORANGE"}},
			},
			{
				// When there is ambiguity between a reference in an outer scope and a reference in the current
				// scope, the reference in the innermost scope will be used.
				// https://github.com/dolthub/dolt/issues/4723
				Query:    "SELECT id, (SELECT -1 as id having id < 10) as upper_team FROM members where id < 6;",
				Expected: []sql.UntypedSqlRow{{3, -1}, {4, -1}, {5, -1}},
			},
		},
	},
	{
		Name: "Group by BINARY: https://github.com/dolthub/dolt/issues/6179",
		SetUpScript: []string{
			"create table t (s varchar(100));",
			"insert into t values ('abc'), ('def');",
			"create table t1 (b binary(3));",
			"insert into t1 values ('abc'), ('abc'), ('def'), ('abc'), ('def');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select binary s from t group by binary s order by binary s",
				Expected: []sql.UntypedSqlRow{
					{[]uint8("abc")},
					{[]uint8("def")},
				},
			},
			{
				Query: "select count(b), b from t1 group by b order by b",
				Expected: []sql.UntypedSqlRow{
					{3, []uint8("abc")},
					{2, []uint8("def")},
				},
			},
			{
				Query: "select binary s from t group by binary s order by s",
				Expected: []sql.UntypedSqlRow{
					{[]uint8("abc")},
					{[]uint8("def")},
				},
			},
		},
	},
	{
		Name: "https://github.com/dolthub/dolt/issues/3016",
		SetUpScript: []string{
			"CREATE TABLE `users` (`id` int NOT NULL AUTO_INCREMENT,  `username` varchar(255) NOT NULL,  PRIMARY KEY (`id`));",
			"INSERT INTO `users` (`id`,`username`) VALUES (1,'u2');",
			"INSERT INTO `users` (`id`,`username`) VALUES (2,'u3');",
			"INSERT INTO `users` (`id`,`username`) VALUES (3,'u4');",
			"CREATE TABLE `tweet` (`id` int NOT NULL AUTO_INCREMENT,  `user_id` int NOT NULL,  `content` text NOT NULL,  `timestamp` bigint NOT NULL,  PRIMARY KEY (`id`),  KEY `tweet_user_id` (`user_id`));",
			"INSERT INTO `tweet` (`id`,`user_id`,`content`,`timestamp`) VALUES (1,1,'meow',1647463727);",
			"INSERT INTO `tweet` (`id`,`user_id`,`content`,`timestamp`) VALUES (2,1,'purr',1647463727);",
			"INSERT INTO `tweet` (`id`,`user_id`,`content`,`timestamp`) VALUES (3,2,'hiss',1647463727);",
			"INSERT INTO `tweet` (`id`,`user_id`,`content`,`timestamp`) VALUES (4,3,'woof',1647463727);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT t1.username, COUNT(t1.id) FROM ((SELECT t2.id, t2.content, t3.username FROM tweet AS t2 INNER JOIN users AS t3 ON (-t2.user_id = -t3.id) WHERE (t3.username = 'u3')) UNION (SELECT t4.id, t4.content, `t5`.`username` FROM `tweet` AS t4 INNER JOIN users AS t5 ON (-t4.user_id = -t5.id) WHERE (t5.username IN ('u2', 'u4')))) AS t1 GROUP BY `t1`.`username` ORDER BY 1,2 DESC;",
				Expected: []sql.UntypedSqlRow{{"u2", 2}, {"u3", 1}, {"u4", 1}},
			},
			{
				Query:    "SELECT t1.username, COUNT(t1.id) AS ct FROM ((SELECT t2.id, t2.content, t3.username FROM tweet AS t2 INNER JOIN users AS t3 ON (-t2.user_id = -t3.id) WHERE (t3.username = 'u3')) UNION (SELECT t4.id, t4.content, `t5`.`username` FROM `tweet` AS t4 INNER JOIN users AS t5 ON (-t4.user_id = -t5.id) WHERE (t5.username IN ('u2', 'u4')))) AS t1 GROUP BY `t1`.`username` ORDER BY 1,2 DESC;",
				Expected: []sql.UntypedSqlRow{{"u2", 2}, {"u3", 1}, {"u4", 1}},
			},
			{
				Query:    "SELECT COUNT(id) as ct, user_id as uid FROM tweet GROUP BY tweet.user_id ORDER BY COUNT(id), user_id;",
				Expected: []sql.UntypedSqlRow{{1, 2}, {1, 3}, {2, 1}},
			},
			{
				Query:    "SELECT COUNT(tweet.id) as ct, user_id as uid FROM tweet GROUP BY tweet.user_id ORDER BY COUNT(id), user_id;",
				Expected: []sql.UntypedSqlRow{{1, 2}, {1, 3}, {2, 1}},
			},
			{
				Query:    "SELECT COUNT(id) as ct, user_id as uid FROM tweet GROUP BY tweet.user_id ORDER BY COUNT(tweet.id), user_id;",
				Expected: []sql.UntypedSqlRow{{1, 2}, {1, 3}, {2, 1}},
			},
			{
				Query:    "SELECT COUNT(id) as ct, user_id as uid FROM tweet GROUP BY tweet.user_id HAVING COUNT(tweet.id) > 0 ORDER BY COUNT(tweet.id), user_id;",
				Expected: []sql.UntypedSqlRow{{1, 2}, {1, 3}, {2, 1}},
			},
			{
				Query:    "SELECT COUNT(id) as ct, user_id as uid FROM tweet WHERE tweet.id is NOT NULL GROUP BY tweet.user_id ORDER BY COUNT(tweet.id), user_id;",
				Expected: []sql.UntypedSqlRow{{1, 2}, {1, 3}, {2, 1}},
			},
			{
				Query:    "SELECT COUNT(id) as ct, user_id as uid FROM tweet WHERE tweet.id is NOT NULL GROUP BY tweet.user_id HAVING COUNT(tweet.id) > 0 ORDER BY COUNT(tweet.id), user_id;",
				Expected: []sql.UntypedSqlRow{{1, 2}, {1, 3}, {2, 1}},
			},
			{
				Query:    "SELECT COUNT(id) as ct, user_id as uid FROM tweet WHERE tweet.id is NOT NULL GROUP BY tweet.user_id HAVING COUNT(tweet.id) > 0 ORDER BY COUNT(tweet.id), user_id LIMIT 1;",
				Expected: []sql.UntypedSqlRow{{1, 2}},
			},
		},
	},
	{
		Name: "Group by with decimal columns",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT column_0, sum(column_1) FROM (values row(1.00,1), row(1.00,3), row(2,2), row(2,5), row(3,9)) a group by 1 order by 1;",
				Expected: []sql.UntypedSqlRow{{"1.00", float64(4)}, {"2.00", float64(7)}, {"3.00", float64(9)}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/4739
		Name: "Validation for use of non-aggregated columns with implicit grouping of all rows",
		SetUpScript: []string{
			"CREATE TABLE t (num INTEGER, val DOUBLE);",
			"INSERT INTO t VALUES (1, 0.01), (2,0.5);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "SELECT AVG(val), LAST_VALUE(val) OVER w FROM t WINDOW w AS (ORDER BY num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);",
				ExpectedErr: sql.ErrNonAggregatedColumnWithoutGroupBy,
			},
			{
				Query:       "SELECT 1 + AVG(val) + 1, LAST_VALUE(val) OVER w FROM t WINDOW w AS (ORDER BY num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);",
				ExpectedErr: sql.ErrNonAggregatedColumnWithoutGroupBy,
			},
			{
				Query:       "SELECT AVG(1), 1 + LAST_VALUE(val) OVER w FROM t WINDOW w AS (ORDER BY num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);",
				ExpectedErr: sql.ErrNonAggregatedColumnWithoutGroupBy,
			},
			{
				// GMS currently allows this query to execute and chooses the first result for val.
				// To match MySQL's behavior, GMS should be throwing an ErrNonAggregatedColumnWithoutGroupBy error.
				Skip:        true,
				Query:       "select AVG(val), val from t;",
				ExpectedErr: sql.ErrNonAggregatedColumnWithoutGroupBy,
			},
			{
				// Test validation for a derived table opaque node
				Query:       "select * from (SELECT AVG(val), LAST_VALUE(val) OVER w FROM t WINDOW w AS (ORDER BY num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) as dt;",
				ExpectedErr: sql.ErrNonAggregatedColumnWithoutGroupBy,
			},
			{
				// Test validation for a union opaque node
				Query:       "select 1, 1 union SELECT AVG(val), LAST_VALUE(val) OVER w FROM t WINDOW w AS (ORDER BY num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);",
				ExpectedErr: sql.ErrNonAggregatedColumnWithoutGroupBy,
			},
			{
				// Test validation for a recursive CTE opaque node
				Query:       "select * from (with recursive a as (select 1 as c1, 1 as c2 union SELECT AVG(t.val), LAST_VALUE(t.val) OVER w FROM t WINDOW w AS (ORDER BY num RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) select * from a union select * from a limit 1) as dt;",
				ExpectedErr: sql.ErrNonAggregatedColumnWithoutGroupBy,
			},
		},
	},
	{
		Name: "group by with any_value()",
		SetUpScript: []string{
			"use mydb;",
			"create table members (id bigint primary key, team text);",
			"insert into members values (3,'red'), (4,'red'),(5,'orange'),(6,'orange'),(7,'orange'),(8,'purple');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select @@global.sql_mode",
				Expected: []sql.UntypedSqlRow{
					{"NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES"},
				},
			},
			{
				Query: "select @@session.sql_mode",
				Expected: []sql.UntypedSqlRow{
					{"NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES"},
				},
			},
			{
				Query: "select any_value(id), any_value(team) from members order by id",
				Expected: []sql.UntypedSqlRow{
					{3, "red"},
					{4, "red"},
					{5, "orange"},
					{6, "orange"},
					{7, "orange"},
					{8, "purple"},
				},
			},
		},
	},
	{
		Name: "group by with strict errors",
		SetUpScript: []string{
			"use mydb;",
			"create table members (id bigint primary key, team text);",
			"insert into members values (3,'red'), (4,'red'),(5,'orange'),(6,'orange'),(7,'orange'),(8,'purple');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select @@global.sql_mode",
				Expected: []sql.UntypedSqlRow{
					{"NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES"},
				},
			},
			{
				Query: "select @@session.sql_mode",
				Expected: []sql.UntypedSqlRow{
					{"NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES"},
				},
			},
			{
				Query:       "select id, team from members group by team",
				ExpectedErr: analyzererrors.ErrValidationGroupBy,
			},
		},
	},
	{
		Name: "Group by null handling",
		// https://github.com/dolthub/go-mysql-server/issues/1503
		SetUpScript: []string{
			"create table t (pk int primary key, c1 varchar(10));",
			"insert into t values (1, 'foo'), (2, 'foo'), (3, NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select c1, count(pk) from t group by c1;",
				Expected: []sql.UntypedSqlRow{
					{"foo", 2},
					{nil, 1},
				},
			},
			{
				Query: "select c1, count(c1) from t group by c1;",
				Expected: []sql.UntypedSqlRow{
					{"foo", 2},
					{nil, 0},
				},
			},
		},
	},
}
