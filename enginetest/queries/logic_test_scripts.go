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

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// SQLLogicJoinTests is a list of all the logic tests that are run against the sql engine.
var SQLLogicJoinTests = []ScriptTest{
	{
		Name: "joining on different types panics",
		SetUpScript: []string{
			"CREATE TABLE foo (a INT, b INT, c FLOAT, d FLOAT);",
			"INSERT INTO foo VALUES  (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3);",
			"CREATE TABLE bar (a INT, b FLOAT, c FLOAT, d INT);",
			"INSERT INTO bar VALUES (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// get field index error
				Skip:           true,
				Query:          "SELECT * FROM foo JOIN bar ON max(foo.c) < 2",
				ExpectedErrStr: "invalid use of group function",
			},
			{
				// SQLLogicTests incorrectly reports this as an error
				Query: "SELECT * FROM foo NATURAL JOIN bar",
				Expected: []sql.UntypedSqlRow{
					{1, 1, 1.0, 1.0},
					{2, 2, 2.0, 2.0},
					{3, 3, 3.0, 3.0},
				},
			},
			{
				Query: "SELECT * FROM foo JOIN bar USING (b);",
				Expected: []sql.UntypedSqlRow{
					{1, 1, 1.0, 1.0, 1, 1.0, 1},
					{2, 2, 2.0, 2.0, 2, 2.0, 2},
					{3, 3, 3.0, 3.0, 3, 3.0, 3},
				},
			},
			{
				Query: "SELECT * FROM foo JOIN bar USING (a, b);",
				Expected: []sql.UntypedSqlRow{
					{1, 1, 1.0, 1.0, 1.0, 1},
					{2, 2, 2.0, 2.0, 2.0, 2},
					{3, 3, 3.0, 3.0, 3.0, 3},
				},
			},
			{
				Query: "SELECT * FROM foo JOIN bar USING (a, b, c);",
				Expected: []sql.UntypedSqlRow{
					{1, 1, 1.0, 1.0, 1},
					{2, 2, 2.0, 2.0, 2},
					{3, 3, 3.0, 3.0, 3},
				},
			},
			{
				Query: "SELECT * FROM foo JOIN bar ON foo.b = bar.b;",
				Expected: []sql.UntypedSqlRow{
					{1, 1, 1.0, 1.0, 1, 1.0, 1.0, 1},
					{2, 2, 2.0, 2.0, 2, 2.0, 2.0, 2},
					{3, 3, 3.0, 3.0, 3, 3.0, 3.0, 3},
				},
			},
			{
				Query: "SELECT * FROM foo JOIN bar ON foo.a = bar.a AND foo.b = bar.b;",
				Expected: []sql.UntypedSqlRow{
					{1, 1, 1.0, 1.0, 1, 1.0, 1.0, 1},
					{2, 2, 2.0, 2.0, 2, 2.0, 2.0, 2},
					{3, 3, 3.0, 3.0, 3, 3.0, 3.0, 3},
				},
			},
			{
				Query: "SELECT * FROM foo, bar WHERE foo.b = bar.b;",
				Expected: []sql.UntypedSqlRow{
					{1, 1, 1.0, 1.0, 1, 1.0, 1.0, 1},
					{2, 2, 2.0, 2.0, 2, 2.0, 2.0, 2},
					{3, 3, 3.0, 3.0, 3, 3.0, 3.0, 3},
				},
			},
			{
				Query: "SELECT * FROM foo, bar WHERE foo.a = bar.a AND foo.b = bar.b;",
				Expected: []sql.UntypedSqlRow{
					{1, 1, 1.0, 1.0, 1, 1.0, 1.0, 1},
					{2, 2, 2.0, 2.0, 2, 2.0, 2.0, 2},
					{3, 3, 3.0, 3.0, 3, 3.0, 3.0, 3},
				},
			},
		},
	},
	{
		Name: "case insensitive join with using clause",
		SetUpScript: []string{
			"CREATE TABLE str1 (a INT PRIMARY KEY, s TEXT COLLATE utf8mb4_0900_ai_ci);",
			"INSERT INTO str1 VALUES (1, 'a' COLLATE utf8mb4_0900_ai_ci), (2, 'A' COLLATE utf8mb4_0900_ai_ci), (3, 'c' COLLATE utf8mb4_0900_ai_ci), (4, 'D' COLLATE utf8mb4_0900_ai_ci);",
			"CREATE TABLE str2 (a INT PRIMARY KEY, s TEXT COLLATE utf8mb4_0900_ai_ci);",
			"INSERT INTO str2 VALUES (1, 'A' COLLATE utf8mb4_0900_ai_ci), (2, 'B' COLLATE utf8mb4_0900_ai_ci), (3, 'C' COLLATE utf8mb4_0900_ai_ci), (4, 'E' COLLATE utf8mb4_0900_ai_ci);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Skip:  true,
				Query: "SELECT s, str1.s, str2.s FROM str1 INNER JOIN str2 USING(s);",
				Expected: []sql.UntypedSqlRow{
					{"A", "A", "A"},
					{"a", "a", "A"},
					{"c", "c", "C"},
				},
			},
			{
				Query: "SELECT s, str1.s, str2.s FROM str1 LEFT OUTER JOIN str2 USING(s)",
				Expected: []sql.UntypedSqlRow{
					{"a", "a", "A"},
					{"A", "A", "A"},
					{"c", "c", "C"},
					{"D", "D", nil},
				},
			},
			{
				Query: "SELECT s, str1.s, str2.s FROM str1 RIGHT OUTER JOIN str2 USING(s)",
				Expected: []sql.UntypedSqlRow{
					{"A", "A", "A"},
					{"A", "a", "A"},
					{"B", nil, "B"},
					{"C", "c", "C"},
					{"E", nil, "E"},
				},
			},
		},
	},
	{
		Name: "values and rows",
		SetUpScript: []string{
			"CREATE TABLE xy (x INT PRIMARY KEY, y INT)",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Syntax error
				Query: "INSERT INTO xy (VALUES ROW(1, 1))",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
		},
	},
	{
		Name: "using join",
		SetUpScript: []string{
			"CREATE TABLE abcd (a INT, b INT, c INT, d INT);",
			"INSERT INTO abcd VALUES (1, 1, 1, 1), (2, 2, 2, 2);",
			"CREATE TABLE dxby (d INT, x INT, b INT, y INT);",
			"INSERT INTO dxby VALUES (2, 2, 2, 2), (3, 3, 3, 3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT abcd.*, dxby.* FROM abcd INNER JOIN dxby USING (d, b);",
				Expected: []sql.UntypedSqlRow{
					{2, 2, 2, 2, 2, 2, 2, 2},
				},
			},
		},
	},
}

// SQLLogicSubqueryTests is a list of all the logic tests that are run against the sql engine.
var SQLLogicSubqueryTests = []ScriptTest{
	{
		Name: "exists, in, all, any subquery",
		SetUpScript: []string{
			"CREATE TABLE c (c_id INT PRIMARY KEY, bill TEXT);",
			"CREATE TABLE o (o_id INT PRIMARY KEY, c_id INT, ship TEXT);",
			"INSERT INTO c VALUES (1, 'CA'), (2, 'TX'), (3, 'MA'), (4, 'TX'), (5, NULL), (6, 'FL');",
			"INSERT INTO o VALUES (10, 1, 'CA'), (20, 1, 'CA'), (30, 1, 'CA'), (40, 2, 'CA'), (50, 2, 'TX'), (60, 2, NULL), (70, 4, 'WY'), (80, 4, NULL), (90, 6, 'WA');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM c WHERE EXISTS(SELECT * FROM o WHERE o.c_id=c.c_id);",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{2, "TX"},
					{4, "TX"},
					{6, "FL"},
				},
			},
			{
				Query: "SELECT * FROM c WHERE NOT EXISTS(SELECT * FROM o WHERE o.c_id=c.c_id);",
				Expected: []sql.UntypedSqlRow{
					{3, "MA"},
					{5, nil},
				},
			},
			{
				Query: "SELECT * FROM c WHERE EXISTS(SELECT * FROM o WHERE o.c_id=c.c_id) OR NOT EXISTS(SELECT * FROM o WHERE o.c_id=c.c_id);",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{2, "TX"},
					{3, "MA"},
					{4, "TX"},
					{5, nil},
					{6, "FL"},
				},
			},
			{
				Query: "SELECT * FROM c WHERE EXISTS(SELECT * FROM o WHERE o.c_id=c.c_id AND c.bill='TX');",
				Expected: []sql.UntypedSqlRow{
					{2, "TX"},
					{4, "TX"},
				},
			},
			{
				Query: "SELECT * FROM c WHERE 'WY' IN (SELECT ship FROM o WHERE o.c_id=c.c_id);",
				Expected: []sql.UntypedSqlRow{
					{4, "TX"},
				},
			},
			{
				Query: "SELECT * FROM c WHERE 'WY' IN (SELECT ship FROM o WHERE o.c_id=c.c_id) OR 'WA' IN (SELECT ship FROM o WHERE o.c_id=c.c_id);",
				Expected: []sql.UntypedSqlRow{
					{4, "TX"},
					{6, "FL"},
				},
			},
			{
				Query: "SELECT * FROM c WHERE 'CA' IN (SELECT ship FROM o WHERE o.c_id=c.c_id) AND 'TX' NOT IN (SELECT ship FROM o WHERE o.c_id=c.c_id);",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
				},
			},
			{
				Query: "SELECT * FROM c WHERE bill IN (SELECT ship FROM o WHERE o.c_id=c.c_id);",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{2, "TX"},
				},
			},
			{
				Skip:  true,
				Query: "SELECT * FROM c WHERE bill = ALL(SELECT ship FROM o WHERE o.c_id=c.c_id);",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{3, "MA"},
					{5, nil},
				},
			},
			{
				Query: "SELECT * FROM c WHERE bill NOT IN (SELECT ship FROM o WHERE o.c_id=c.c_id);",
				Expected: []sql.UntypedSqlRow{
					{3, "MA"},
					{5, nil},
					{6, "FL"},
				},
			},
			{
				Query: "SELECT * FROM c WHERE bill NOT IN (SELECT ship FROM o WHERE o.c_id=c.c_id AND ship IS NOT NULL);",
				Expected: []sql.UntypedSqlRow{
					{3, "MA"},
					{4, "TX"},
					{5, nil},
					{6, "FL"},
				},
			},
			{
				Query: "SELECT * FROM c WHERE bill NOT IN (SELECT ship FROM o WHERE o.c_id=c.c_id AND ship IS NULL);",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{3, "MA"},
					{5, nil},
					{6, "FL"},
				},
			},
			{
				Skip:  true,
				Query: "SELECT * FROM c WHERE bill < ANY(SELECT ship FROM o WHERE o.c_id=c.c_id);",
				Expected: []sql.UntypedSqlRow{
					{4, "TX"},
					{6, "FL"},
				},
			},
			{
				Skip:  true,
				Query: "SELECT * FROM c WHERE bill < SOME(SELECT ship FROM o WHERE o.c_id=c.c_id);",
				Expected: []sql.UntypedSqlRow{
					{4, "TX"},
					{6, "FL"},
				},
			},
			{
				Skip:  true,
				Query: "SELECT * FROM c WHERE (bill < ANY(SELECT ship FROM o WHERE o.c_id=c.c_id)) IS NULL;",
				Expected: []sql.UntypedSqlRow{
					{2, "TX"},
				},
			},
			{
				Skip:  true,
				Query: "SELECT * FROM c WHERE (bill < ANY(SELECT ship FROM o WHERE o.c_id=c.c_id)) IS NOT NULL;",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{3, "MA"},
					{4, "TX"},
					{5, nil},
					{6, "FL"},
				},
			},
			{
				Skip:  true,
				Query: "SELECT * FROM c WHERE bill > ANY(SELECT ship FROM o WHERE o.c_id=c.c_id);",
				Expected: []sql.UntypedSqlRow{
					{2, "TX"},
				},
			},
			{
				Skip:  true,
				Query: "SELECT * FROM c WHERE (bill > ANY(SELECT ship FROM o WHERE o.c_id=c.c_id)) IS NULL;",
				Expected: []sql.UntypedSqlRow{
					{4, "TX"},
				},
			},
			{
				Skip:  true,
				Query: "SELECT * FROM c WHERE (bill > ANY(SELECT ship FROM o WHERE o.c_id=c.c_id)) IS NOT NULL;",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{3, "MA"},
					{4, "TX"},
					{5, nil},
					{6, "FL"},
				},
			},
			{
				Skip:  true,
				Query: "SELECT * FROM c WHERE bill = ANY(SELECT ship FROM o);",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{3, "MA"},
					{4, "TX"},
					{5, nil},
					{6, "FL"},
				},
			},
			{
				Skip:  true,
				Query: "SELECT * FROM c WHERE bill = ANY(SELECT ship FROM o) OR bill IS NULL;",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{3, "MA"},
					{4, "TX"},
					{5, nil},
				},
			},
			{
				Query: "SELECT * FROM c WHERE (NULL IN (SELECT ship FROM o WHERE o.c_id=c.c_id)) IS NOT NULL;",
				Expected: []sql.UntypedSqlRow{
					{3, "MA"},
					{5, nil},
				},
			},
			{
				Query: "SELECT * FROM c WHERE (NULL NOT IN (SELECT ship FROM o WHERE o.c_id=c.c_id)) IS NOT NULL;",
				Expected: []sql.UntypedSqlRow{
					{3, "MA"},
					{5, nil},
				},
			},
			{
				Query: "SELECT * FROM c WHERE (replace(bill, 'TX', 'WY') IN (SELECT ship FROM o WHERE o.c_id=c.c_id)) IS NULL;",
				Expected: []sql.UntypedSqlRow{
					{2, "TX"},
				},
			},
			{
				Skip:  true,
				Query: "SELECT * FROM c WHERE bill = ALL(SELECT ship FROM o WHERE o.c_id=c.c_id) OR EXISTS(SELECT * FROM o WHERE o.c_id=c.c_id AND ship='WY');",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{3, "MA"},
					{4, "TX"},
					{5, nil},
				},
			},
			{
				Skip:  true,
				Query: "SELECT * FROM c WHERE bill = ALL(SELECT ship FROM o WHERE o.c_id=c.c_id) AND EXISTS(SELECT * FROM o WHERE o.c_id=c.c_id);",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
				},
			},
			{
				Query: "SELECT * FROM c WHERE (SELECT count(*) FROM o WHERE o.c_id=c.c_id) > 1;",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{2, "TX"},
					{4, "TX"},
				},
			},
			{
				Query: "SELECT * FROM c WHERE (SELECT count(ship) FROM o WHERE o.c_id=c.c_id) > 1;",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{2, "TX"},
				},
			},
			{
				Query: "SELECT c.c_id, o.o_id, o.ship FROM c INNER JOIN o ON o.ship = (SELECT min(o.ship) FROM o WHERE o.c_id=c.c_id) ORDER BY c.c_id, o.o_id, o.ship;",
				Expected: []sql.UntypedSqlRow{
					{1, 10, "CA"},
					{1, 20, "CA"},
					{1, 30, "CA"},
					{1, 40, "CA"},
					{2, 10, "CA"},
					{2, 20, "CA"},
					{2, 30, "CA"},
					{2, 40, "CA"},
					{4, 70, "WY"},
					{6, 90, "WA"},
				},
			},
			{
				Query: "SELECT c.c_id, o.o_id, o.ship FROM c INNER JOIN o ON c.c_id=o.c_id AND o.ship = (SELECT min(o.ship) FROM o WHERE o.c_id=c.c_id) ORDER BY c.c_id, o.o_id, o.ship;",
				Expected: []sql.UntypedSqlRow{
					{1, 10, "CA"},
					{1, 20, "CA"},
					{1, 30, "CA"},
					{2, 40, "CA"},
					{4, 70, "WY"},
					{6, 90, "WA"},
				},
			},
			{
				Query: "SELECT c.c_id, o.ship, count(*) FROM c INNER JOIN o ON c.c_id=o.c_id WHERE (SELECT count(*) FROM o AS o2 WHERE o2.ship = o.ship AND o2.c_id = o.c_id) > (SELECT count(*) FROM o AS o2 WHERE o2.ship = o.ship AND o2.c_id <> o.c_id) GROUP BY c.c_id, o.ship;",
				Expected: []sql.UntypedSqlRow{
					{1, "CA", 3},
					{2, "TX", 1},
					{4, "WY", 1},
					{6, "WA", 1},
				},
			},
			{
				Query: "SELECT * FROM c WHERE (SELECT count(*) FROM o WHERE o.c_id=c.c_id) > 1 AND (SELECT max(ship) FROM o WHERE o.c_id=c.c_id) = 'CA';",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
				},
			},
			{
				Query: "SELECT * FROM c WHERE (SELECT count(*) FROM o WHERE o.c_id=c.c_id) > 1 OR EXISTS(SELECT ship FROM o WHERE o.c_id=c.c_id AND ship IS NULL);",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{2, "TX"},
					{4, "TX"},
				},
			},
			{
				Query: "SELECT c_id, bill FROM c AS c2 WHERE EXISTS(SELECT * FROM c WHERE bill=(SELECT max(ship) FROM o WHERE c_id=c2.c_id AND c_id=c.c_id));",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{2, "TX"},
				},
			},
			{
				Query: "SELECT c_id, bill FROM c AS c2 WHERE EXISTS(SELECT * FROM c WHERE bill=(SELECT min(ship) FROM o WHERE c_id=c2.c_id AND c_id=c.c_id));",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
				},
			},
			{
				Skip:  true,
				Query: "SELECT c_id, bill FROM c AS c2 WHERE EXISTS(SELECT * FROM c WHERE bill=(SELECT coalesce(min(ship), bill) FROM o WHERE c_id=c2.c_id AND c_id=c.c_id));",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{2, "TX"},
					{3, "MA"},
					{4, "TX"},
					{5, nil},
					{6, "FL"},
				},
			},
			{
				Query: "SELECT c_id, bill FROM c AS c2 WHERE EXISTS(SELECT * FROM (SELECT c_id, coalesce(ship, bill) AS state FROM o WHERE c_id=c2.c_id) AS o WHERE state=bill);",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{2, "TX"},
					{4, "TX"},
				},
			},
			{
				Query: "SELECT c_id, bill FROM c AS c2 WHERE EXISTS(SELECT * FROM (SELECT c_id, coalesce(ship, bill) AS state FROM o) AS o WHERE c_id = c2.c_id AND state = bill);",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{2, "TX"},
					{4, "TX"},
				},
			},
			{
				Query: "SELECT c_id, bill FROM c AS c2 WHERE EXISTS(SELECT * FROM (SELECT c_id, ship AS state FROM o) AS o WHERE c_id = c2.c_id AND coalesce(state, bill) = bill);",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{2, "TX"},
					{4, "TX"},
				},
			},
			{
				Query: "SELECT c_id, bill FROM c AS c2 WHERE EXISTS(SELECT c_id, ship FROM o WHERE c_id = c2.c_id AND coalesce(ship, bill) = bill);",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{2, "TX"},
					{4, "TX"},
				},
			},
			{
				Query: "SELECT c_id, bill FROM c AS c2 WHERE EXISTS(SELECT * FROM (SELECT c_id, ship AS state FROM o) AS o WHERE c_id = c2.c_id AND coalesce(state, bill) = bill);",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{2, "TX"},
					{4, "TX"},
				},
			},
			{
				Query: "SELECT * FROM c WHERE (SELECT ship FROM o WHERE o.c_id=c.c_id ORDER BY ship LIMIT 1) IS NOT NULL",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{6, "FL"},
				},
			},
			{
				Query: "SELECT * FROM c WHERE (SELECT ship FROM o WHERE o.c_id=c.c_id AND ship IS NOT NULL ORDER BY ship LIMIT 1)='CA' OR (SELECT ship FROM o WHERE o.c_id=c.c_id AND ship IS NOT NULL ORDER BY ship LIMIT 1)='WY' ORDER BY c_id",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{2, "TX"},
					{4, "TX"},
				},
			},
			{
				Query:    "SELECT * FROM c WHERE (SELECT o_id FROM o WHERE o.c_id=c.c_id AND ship='WY')=4;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query: "SELECT * FROM c WHERE c_id=(SELECT c_id FROM o WHERE ship='CA' AND c_id<>1 AND bill='TX');",
				Expected: []sql.UntypedSqlRow{
					{2, "TX"},
				},
			},
			{
				Query: "SELECT * FROM c WHERE c_id=(SELECT c_id FROM o WHERE ship='WA' AND bill='FL')",
				Expected: []sql.UntypedSqlRow{
					{6, "FL"},
				},
			},
			{
				Query: "SELECT * FROM c WHERE (SELECT ship  FROM o WHERE o.c_id=c.c_id AND ship IS NOT NULL AND (SELECT count(*) FROM o WHERE o.c_id=c.c_id)<=1)='WA';",
				Expected: []sql.UntypedSqlRow{
					{6, "FL"},
				},
			},
			{
				Query: "SELECT c_id, EXISTS(SELECT * FROM o WHERE o.c_id=c.c_id) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, true},
					{2, true},
					{3, false},
					{4, true},
					{5, false},
					{6, true},
				},
			},
			{
				Query: "SELECT c_id, NOT EXISTS(SELECT * FROM o WHERE o.c_id=c.c_id) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, false},
					{2, false},
					{3, true},
					{4, false},
					{5, true},
					{6, false},
				},
			},
			{
				Query: "SELECT c_id, EXISTS(SELECT * FROM o WHERE o.c_id=c.c_id) OR NOT EXISTS(SELECT * FROM o WHERE o.c_id=c.c_id) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, true},
					{2, true},
					{3, true},
					{4, true},
					{5, true},
					{6, true},
				},
			},
			{
				Query: "SELECT c_id, EXISTS(SELECT * FROM o WHERE o.c_id=c.c_id AND c.bill='TX') FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, false},
					{2, true},
					{3, false},
					{4, true},
					{5, false},
					{6, false},
				},
			},
			{
				Query: "SELECT c_id, 'WY' IN (SELECT ship FROM o WHERE o.c_id=c.c_id) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, false},
					{2, nil},
					{3, false},
					{4, true},
					{5, false},
					{6, false},
				},
			},
			{
				Query: "SELECT c_id, 'WY' IN (SELECT ship FROM o WHERE o.c_id=c.c_id) OR 'WA' IN (SELECT ship FROM o WHERE o.c_id=c.c_id) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, false},
					{2, nil},
					{3, false},
					{4, true},
					{5, false},
					{6, true},
				},
			},
			{
				Query: "SELECT c_id, 'CA' IN (SELECT ship FROM o WHERE o.c_id=c.c_id) AND 'TX' NOT IN (SELECT ship FROM o WHERE o.c_id=c.c_id) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, true},
					{2, false},
					{3, false},
					{4, nil},
					{5, false},
					{6, false},
				},
			},
			{
				Query: "SELECT c_id, bill IN (SELECT ship FROM o WHERE o.c_id=c.c_id) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, true},
					{2, true},
					{3, false},
					{4, nil},
					{5, false},
					{6, false},
				},
			},
			{
				Skip:  true,
				Query: "SELECT c_id, bill = ALL(SELECT ship FROM o WHERE o.c_id=c.c_id) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, 1},
					{2, 0},
					{3, 1},
					{4, 0},
					{5, true},
					{6, 0},
				},
			},
			{
				Query: "SELECT c_id, bill NOT IN (SELECT ship FROM o WHERE o.c_id=c.c_id) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, false},
					{2, false},
					{3, true},
					{4, nil},
					{5, true},
					{6, true},
				},
			},
			{
				Query: "SELECT c_id, bill NOT IN (SELECT ship FROM o WHERE o.c_id=c.c_id AND ship IS NOT NULL) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, false},
					{2, false},
					{3, true},
					{4, true},
					{5, true},
					{6, true},
				},
			},
			{
				Query: "SELECT c_id, bill NOT IN (SELECT ship FROM o WHERE o.c_id=c.c_id AND ship IS NULL) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, true},
					{2, nil},
					{3, true},
					{4, nil},
					{5, true},
					{6, true},
				},
			},
			{
				Skip:  true,
				Query: "SELECT c_id, bill < ANY(SELECT ship FROM o WHERE o.c_id=c.c_id) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, false},
					{2, nil},
					{3, false},
					{4, true},
					{5, false},
					{6, true},
				},
			},
			{
				Skip:  true,
				Query: "SELECT c_id, (bill < ANY(SELECT ship FROM o WHERE o.c_id=c.c_id)) IS NULL FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, false},
					{2, true},
					{3, false},
					{4, false},
					{5, false},
					{6, false},
				},
			},
			{
				Skip:  true,
				Query: "SELECT c_id, (bill < ANY(SELECT ship FROM o WHERE o.c_id=c.c_id)) IS NOT NULL FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, true},
					{2, false},
					{3, true},
					{4, true},
					{5, true},
					{6, true},
				},
			},
			{
				Skip:  true,
				Query: "SELECT c_id, bill > ANY(SELECT ship FROM o WHERE o.c_id=c.c_id) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, false},
					{2, true},
					{3, false},
					{4, nil},
					{5, false},
					{6, false},
				},
			},
			{
				Skip:  true,
				Query: "SELECT c_id, (bill > ANY(SELECT ship FROM o WHERE o.c_id=c.c_id)) IS NULL FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, false},
					{2, false},
					{3, false},
					{4, true},
					{5, false},
					{6, false},
				},
			},
			{
				Skip:  true,
				Query: "SELECT c_id, (bill > ANY(SELECT ship FROM o WHERE o.c_id=c.c_id)) IS NOT NULL FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, true},
					{2, true},
					{3, true},
					{4, false},
					{5, true},
					{6, true},
				},
			},
			{
				Skip:  true,
				Query: "SELECT c_id, bill = ANY(SELECT ship FROM o WHERE ship IS NOT NULL) FROM c;",
				Expected: []sql.UntypedSqlRow{
					{1, true},
					{2, true},
					{3, false},
					{4, true},
					{5, nil},
					{6, false},
				},
			},
			{
				Skip:  true,
				Query: "SELECT c_id, bill = ANY(SELECT ship FROM o WHERE ship IS NOT NULL) OR bill IS NULL FROM c;",
				Expected: []sql.UntypedSqlRow{
					{1, true},
					{2, true},
					{3, false},
					{4, true},
					{5, true},
					{6, false},
				},
			},
			{
				Query: "SELECT c_id, (NULL IN (SELECT ship FROM o WHERE o.c_id=c.c_id)) IS NOT NULL FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, false},
					{2, false},
					{3, true},
					{4, false},
					{5, true},
					{6, false},
				},
			},
			{
				Query: "SELECT c_id, (NULL NOT IN (SELECT ship FROM o WHERE o.c_id=c.c_id)) IS NOT NULL FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, false},
					{2, false},
					{3, true},
					{4, false},
					{5, true},
					{6, false},
				},
			},
			{
				Query: "SELECT c_id, (replace(bill, 'TX', 'WY') IN (SELECT ship FROM o WHERE o.c_id=c.c_id)) IS NULL FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, false},
					{2, true},
					{3, false},
					{4, false},
					{5, false},
					{6, false},
				},
			},
			{
				Skip:  true,
				Query: "SELECT c_id, bill = ALL(SELECT ship FROM o WHERE o.c_id=c.c_id) OR EXISTS(SELECT * FROM o WHERE o.c_id=c.c_id AND ship='WY') FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, true},
					{2, false},
					{3, true},
					{4, true},
					{5, true},
					{6, false},
				},
			},
			{
				Skip:  true,
				Query: "SELECT c_id, bill = ALL(SELECT ship FROM o WHERE o.c_id=c.c_id) AND EXISTS(SELECT * FROM o WHERE o.c_id=c.c_id) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, true},
					{2, false},
					{3, false},
					{4, false},
					{5, false},
					{6, false},
				},
			},
			{
				Query: "SELECT * FROM c WHERE (SELECT min(ship) FROM o WHERE o.c_id=c.c_id) IN (SELECT ship FROM o WHERE o.c_id=c.c_id);",
				Expected: []sql.UntypedSqlRow{
					{1, "CA"},
					{2, "TX"},
					{4, "TX"},
					{6, "FL"},
				},
			},
			{
				Query: "SELECT c_id, (SELECT min(ship) FROM o WHERE o.c_id=c.c_id) IN (SELECT ship FROM o WHERE o.c_id=c.c_id) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, true},
					{2, true},
					{3, false},
					{4, true},
					{5, false},
					{6, true},
				},
			},
			{
				Query: "SELECT max((SELECT count(*) FROM o WHERE o.c_id=c.c_id)) FROM c;",
				Expected: []sql.UntypedSqlRow{
					{3},
				},
			},
			{
				Query: "SELECT c_id, (SELECT count(*) FROM o WHERE o.c_id=c.c_id) FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, 3},
					{2, 3},
					{3, 0},
					{4, 2},
					{5, 0},
					{6, 1},
				},
			},
			{
				Query: "SELECT s.st, (SELECT count(*) FROM c WHERE c.bill=s.st) + (SELECT count(*) FROM o WHERE o.ship=s.st) FROM (SELECT c.bill AS st FROM c UNION SELECT o.ship AS st FROM o) s ORDER BY s.st;",
				Expected: []sql.UntypedSqlRow{
					{nil, 0},
					{"CA", 5},
					{"FL", 1},
					{"MA", 1},
					{"TX", 3},
					{"WA", 1},
					{"WY", 1},
				},
			},
			{
				Query: "SELECT c.c_id, o.ship, count(*) AS cust, (SELECT count(*) FROM o AS o2 WHERE o2.ship = o.ship AND o2.c_id <> c.c_id) AS other  FROM c INNER JOIN o ON c.c_id=o.c_id GROUP BY c.c_id, o.ship;",
				Expected: []sql.UntypedSqlRow{
					{1, "CA", 3, 1},
					{2, "CA", 1, 3},
					{2, "TX", 1, 0},
					{2, nil, 1, 0},
					{4, "WY", 1, 0},
					{4, nil, 1, 0},
					{6, "WA", 1, 0},
				},
			},
			{
				Query: "SELECT c.c_id, o.o_id, (SELECT max(CASE WHEN c2.bill > o2.ship THEN c2.bill ELSE o2.ship END) FROM c AS c2, o AS o2 WHERE c2.c_id=o2.c_id AND c2.c_id=c.c_id)  FROM c LEFT JOIN o ON c.c_id=o.c_id ORDER BY c.c_id, o.o_id;",
				Expected: []sql.UntypedSqlRow{
					{1, 10, "CA"},
					{1, 20, "CA"},
					{1, 30, "CA"},
					{2, 40, "TX"},
					{2, 50, "TX"},
					{2, 60, "TX"},
					{3, nil, nil},
					{4, 70, "WY"},
					{4, 80, "WY"},
					{5, nil, nil},
					{6, 90, "WA"},
				},
			},
			{
				Query: "SELECT c.c_id, (SELECT ship FROM o WHERE o.c_id=c.c_id ORDER BY ship LIMIT 1) IS NOT NULL  FROM c ORDER BY c.c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, true},
					{2, false},
					{3, false},
					{4, false},
					{5, false},
					{6, true},
				},
			},
			{
				Query: "SELECT c.c_id, (SELECT ship FROM o WHERE o.c_id=c.c_id AND ship IS NOT NULL ORDER BY ship LIMIT 1)='CA' OR (SELECT ship FROM o WHERE o.c_id=c.c_id AND ship IS NOT NULL ORDER BY ship LIMIT 1)='WY' FROM c ORDER BY c_id;",
				Expected: []sql.UntypedSqlRow{
					{1, true},
					{2, true},
					{3, nil},
					{4, true},
					{5, nil},
					{6, false},
				},
			},
			{
				Skip:  true,
				Query: "SELECT * FROM (SELECT c_id AS c_c_id, bill FROM c) sq1, LATERAL (SELECT row_number() OVER () AS rownum FROM o WHERE c_id = c_c_id) sq2 ORDER BY c_c_id, bill, rownum;",
				Expected: []sql.UntypedSqlRow{
					{1, "CA", 1},
					{1, "CA", 2},
					{1, "CA", 3},
					{2, "TX", 1},
					{2, "TX", 2},
					{2, "TX", 3},
					{4, "TX", 1},
					{4, "TX", 2},
					{6, "FL", 1},
				},
			},
			{
				Query: "SELECT *  FROM (SELECT bill FROM c) sq1, LATERAL (SELECT row_number() OVER (PARTITION BY bill) AS rownum FROM o WHERE ship = bill) sq2 ORDER BY bill, rownum;",
				Expected: []sql.UntypedSqlRow{
					{"CA", 1},
					{"CA", 2},
					{"CA", 3},
					{"CA", 4},
					{"TX", 1},
					{"TX", 1},
				},
			},
			{
				Skip:  true,
				Query: "SELECT (SELECT count(*) FROM o WHERE o.c_id=c.c_id) AS order_cnt, count(*) AS cust_cnt  FROM c GROUP BY (SELECT count(*) FROM o WHERE o.c_id=c.c_id) ORDER BY (SELECT count(*) FROM o WHERE o.c_id=c.c_id) DESC;",
				Expected: []sql.UntypedSqlRow{
					{3, 2},
					{2, 1},
					{1, 1},
					{0, 2},
				},
			},
			{
				Query: "SELECT c_cnt, o_cnt, c_cnt + o_cnt AS total  FROM (VALUES ROW((SELECT count(*) FROM c), (SELECT count(*) FROM o))) AS v(c_cnt, o_cnt)  WHERE c_cnt > 0 AND o_cnt > 0;",
				Expected: []sql.UntypedSqlRow{
					{6, 9, 15},
				},
			},
			{
				Query: "SELECT c.c_id, o.o_id  FROM c INNER JOIN o ON c.c_id=o.c_id AND EXISTS(SELECT * FROM o WHERE o.c_id=c.c_id AND ship IS NULL);",
				Expected: []sql.UntypedSqlRow{
					{2, 40},
					{2, 50},
					{2, 60},
					{4, 70},
					{4, 80},
				},
			},
		},
	},
	//{
	//	Name: "multiple nested subquery",
	//	SetUpScript: []string{
	//		"CREATE TABLE `groups`(id SERIAL PRIMARY KEY, data JSON);",
	//		"INSERT INTO `groups`(data) VALUES('{\"name\": \"Group 1\", \"members\": [{\"name\": \"admin\", \"type\": \"USER\"}, {\"name\": \"user\", \"type\": \"USER\"}]}');",
	//		"INSERT INTO `groups`(data) VALUES('{\"name\": \"Group 2\", \"members\": [{\"name\": \"admin2\", \"type\": \"USER\"}]}');",
	//		"CREATE TABLE t32786 (id VARCHAR(36) PRIMARY KEY, parent_id VARCHAR(36), parent_path text);",
	//		"INSERT INTO t32786 VALUES ('3AAA2577-DBC3-47E7-9E85-9CC7E19CF48A', null, null);",
	//		"INSERT INTO t32786 VALUES ('5AE7EAFD-8277-4F41-83DE-0FD4B4482169', '3AAA2577-DBC3-47E7-9E85-9CC7E19CF48A', null);",
	//		"CREATE TABLE users (id INT8 NOT NULL, name VARCHAR(50), PRIMARY KEY (id));",
	//		"INSERT INTO users(id, name) VALUES (1, 'user1');",
	//		"INSERT INTO users(id, name) VALUES (2, 'user2');",
	//        "INSERT INTO users(id, name) VALUES (3, 'user3');",
	//        "CREATE TABLE stuff(id INT8 NOT NULL, date DATE, user_id INT8, PRIMARY KEY (id), FOREIGN KEY (user_id) REFERENCES users (id));",
	//        "INSERT INTO stuff(id, date, user_id) VALUES (1, '2007-10-15', 1);",
	//        "INSERT INTO stuff(id, date, user_id) VALUES (2, '2007-12-15', 1);",
	//        "INSERT INTO stuff(id, date, user_id) VALUES (3, '2007-11-15', 1);",
	//        "INSERT INTO stuff(id, date, user_id) VALUES (4, '2008-01-15', 2);",
	//        "INSERT INTO stuff(id, date, user_id) VALUES (5, '2007-06-15', 3);",
	//        "INSERT INTO stuff(id, date, user_id) VALUES (6, '2007-03-15', 3);",
	//	},
	//	Assertions: []ScriptTestAssertion{
	//		{
	//			Skip: true,
	//			Query: "SELECT users.id AS users_id, users.name AS users_name, stuff_1.id AS stuff_1_id, stuff_1.date AS stuff_1_date, stuff_1.user_id AS stuff_1_user_id FROM users LEFT JOIN stuff AS stuff_1 ON users.id = stuff_1.user_id AND stuff_1.id = (SELECT stuff_2.id FROM stuff AS stuff_2 WHERE stuff_2.user_id = users.id ORDER BY stuff_2.date DESC LIMIT 1) ORDER BY users.name;",
	//			Expected: []sql.UntypedSqlRow{
	//				{1, "user1", 2, 2007-12-15, 1},
	//				{2, "user2", 4, 2008-01-15, 2},
	//				{3, "user3", 5, 2007-06-15, 3},
	//			},
	//		},
	//	},
	//},
	{
		Name: "multiple nested subquery again",
		SetUpScript: []string{
			"CREATE TABLE IF NOT EXISTS t_48638 (`key` INT NOT NULL, `value` INTEGER NOT NULL, PRIMARY KEY (`key`, `value`));",
			"INSERT INTO t_48638 values (1, 4);",
			"INSERT INTO t_48638 values (4, 3);",
			"INSERT INTO t_48638 values (3, 2);",
			"INSERT INTO t_48638 values (4, 1);",
			"INSERT INTO t_48638 values (1, 2);",
			"INSERT INTO t_48638 values (6, 5);",
			"INSERT INTO t_48638 values (7, 8);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT *  FROM t_48638  WHERE `key` IN (WITH v AS (SELECT level1.`value` AS `value`, level1.`key`AS level1, level2.`key` AS level2, level3.`key` AS level3 FROM t_48638 AS level2 RIGHT JOIN (SELECT * FROM t_48638 WHERE `value` = 4) AS level1 ON level1.`value` = level2.`key`      LEFT JOIN (SELECT * FROM t_48638) AS level3 ON level3.`key` = level2.`value`  )  SELECT v.level1 FROM v WHERE v.level1 IS NOT NULL  UNION ALL SELECT v.level2 FROM v WHERE v.level2 IS NOT NULL  UNION ALL SELECT v.level3 FROM v WHERE v.level3 IS NOT NULL);",
				Expected: []sql.UntypedSqlRow{
					{1, 2},
					{1, 4},
					{3, 2},
					{4, 1},
					{4, 3},
				},
			},
		},
	},
}
