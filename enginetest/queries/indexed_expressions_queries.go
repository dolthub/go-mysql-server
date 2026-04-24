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
	"github.com/dolthub/go-mysql-server/sql/plan"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
)

var IndexedExpressionsScriptTests = []ScriptTest{
	{
		Name: "filtering: IN list",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, name VARCHAR(100))",
			"INSERT INTO t VALUES (1,'Alice'),(2,'BOB'),(3,'Carol')",
			"CREATE INDEX idx_lower ON t ((LOWER(name)))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT pk FROM t WHERE LOWER(name) IN ('alice', 'bob', 'carol')",
				Expected:        []sql.Row{{1}, {2}, {3}},
				ExpectedIndexes: []string{"idx_lower"},
			},
		},
	},
	{
		Name: "filtering: all comparison operators, both orderings",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200);",
			"CREATE INDEX idx1 ON test ((C1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT pk FROM test WHERE C1*10 = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE (c1*10) = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE 1000 = (c1*10);",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE (c1*10) >= 1000;",
				Expected:        []sql.Row{{1}, {2}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE 1000 <= (c1*10);",
				Expected:        []sql.Row{{1}, {2}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk, c1*10 FROM test WHERE (c1*10) > 1;",
				Expected:        []sql.Row{{1, 1000}, {2, 2000}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk, c1*10 FROM test WHERE 1 < (c1*10);",
				Expected:        []sql.Row{{1, 1000}, {2, 2000}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE (c1*10) < 2000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE 2000 > (c1*10);",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE (c1*10) <= 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE 1000 >= (c1*10);",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE (c1*10) <=> 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE 1000 <=> (c1*10);",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				// != uses the index via a not-equal range scan; both orderings are supported.
				Query:           "SELECT pk FROM test WHERE c1*10 != 1000;",
				Expected:        []sql.Row{{2}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE 1000 != (c1*10);",
				Expected:        []sql.Row{{2}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				// Equivalent expression, but not syntactically identical. Current implementation
				// is expected NOT to match this to the functional index.
				Query:           "SELECT pk FROM test WHERE 10*c1 = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{},
			},
			{
				// Equivalent expression, but not syntactically identical. Current implementation
				// is expected NOT to match this to the functional index.
				Query:           "SELECT pk FROM test WHERE (c1*5)*2 = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{},
			},
			{
				// Equivalent expression, but not syntactically identical. Current implementation
				// is expected NOT to match this to the functional index.
				Query:           "SELECT pk FROM test WHERE c1*10+0 = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{},
			},
		},
	},
	{
		Name: "filtering: IS NULL, IS NOT NULL, NULL-safe equality",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, NULL), (2, NULL), (3, 100);",
			"CREATE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT pk FROM test WHERE c1*10 IS NULL ORDER BY pk;",
				Expected:        []sql.Row{{1}, {2}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE c1*10 IS NOT NULL ORDER BY pk;",
				Expected:        []sql.Row{{3}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE (c1*10) <=> NULL ORDER BY pk;",
				Expected:        []sql.Row{{1}, {2}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE NULL <=> (c1*10) ORDER BY pk;",
				Expected:        []sql.Row{{1}, {2}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE (c1*10) <=> 1000;",
				Expected:        []sql.Row{{3}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE 1000 <=> (c1*10);",
				Expected:        []sql.Row{{3}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE c1*10 = 1000;",
				Expected:        []sql.Row{{3}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:    "UPDATE test SET c1 = NULL WHERE pk = 3;",
				Expected: []sql.Row{{gmstypes.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:           "SELECT pk FROM test WHERE c1*10 IS NULL ORDER BY pk;",
				Expected:        []sql.Row{{1}, {2}, {3}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:    "UPDATE test SET c1 = 101 WHERE pk = 1;",
				Expected: []sql.Row{{gmstypes.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:           "SELECT pk FROM test WHERE c1*10 = 1010;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "filtering: LOWER function",
		SetUpScript: []string{
			"CREATE TABLE people (pk INT PRIMARY KEY, name VARCHAR(100));",
			"INSERT INTO people VALUES (1, 'Alice'), (2, 'ALICE'), (3, 'Bob'), (4, NULL);",
			"CREATE INDEX idx1 ON people ((LOWER(name)));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT pk FROM people WHERE LOWER(name) = 'alice' ORDER BY pk;",
				Expected:        []sql.Row{{1}, {2}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM people WHERE LOWER(name) IS NULL;",
				Expected:        []sql.Row{{4}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				// NOTE: as in MySQL, functional indexes are not used for projection values
				Query:           "SELECT pk, LOWER(name) FROM people ORDER BY pk;",
				Expected:        []sql.Row{{1, "alice"}, {2, "alice"}, {3, "bob"}, {4, nil}},
				ExpectedIndexes: nil,
			},
		},
	},
	{
		Name: "filtering: multi-column functional expression",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT, c2 INT);",
			"INSERT INTO test VALUES (1, 10, 20), (2, 30, 40);",
			"CREATE UNIQUE INDEX idx1 ON test (((c1+c2)*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT pk FROM test WHERE (c1+c2)*10 = 300;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE (c1+c2)*10 = 700;",
				Expected:        []sql.Row{{2}},
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "joins: arithmetic expression",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			// NOTE: For the indexed access plan to be chosen, we need to populate enough test data
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, -3), (4, -4), (5, -5), (6, -6), (7, -7), (8, -8), (9, -9), (10, -10), (11, -11), (12, -12), (13, -13), (14, -14), (15, -15), (16, -16), (17, -17), (18, -18), (19, -19), (20, -20), (21, -21), (22, -22), (23, -23), (24, -24), (25, -25), (26, -26), (27, -27), (28, -28), (29, -29);",

			// NOTE: The index must be unique in order for it to be used for strict lookups
			"CREATE UNIQUE INDEX idx1 ON test ((C1*10));",

			"CREATE TABLE t2 (t2pk INT PRIMARY KEY, t2c1 INT);",
			"INSERT INTO t2 VALUES (1, 1000), (2, -1), (3, -1), (4, -1), (5, -1), (6, -1), (7, -1), (8, -1), (9, -1), (10, -10);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT pk FROM test JOIN t2 ON (C1*10) = t2c1;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test JOIN t2 ON t2c1 = (c1*10);",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "joins: LOWER() function expression",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 VARCHAR(100));",
			// NOTE: For the indexed access plan to be chosen, we need to populate enough test data for the
			//       lookup cost to be less than a hash join.
			"INSERT INTO test VALUES (1, 'ONE'), (2, 'TWO'), (3, 'THREE'), (4, 'FOUR'), (5, 'FIVE'), (6, 'SIX'), (7, 'SEVEN'), (8, 'EIGHT'), (9, 'NINE'), (10, 'TEN'), (11, 'ELEVEN'), (12, 'TWELVE'), (13, 'THIRTEEN'), (14, 'FOURTEEN'), (15, 'FIFTEEN'), (16, 'SIXTEEN'), (17, 'SEVENTEEN'), (18, 'EIGHTEEN'), (19, 'NINETEEN'), (20, 'TWENTY'), (21, 'TWENTY-ONE'), (22, 'TWENTY-TWO'), (23, 'TWENTY-THREE'), (24, 'TWENTY-FOUR'), (25, 'TWENTY-FIVE'), (26, 'TWENTY-SIX'), (27, 'TWENTY-SEVEN'), (28, 'TWENTY-EIGHT'), (29, 'TWENTY-NINE'), (30, 'THIRTY');",

			// NOTE: The index must be unique in order for it to be used for strict lookups
			"CREATE UNIQUE INDEX idx1 ON test ((LOWER(c1)));",

			"CREATE TABLE t2 (pk INT PRIMARY KEY, c1 VARCHAR(100));",
			"INSERT INTO t2 VALUES (1, 'one');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT test.pk FROM test JOIN t2 ON lower(test.c1) = t2.c1;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "joins: expression with whitespace variations",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			// NOTE: For the indexed access plan to be chosen, we need to populate enough test data
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, -3), (4, -4), (5, -5), (6, -6), (7, -7), (8, -8), (9, -9), (10, -10), (11, -11), (12, -12), (13, -13), (14, -14), (15, -15), (16, -16), (17, -17), (18, -18), (19, -19), (20, -20), (21, -21), (22, -22), (23, -23), (24, -24), (25, -25), (26, -26), (27, -27), (28, -28), (29, -29), (30, -30);",

			// NOTE: The index must be unique in order for it to be used for strict lookups in a lookup join
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",

			"CREATE TABLE t2 (t2pk INT PRIMARY KEY, t2c1 INT);",
			"INSERT INTO t2 VALUES (1, 1000);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT pk FROM test JOIN t2 ON (c1 * 10) = t2c1;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "joins: index used regardless of table order",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500), (6, 600), (7, 700), (8, 800), (9, 900), (10, 1000), (11, 1100), (12, 1200), (13, 1300), (14, 1400), (15, 1500), (16, 1600), (17, 1700), (18, 1800), (19, 1900), (20, 2000), (21, 2100), (22, 2200), (23, 2300), (24, 2400), (25, 2500), (26, 2600), (27, 2700), (28, 2800), (29, 2900), (30, 3000);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
			"CREATE TABLE t2 (id INT PRIMARY KEY, val INT);",
			"INSERT INTO t2 VALUES (1, 1000), (2, 3000), (3, 9999);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT pk, id FROM test JOIN t2 ON c1*10 = val ORDER BY pk;",
				Expected:        []sql.Row{{1, 1}, {3, 2}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk, id FROM t2 JOIN test ON val = c1*10 ORDER BY pk;",
				Expected:        []sql.Row{{1, 1}, {3, 2}},
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "joins: three table join",
		SetUpScript: []string{
			// NOTE: Need about 30 rows before lookup join becomes the best join plan
			"CREATE TABLE t1 (pk INT PRIMARY KEY, c1 INT, payload VARCHAR(20));",
			"INSERT INTO t1 VALUES (1, 100, 'a'), (2, 200, 'b'), (3, 300, 'c'), (4, 400, 'd'), (5, 500, 'e'), (6, 600, 'f'), (7, 700, 'g'), (8, 800, 'h'), (9, 900, 'i'), (10, 1000, 'j'), (11, 1100, 'k'), (12, 1200, 'l'), (13, 1300, 'm'), (14, 1400, 'n'), (15, 1500, 'o'), (16, 1600, 'p'), (17, 1700, 'q'), (18, 1800, 'r'), (19, 1900, 's'), (20, 2000, 't'), (21, 2100, 'u'), (22, 2200, 'v'), (23, 2300, 'w'), (24, 2400, 'x'), (25, 2500, 'y'), (26, 2600, 'z'), (27, 2700, 'aa'), (28, 2800, 'bb'), (29, 2900, 'cc'), (30, 3000, 'dd'), (31, 3100, 'ee'), (32, 3200, 'ff');",
			"CREATE UNIQUE INDEX idx_expr ON t1 ((c1*10));",

			"CREATE TABLE t2 (pk INT PRIMARY KEY, expr_val INT, category_id INT);",
			"INSERT INTO t2 VALUES (1, 1000, 10), (2, 3000, 10), (3, 8000, 20), (4, 12345, 30);",
			"CREATE INDEX idx_expr_val ON t2 (expr_val);",
			"CREATE INDEX idx_category_id ON t2 (category_id);",

			"CREATE TABLE t3 (pk INT PRIMARY KEY, category_id INT, label VARCHAR(20));",
			"INSERT INTO t3 VALUES (1, 10, 'keep'), (2, 20, 'keep'), (3, 30, 'drop');",
			"CREATE INDEX idx_category ON t3 (category_id);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
				SELECT t1.pk, t2.pk, t3.label
				FROM t1
				JOIN t2 ON t1.c1*10 = t2.expr_val
				JOIN t3 ON t2.category_id = t3.category_id
				WHERE t3.label = 'keep'
				ORDER BY t1.pk
			`,
				Expected: []sql.Row{
					{1, 1, "keep"},
					{3, 2, "keep"},
					{8, 3, "keep"},
				},
				ExpectedIndexes: []string{"idx_category_id", "idx_category", "idx_expr"},
			},
		},
	},
	{
		Name: "projections: non-covering queries; index remains correct after ALTER TABLE ADD COLUMN",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200);",

			// NOTE: The index must be unique in order for it to be used for strict lookups in a join
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",

			"CREATE TABLE t2 (t2pk INT PRIMARY KEY, t2c1 INT);",
			"INSERT INTO t2 VALUES (1, 1000);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Alter the table to add a column AFTER the system hidden column so we can
				// test that we still pull the values out of the row correctly when there
				// is a system hidden column before the field location.
				Query:    "ALTER TABLE test ADD COLUMN c2 INT;",
				Expected: []sql.Row{{gmstypes.NewOkResult(0)}},
			},
			{
				Query: "SELECT * FROM test;",
				Expected: []sql.Row{
					{1, 100, nil},
					{2, 200, nil},
				},
			},
			{
				Query:    "INSERT INTO test VALUES (1000, 101, 201);",
				Expected: []sql.Row{{gmstypes.NewOkResult(1)}},
			},
			{
				Query: "SELECT * FROM test;",
				Expected: []sql.Row{
					{1, 100, nil},
					{2, 200, nil},
					{1000, 101, 201},
				},
			},
			{
				// When we have a filter predicate that matches the function expression in
				// our index, we expect the index to be used to execute the query.
				Query:           "SELECT pk FROM test WHERE c1*10 = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				// We're projecting the indexed expression, but we don't use the index, because either the
				// table or the index needs to be scanned, so we just use the table. This matches MySQL's
				// behavior, where it doesn't use indexed expressions for projection values.
				Query:    "SELECT pk, c1*10 FROM test;",
				Expected: []sql.Row{{1, 1000}, {2, 2000}, {1000, 1010}},
			},
		},
	},
	{
		Name: "sorting: indexed expression used for sort",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 20), (2, 10), (3, 30);",
			"CREATE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				// TODO: We don't currently use the secondary index for sort order, but this is an
				//       optimization we could add in the future.
				Skip:            true,
				Query:           "SELECT pk, c1*10 FROM test ORDER BY c1*10;",
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "index maintenance: secondary index kept in sync with INSERT/UPDATE/DELETE",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT, c2 INT);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
			"INSERT INTO test VALUES (1, 100, 5), (2, 200, 6);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT pk FROM test WHERE c1*10 = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:    "UPDATE test SET c1 = 101 WHERE pk = 1;",
				Expected: []sql.Row{{gmstypes.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:           "SELECT pk FROM test WHERE c1*10 = 1000;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk FROM test WHERE c1*10 = 1010;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:    "UPDATE test SET c2 = 999 WHERE pk = 1;",
				Expected: []sql.Row{{gmstypes.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:           "SELECT pk FROM test WHERE c1*10 = 1010;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:    "DELETE FROM test WHERE pk = 1;",
				Expected: []sql.Row{{gmstypes.NewOkResult(1)}},
			},
			{
				Query:           "SELECT pk FROM test WHERE c1*10 = 1010;",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "DML filtering: UPDATE and DELETE with expression in WHERE clause",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT, c2 INT);",
			"INSERT INTO test VALUES (1, 100, 0), (2, 200, 0), (3, 300, 0);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "UPDATE test SET c2 = 7 WHERE c1*10 = 1000;",
				Expected:        []sql.Row{{gmstypes.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:    "SELECT pk, c2 FROM test ORDER BY pk;",
				Expected: []sql.Row{{1, 7}, {2, 0}, {3, 0}},
			},
			{
				Query:           "DELETE FROM test WHERE c1*10 = 2000;",
				Expected:        []sql.Row{{gmstypes.NewOkResult(1)}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:    "SELECT pk FROM test ORDER BY pk;",
				Expected: []sql.Row{{1}, {3}},
			},
		},
	},
	{
		Name: "constraints: UNIQUE index enforces constraint on expression result",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
			"INSERT INTO test VALUES (1, 100);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "INSERT INTO test VALUES (2, 100);",
				ExpectedErrStr: "duplicate unique key given: [1000]",
			},
			{
				Query:          "INSERT INTO test VALUES (2, 100), (3, 101);",
				ExpectedErrStr: "duplicate unique key given: [1000]",
			},
			{
				Query:    "INSERT INTO test VALUES (2, 101);",
				Expected: []sql.Row{{gmstypes.NewOkResult(1)}},
			},
			{
				Query:          "UPDATE test SET c1 = 101 WHERE pk = 1;",
				ExpectedErrStr: "duplicate unique key given: [1010]",
			},
		},
	},
	{
		Name: "constraints: UNIQUE index allows multiple NULLs",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				// MySQL allows multiple NULLs in a UNIQUE index because NULL != NULL.
				// The functional index must preserve this semantics.
				Query:    "INSERT INTO test VALUES (1, NULL), (2, NULL), (3, NULL);",
				Expected: []sql.Row{{gmstypes.NewOkResult(3)}},
			},
			{
				Query:           "SELECT pk FROM test WHERE c1*10 IS NULL ORDER BY pk;",
				Expected:        []sql.Row{{1}, {2}, {3}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				// Non-NULL values still trigger the uniqueness constraint.
				Query:          "INSERT INTO test VALUES (4, 100), (5, 100);",
				ExpectedErrStr: "duplicate unique key given: [1000]",
			},
		},
	},
	{
		Name: "DDL: DROP INDEX removes hidden system column",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Assert that the system hidden column exists before dropping the index
				Query: "SHOW EXTENDED COLUMNS FROM test;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"c1", "int", "YES", "", nil, ""},
					{"!hidden!idx1!0!0", "bigint", "YES", "UNI", nil, ""},
				},
			},
			{
				// Drop the index, which should remove the system hidden column
				Query:    "DROP INDEX idx1 ON test;",
				Expected: []sql.Row{{gmstypes.NewOkResult(0)}},
			},
			{
				// Assert that the system hidden column has been removed
				Query: "SHOW EXTENDED COLUMNS FROM test;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"c1", "int", "YES", "", nil, ""},
				},
			},
		},
	},
	{
		Name: "DDL: create/drop/recreate index lifecycle",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE INDEX idx1 ON test ((c1*10));",
				Expected: []sql.Row{{gmstypes.NewOkResult(0)}},
			},
			{
				Query:    "DROP INDEX idx1 ON test;",
				Expected: []sql.Row{{gmstypes.NewOkResult(0)}},
			},
			{
				Query:    "CREATE INDEX idx1 ON test ((c1*10));",
				Expected: []sql.Row{{gmstypes.NewOkResult(0)}},
			},
			{
				Query: "SHOW EXTENDED COLUMNS FROM test;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"c1", "int", "YES", "", nil, ""},
					{"!hidden!idx1!0!0", "bigint", "YES", "MUL", nil, ""},
				},
			},
			{
				Query:           "SELECT pk FROM test WHERE c1*10 = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "DDL: RENAME COLUMN blocked by functional index dependency",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "ALTER TABLE test RENAME COLUMN c1 TO c1_new;",
				ExpectedErrStr: "Column 'c1' has a functional index dependency and cannot be dropped or renamed.",
			},
		},
	},
	{
		Name: "DDL: DROP COLUMN blocked by functional index dependency",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"CREATE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "ALTER TABLE test DROP COLUMN c1;",
				ExpectedErrStr: "Column 'c1' has a functional index dependency and cannot be dropped or renamed.",
			},
		},
	},
	{
		Name: "system hidden columns: not accessible via any SQL operation",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200);",

			// NOTE: The index must be unique in order for it to be used for strict lookups
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",

			"CREATE TABLE t2 (t2pk INT PRIMARY KEY, t2c1 INT);",
			"INSERT INTO t2 VALUES (1, 1000);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// SHOW COLUMNS should NOT report hidden system columns
				Query: "SHOW COLUMNS FROM test;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"c1", "int", "YES", "", nil, ""},
				},
			},
			{
				// SHOW EXTENDED COLUMNS should report hidden system columns
				Query: "SHOW EXTENDED COLUMNS FROM test;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"c1", "int", "YES", "", nil, ""},
					{"!hidden!idx1!0!0", "bigint", "YES", "UNI", nil, ""},
				},
			},
			{
				// information_schema.COLUMNS should NOT report hidden system columns
				Query: "SELECT table_name, column_name FROM information_schema.COLUMNS WHERE table_name='test';",
				Expected: []sql.Row{
					{"test", "pk"},
					{"test", "c1"},
				},
			},
			{
				Query:          "SELECT !hidden!idx1!0!0 FROM test;",
				ExpectedErrStr: "syntax error at position 16 near 'hidden'",
			},
			{
				Query:          "SELECT `!hidden!idx1!0!0` FROM test;",
				ExpectedErrStr: `column "!hidden!idx1!0!0" could not be found in any table in scope`,
			},
			{
				Query:          "SELECT '' + `!hidden!idx1!0!0` FROM test;",
				ExpectedErrStr: `column "!hidden!idx1!0!0" could not be found in any table in scope`,
			},
			{
				Query:          "SELECT pk FROM test WHERE `!hidden!idx1!0!0` > 0;",
				ExpectedErrStr: `column "!hidden!idx1!0!0" could not be found in any table in scope`,
			},
			{
				Query:          "ALTER TABLE test DROP COLUMN `!hidden!idx1!0!0`;",
				ExpectedErrStr: `column "!hidden!idx1!0!0" could not be found in any table in scope`,
			},
			{
				Query:          "ALTER TABLE test RENAME COLUMN `!hidden!idx1!0!0` TO `foobarbaz`;",
				ExpectedErrStr: `column "!hidden!idx1!0!0" could not be found in any table in scope`,
			},
			{
				Query:          "ALTER TABLE test MODIFY COLUMN `!hidden!idx1!0!0` INT NOT NULL;",
				ExpectedErrStr: `column "!hidden!idx1!0!0" could not be found in any table in scope`,
			},
			{
				Query:          "ALTER TABLE test ADD COLUMN c3 INT, MODIFY COLUMN `!hidden!idx1!0!0` INT NOT NULL;",
				ExpectedErrStr: `column "!hidden!idx1!0!0" could not be found in any table in scope`,
			},
			{
				Query:          "UPDATE test SET `!hidden!idx1!0!0` = 0;",
				ExpectedErrStr: `column "!hidden!idx1!0!0" could not be found in any table in scope`,
			},
		},
	},
	{
		// Doltgres and Dolt output for SHOW CREATE TABLE is different
		Dialect: "mysql",
		Name:    "system hidden columns: omitted from SHOW CREATE TABLE",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT, c2 VARCHAR(100));",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW CREATE TABLE test;",
				Expected: []sql.Row{
					{
						"test",
						"CREATE TABLE `test` (\n" +
							"  `pk` int NOT NULL,\n" +
							"  `c1` int,\n" +
							"  `c2` varchar(100),\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  UNIQUE KEY `idx1` (((c1 * 10)))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				// SHOW EXTENDED COLUMNS should still reveal the hidden system column,
				// proving that SHOW CREATE TABLE is intentionally hiding it.
				Query: "SHOW EXTENDED COLUMNS FROM test;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"c1", "int", "YES", "", nil, ""},
					{"c2", "varchar(100)", "YES", "", nil, ""},
					{"!hidden!idx1!0!0", "bigint", "YES", "UNI", nil, ""},
				},
			},
		},
	},
	{
		// Doltgres and Dolt output for SHOW CREATE TABLE is different
		Dialect: "mysql",
		Name:    "system hidden columns: omitted from SHOW CREATE TABLE after ALTER TABLE ADD COLUMN",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
			"ALTER TABLE test ADD COLUMN c2 INT;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW CREATE TABLE test;",
				Expected: []sql.Row{
					{
						"test",
						"CREATE TABLE `test` (\n" +
							"  `pk` int NOT NULL,\n" +
							"  `c1` int,\n" +
							"  `c2` int,\n" +
							"  PRIMARY KEY (`pk`),\n" +
							"  UNIQUE KEY `idx1` (((c1 * 10)))\n" +
							") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},
		},
	},
	{
		Name: "subqueries: IN subquery",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500), (6, 600), (7, 700), (8, 800), (9, 900), (10, 1000), (11, 1100), (12, 1200), (13, 1300), (14, 1400), (15, 1500), (16, 1600), (17, 1700), (18, 1800), (19, 1900), (20, 2000), (21, 2100), (22, 2200), (23, 2300), (24, 2400), (25, 2500), (26, 2600), (27, 2700), (28, 2800), (29, 2900), (30, 3000);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",

			"CREATE TABLE lookup (id INT PRIMARY KEY, val INT);",
			"INSERT INTO lookup VALUES (1, 1000), (2, 3000), (3, 9999);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
				SELECT pk
				FROM test
				WHERE c1*10 IN (SELECT val FROM lookup)
				ORDER BY pk
			`,
				Expected: []sql.Row{
					{1},
					{3},
				},
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "subqueries: scalar subquery RHS",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500), (6, 600), (7, 700), (8, 800), (9, 900), (10, 1000), (11, 1100), (12, 1200), (13, 1300), (14, 1400), (15, 1500), (16, 1600), (17, 1700), (18, 1800), (19, 1900), (20, 2000), (21, 2100), (22, 2200), (23, 2300), (24, 2400), (25, 2500), (26, 2600), (27, 2700), (28, 2800), (29, 2900), (30, 3000);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",

			"CREATE TABLE single_val (id INT PRIMARY KEY, v INT);",
			"INSERT INTO single_val VALUES (1, 2000), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16), (17, 17), (18, 18), (19, 19), (20, 20), (21, 21), (22, 22), (23, 23), (24, 24), (25, 25), (26, 26), (27, 27), (28, 28), (29, 29), (30, 30);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Force a lookup join with a join hint so we can test the functional index
				Query: `
				SELECT /*+ lookup_join(single_val,test) */ pk
				FROM test
				WHERE c1*10 = (SELECT v FROM single_val WHERE id = 1)
			`,
				Expected:        []sql.Row{{2}},
				ExpectedIndexes: []string{"primary", "idx1"},
			},
		},
	},
	{
		Name: "subqueries: filter inside derived table",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, 300);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
				SELECT *
				FROM (
					SELECT pk
					FROM test
					WHERE c1*10 = 2000
				) AS dt
			`,
				Expected: []sql.Row{
					{2},
				},
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "filtering: BETWEEN range scan",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);",
			"CREATE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				// BETWEEN is rewritten to AND(GTE, LTE) before index planning, so both
				// range bounds use the functional index.
				Query:           "SELECT pk FROM test WHERE c1*10 BETWEEN 150 AND 350 ORDER BY pk;",
				Expected:        []sql.Row{{2}, {3}},
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "filtering: YEAR() function on date column",
		SetUpScript: []string{
			"CREATE TABLE events (pk INT PRIMARY KEY, created_at DATE);",
			"INSERT INTO events VALUES (1, '2023-03-15'), (2, '2024-01-01'), (3, '2024-07-04'), (4, '2025-12-31'), (5, NULL);",
			"CREATE INDEX idx_year ON events ((YEAR(created_at)));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT pk FROM events WHERE YEAR(created_at) = 2024 ORDER BY pk;",
				Expected:        []sql.Row{{2}, {3}},
				ExpectedIndexes: []string{"idx_year"},
			},
			{
				Query:           "SELECT pk FROM events WHERE YEAR(created_at) < 2024;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx_year"},
			},
			{
				Query:           "SELECT pk FROM events WHERE YEAR(created_at) IS NULL;",
				Expected:        []sql.Row{{5}},
				ExpectedIndexes: []string{"idx_year"},
			},
		},
	},
	{
		Name: "filtering: table alias does not prevent index use",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, 300);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				// When the query uses a table alias, column references carry the alias as
				// their table qualifier. The index matcher must handle this correctly.
				Query:           "SELECT t.pk FROM test t WHERE t.c1*10 = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT t.pk FROM test AS t WHERE t.c1*10 = 2000;",
				Expected:        []sql.Row{{2}},
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "JSON functions",
		SetUpScript: []string{
			"CREATE TABLE events (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, payload JSON NOT NULL);",
			"INSERT INTO events (payload) VALUES (JSON_OBJECT('amount', 149, 'user_id', 42, 'event_type', 'purchase'));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select id, (CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, '$.amount')) AS DECIMAL(10,2))) from events;",
				Expected: []sql.Row{{uint64(1), "149.00"}},
			},
			{
				Query:    "CREATE INDEX idx1 ON events ((CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, '$.amount')) AS DECIMAL(10,2))));",
				Expected: []sql.Row{{gmstypes.NewOkResult(0)}},
			},
			{
				Query:           "SELECT id FROM events WHERE (CAST(JSON_UNQUOTE(JSON_EXTRACT(payload, '$.amount')) AS DECIMAL(10,2))) = 149.0;",
				Expected:        []sql.Row{{uint64(1)}},
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "JSON functions: multiple functional indexes",
		SetUpScript: []string{
			"CREATE TABLE events (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, payload JSON NOT NULL);",
			`INSERT INTO events (payload) VALUES
				('{"event_type": "purchase", "country": "US"}'),
				('{"event_type": "purchase", "country": "GB"}'),
				('{"event_type": "signup",   "country": "US"}'),
				('{"event_type": "signup",   "country": "GB"}'),
				('{"event_type": "purchase", "country": "CA"}');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE INDEX idx_event_type ON events ((JSON_UNQUOTE(JSON_EXTRACT(payload, '$.event_type'))));",
				Expected: []sql.Row{{gmstypes.NewOkResult(0)}},
			},
			{
				Query:           "SELECT COUNT(*) FROM events WHERE JSON_UNQUOTE(JSON_EXTRACT(payload, '$.event_type')) = 'purchase';",
				Expected:        []sql.Row{{int64(3)}},
				ExpectedIndexes: []string{"idx_event_type"},
			},
			{
				Query:    "CREATE INDEX idx_country ON events ((JSON_UNQUOTE(JSON_EXTRACT(payload, '$.country'))));",
				Expected: []sql.Row{{gmstypes.NewOkResult(0)}},
			},
			{
				Query:           "SELECT COUNT(*) FROM events WHERE JSON_UNQUOTE(JSON_EXTRACT(payload, '$.event_type')) = 'purchase';",
				Expected:        []sql.Row{{int64(3)}},
				ExpectedIndexes: []string{"idx_event_type"},
			},
			{
				Query:           "SELECT COUNT(*) FROM events WHERE JSON_UNQUOTE(JSON_EXTRACT(payload, '$.country')) = 'US';",
				Expected:        []sql.Row{{int64(2)}},
				ExpectedIndexes: []string{"idx_country"},
			},
		},
	},
	{
		Name: "functional index value remains correct after ALTER TABLE ADD COLUMN FIRST",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO t VALUES (1, 100), (2, 200);",
			"CREATE UNIQUE INDEX idx ON t ((c1 * 10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t ADD COLUMN x INT DEFAULT 0 FIRST;",
				Expected: []sql.Row{{gmstypes.NewOkResult(0)}},
			},
			{
				Query:           "SELECT pk FROM t WHERE c1 * 10 = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx"},
			},
			{
				Query:           "SELECT pk FROM t WHERE c1 * 10 = 2000;",
				Expected:        []sql.Row{{2}},
				ExpectedIndexes: []string{"idx"},
			},
		},
	},
	{
		Name: "virtual generated column index value remains correct after ALTER TABLE ADD COLUMN FIRST",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, c1 INT, vc INT GENERATED ALWAYS AS (c1 * 10) VIRTUAL, INDEX idx_vc (vc));",
			"INSERT INTO t (pk, c1) VALUES (1, 100), (2, 200);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t ADD COLUMN x INT DEFAULT 0 FIRST;",
				Expected: []sql.Row{{gmstypes.NewOkResult(0)}},
			},
			{
				Query:           "SELECT pk FROM t WHERE vc = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx_vc"},
			},
			{
				Query:           "SELECT pk FROM t WHERE vc = 2000;",
				Expected:        []sql.Row{{2}},
				ExpectedIndexes: []string{"idx_vc"},
			},
		},
	},
	{
		// TODO: Parser changes needed in order to support inline functional expression index
		Skip: true,
		Name: "create functional index inline in table definition",
		Assertions: []ScriptTestAssertion{
			{
				Query: `CREATE TABLE t (
  pk int NOT NULL,
  c1 int DEFAULT NULL,
  PRIMARY KEY (pk),
  KEY idx1 (((10 * c1)))
) `,

				Expected: []sql.Row{{gmstypes.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO t VALUES (1, 100), (2, 200);",
				Expected: []sql.Row{{gmstypes.NewOkResult(2)}},
			},
			{
				Query:           "SELECT pk FROM t WHERE c1 * 10 = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "!hidden! column name prefix is reserved",
		SetUpScript: []string{
			"create table t2 (pk int primary key, c1 int);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table t (pk int primary key, `!hidden!idx!0` int);",
				ExpectedErrStr: "invalid column name: !hidden!idx!0",
			},
			{
				Query:          "create table t (pk int primary key, `!HIDDEN!idx!0` int);",
				ExpectedErrStr: "invalid column name: !HIDDEN!idx!0",
			},
			{
				Query:          "ALTER TABLE t2 RENAME COLUMN c1 TO `!hidden!idx!0`;",
				ExpectedErrStr: "invalid column name: !hidden!idx!0",
			},
		},
	},
}
