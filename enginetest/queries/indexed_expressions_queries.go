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
		Name: "functional index: IN list does not panic",
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

	// --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

	{
		Name: "Indexed Expressions: planner/filter matching",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200);",

			// NOTE: The index must be unique in order for it to be used for strict lookups
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
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
		},
	},

	{
		Name: "Indexed Expressions: exact expression matching rules",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT pk FROM test WHERE c1*10 = 1000;",
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
				// Equivalent expression, but not syntactically identical. Current implementation
				// is expected NOT to match this to the functional index.
				Query:           "SELECT pk FROM test WHERE 10*c1 = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{},
			},
			{
				Query:           "SELECT pk FROM test WHERE (c1*5)*2 = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{},
			},
			{
				Query:           "SELECT pk FROM test WHERE c1*10+0 = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{},
			},
		},
	},

	{
		Name: "Indexed Expressions: use in join conditions 1",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			// NOTE: For the indexed access plan to be chosen, we need to populate enough test data
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, -3), (4, -4), (5, -5), (6, -6), (7, -7), (8, -8), (9, -9), (10, -10), (11, -11), (12, -12), (13, -13), (14, -14), (15, -15), (16, -16), (17, -17), (18, -18), (19, -19), (20, -20), (21, -21), (22, -22), (23, -23), (24, -24), (25, -25), (26, -26), (27, -27), (28, -28), (29, -29);",

			// NOTE: The index must be unique in order for it to be used for strict lookups
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",

			"CREATE TABLE t2 (t2pk INT PRIMARY KEY, t2c1 INT);",
			"INSERT INTO t2 VALUES (1, 1000), (2, -1), (3, -1), (4, -1), (5, -1), (6, -1), (7, -1), (8, -1), (9, -1), (10, -10);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT pk FROM test JOIN t2 ON (c1*10) = t2c1;",
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
		Name: "Indexed Expressions: use in join conditions 2",
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
		Name: "Indexed Expressions: use in join conditions 3",
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
		Name: "Indexed Expressions: joins with functional index on either side",
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
		Name: "Indexed Expressions: covering and non-covering queries",
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
				// NOTE: Dolt does NOT use an index here. Dolt could do an index scan, but instead it just
				//       does a table scan – not a huge benefit in an index scan here anyway, since we're
				//       iterating over the same number of rows either way.
				Query:           "SELECT c1 FROM test;",
				Expected:        []sql.Row{{100}, {200}},
				ExpectedIndexes: []string{},
			},
			{
				Query: "SELECT * FROM test;",
				Expected: []sql.Row{
					{1, 100},
					{2, 200},
				},
			},
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
				// table or the index needs to be scanned, so we just use the table.
				Query:    "SELECT pk, c1*10 FROM test;",
				Expected: []sql.Row{{1, 1000}, {2, 2000}, {1000, 1010}},
				// TODO: There is still a perf win if we use the secondary index though – the table uses a
				//       virtual generated column, so the expression has to be evaluated for every row, but
				//       the secondary index stores the generated value.
				// ExpectedIndexes: []string{"idx1"},
			},
			{
				// In this example we project the pk, the indexed expression, as well as c1. c1 is not
				// included in the index, so the index should NOT be used for this query.
				Query:           "SELECT pk, c1, c1*10 FROM test;",
				Expected:        []sql.Row{{1, 100, 1000}, {2, 200, 2000}, {1000, 101, 1010}},
				ExpectedIndexes: []string{},
			},
		},
	},

	{
		Name: "Indexed Expressions: order by expression",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 20), (2, 10), (3, 30);",
			"CREATE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT pk, c1*10 FROM test ORDER BY c1*10;",
				Expected: []sql.Row{{2, 100}, {1, 200}, {3, 300}},
			},
		},
	},

	{
		Name: "Indexed Expressions: index maintenance on insert update delete",
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
		Name: "Indexed Expressions: update and delete using indexed expression predicate",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT, c2 INT);",
			"INSERT INTO test VALUES (1, 100, 0), (2, 200, 0), (3, 300, 0);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "UPDATE test SET c2 = 7 WHERE c1*10 = 1000;",
				Expected: []sql.Row{{gmstypes.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "SELECT pk, c2 FROM test ORDER BY pk;",
				Expected: []sql.Row{{1, 7}, {2, 0}, {3, 0}},
			},
			{
				Query:    "DELETE FROM test WHERE c1*10 = 2000;",
				Expected: []sql.Row{{gmstypes.NewOkResult(1)}},
			},
			{
				Query:    "SELECT pk FROM test ORDER BY pk;",
				Expected: []sql.Row{{1}, {3}},
			},
		},
	},

	{
		Name: "Indexed Expressions: unique index enforces uniqueness on expression result",
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
		Name: "Indexed Expressions: null handling",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
			"INSERT INTO test VALUES (1, NULL), (2, NULL), (3, 100);",
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
		Name: "Indexed Expressions: lower function text expression",
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
				Query:    "SELECT pk, LOWER(name) FROM people ORDER BY pk;",
				Expected: []sql.Row{{1, "alice"}, {2, "alice"}, {3, "bob"}, {4, nil}},
			},
		},
	},

	{
		Name: "Indexed Expressions: single expression referencing multiple columns",
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
		Name: "Indexed Expressions: dropping index removes hidden system columns",
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
				// Assert that the system hidden column exists before dropping the index
				Query: "SHOW EXTENDED COLUMNS FROM test;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"c1", "int", "YES", "", nil, ""},
					//{"!hidden!idx1!0!0", "bigint", "YES", "", nil, ""},
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
		Name: "Indexed Expressions: create drop recreate index lifecycle",
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
					//{"!hidden!idx1!0!0", "bigint", "YES", "", nil, ""},
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
		Name: "Indexed Expressions: hidden system columns are not directly usable",
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
		Name: "Indexed Expressions: rename base column is rejected with functional index dependency",
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
		Name: "Indexed Expressions: dropping referenced column is rejected",
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

	// ---- Original tests ----
	{
		Name: "Indexed Expressions: use in filters",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 int);",
			"INSERT INTO test VALUES (1, 100), (2, 200);",

			// NOTE: The index must be unique in order for it to be used for strict lookups
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT pk from test where (c1*10) = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk from test where (c1*10) >= 1000;",
				Expected:        []sql.Row{{1}, {2}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				Query:           "SELECT pk, c1*10 from test where (c1*10) > 1;",
				Expected:        []sql.Row{{1, 1000}, {2, 2000}},
				ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "Indexed Expressions: use in join conditions",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 int);",
			// NOTE: For the indexed access plan to be chosen, we need to populate enough test data
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, -3), (4, -4), (5, -5), (6, -6), (7, -7), (8, -8), (9, -9), (10, -10), (11, -11), (12, -12), (13, -13), (14, -14), (15, -15), (16, -16), (17, -17), (18, -18), (19, -19), (20, -20), (21, -21), (22, -22), (23, -23), (24, -24), (25, -25), (26, -26), (27, -27), (28, -28), (29, -29), (30, -30);",

			// NOTE: The index must be unique in order for it to be used for strict lookups
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",

			"CREATE TABLE t2 (t2pk INT PRIMARY KEY, t2c1 int);",
			"INSERT INTO t2 VALUES (1, 1000);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT pk from test join t2 on (c1*10) = t2c1;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			// TODO: Add a test for non-arithmetic expressions, such as lower(name)
			// TODO: Add a more complicated query with multiple table joins
		},
	},

	{
		Name: "Indexed Expressions: basic cases - part 2",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 int);",
			"INSERT INTO test VALUES (1, 100), (2, 200);",

			// NOTE: The index must be unique in order for it to be used for strict lookups
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",

			"CREATE TABLE t2 (t2pk INT PRIMARY KEY, t2c1 int);",
			"INSERT INTO t2 VALUES (1, 1000);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// NOTE: Dolt does NOT use an index here. Dolt could do an index scan, but instead it just
				//       does a table scan – not a huge benefit in an index scan here anyway, since we're
				//       iterating over the same number of rows either way.
				Query:           `SELECT c1 from test;`,
				Expected:        []sql.Row{{100}, {200}},
				ExpectedIndexes: []string{},
			},
			{
				Query: "SELECT * from test;",
				Expected: []sql.Row{
					{1, 100},
					{2, 200},
				},
			},
			{
				// Alter the table to add a column AFTER the system hidden column so we can
				// test that we still pull the values out of the row correctly when there
				// is a system hidden column before the field location.
				Query:    "ALTER TABLE test ADD COLUMN c2 int;",
				Expected: []sql.Row{{gmstypes.NewOkResult(0)}},
			},
			{
				Query: "SELECT * from test;",
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
				Query:           "SELECT pk from test where c1*10 = 1000;",
				Expected:        []sql.Row{{1}},
				ExpectedIndexes: []string{"idx1"},
			},
			{
				// We're projecting the indexed expression, but we don't use the index, because either the
				// table or the index needs to be scanned, so we just use the table.
				Query:    "SELECT pk, c1*10 from test;",
				Expected: []sql.Row{{1, 1000}, {2, 2000}, {1000, 1010}},
				// TODO: There is still a perf win if we use the secondary index though – the table uses a
				//       virtual generated column, so the expression has to be evaluated for every row, but
				//       the secondary index stores the generated value.
				//ExpectedIndexes: []string{"idx1"},
			},
			{
				// In this example we project the pk, the indexed expression, as well as c1. c1 is not
				// included in the index, so the index should NOT be used for this query.
				Query:           "SELECT pk, c1, c1*10 from test;",
				Expected:        []sql.Row{{1, 100, 1000}, {2, 200, 2000}, {1000, 101, 1010}},
				ExpectedIndexes: []string{},
			},
		},
	},

	// MORE ChatGPT generated tests
	{
		Name: "Indexed Expressions: SHOW CREATE TABLE omits hidden system column",
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
		Name: "Indexed Expressions: SHOW CREATE TABLE omits hidden system column after additional ALTER TABLE",
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
		Name: "Indexed Expressions: three table join with functional expression lookup",
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
		Name: "Indexed Expressions: four table join with multiple indexes used",
		SetUpScript: []string{
			"CREATE TABLE customers (cust_id INT PRIMARY KEY, tier_id INT, active INT);",
			"INSERT INTO customers VALUES (1, 1, 1), (2, 1, 1), (3, 2, 0), (4, 2, 1), (5, 3, 1), (6, 3, 0), (7, 1, 1), (8, 2, 0), (9, 3, 1), (10, 1, 0), (11, 2, 1), (12, 3, 0), (13, 1, 1), (14, 2, 1), (15, 3, 0), (16, 1, 1), (17, 2, 0), (18, 3, 1), (19, 1, 0), (20, 2, 1), (21, 3, 0), (22, 1, 1), (23, 2, 1), (24, 3, 0), (25, 1, 1), (26, 2, 0), (27, 3, 1), (28, 1, 0), (29, 2, 1), (30, 3, 0);",
			"CREATE INDEX idx_customers_tier ON customers (tier_id);",
			"CREATE INDEX idx_customers_active ON customers (active);",

			"CREATE TABLE orders (order_id INT PRIMARY KEY, cust_id INT, amount INT);",
			"INSERT INTO orders VALUES (1, 1, 100), (2, 2, 200), (3, 3, 300), (4, 4, 400), (5, 1, 500), (6, 2, 600), (7, 4, 700), (8, 1, 800), (9, 2, 900), (10, 4, 1000), (11, 1, 1100), (12, 2, 1200), (13, 3, 1300), (14, 4, 1400), (15, 1, 1500), (16, 2, 1600), (17, 3, 1700), (18, 4, 1800), (19, 1, 1900), (20, 2, 2000), (21, 3, 2100), (22, 4, 2200), (23, 1, 2300), (24, 2, 2400), (25, 3, 2500), (26, 4, 2600), (27, 1, 2700), (28, 2, 2800), (29, 3, 2900), (30, 4, 3000);",
			"CREATE UNIQUE INDEX idx_order_expr ON orders ((amount*10));",
			"CREATE INDEX idx_orders_cust_id ON orders (cust_id);",

			"CREATE TABLE shipments (shipment_id INT PRIMARY KEY, amount_key INT, region_id INT);",
			"INSERT INTO shipments VALUES (1, 1000, 10), (2, 2000, 10), (3, 4000, 20), (4, 10000, 20), (5, 9999, 30), (6, 12000, 10), (7, 14000, 20), (8, 16000, 30), (9, 18000, 10), (10, 20000, 20), (11, 22000, 30), (12, 24000, 10), (13, 26000, 20), (14, 28000, 30), (15, 30000, 10), (16, 32000, 20), (17, 34000, 30), (18, 36000, 10), (19, 38000, 20), (20, 40000, 30), (21, 42000, 10), (22, 44000, 20), (23, 46000, 30), (24, 48000, 10), (25, 50000, 20), (26, 52000, 30), (27, 54000, 10), (28, 56000, 20), (29, 58000, 30), (30, 60000, 10);",
			"CREATE INDEX idx_ship_amount_key ON shipments (amount_key);",
			"CREATE INDEX idx_ship_region_id ON shipments (region_id);",

			"CREATE TABLE regions (region_id INT PRIMARY KEY, region_name VARCHAR(20), enabled INT);",
			"INSERT INTO regions VALUES (10, 'west', 1), (20, 'east', 0), (30, 'south', 0);",
			"CREATE INDEX idx_regions_enabled ON regions (enabled);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
				SELECT orders.order_id, customers.cust_id, regions.region_name
				FROM orders
				JOIN shipments ON orders.amount*10 = shipments.amount_key
				JOIN customers ON orders.cust_id = customers.cust_id
				JOIN regions ON shipments.region_id = regions.region_id
				WHERE customers.active = 1
				  AND regions.enabled = 1
				ORDER BY orders.order_id
			`,
				Expected: []sql.Row{
					{1, 1, "west"}, {2, 2, "west"}, {12, 2, "west"},
					{18, 4, "west"}, {24, 2, "west"}, {30, 4, "west"}},
				// TODO: The expected plan should use idx_order_expr (for the functional index on
				// orders.amount*10) and idx_ship_amount_key / idx_customers_active, but the
				// optimizer currently picks a different join order. These aspirational indexes are:
				//   "idx_regions_enabled", "idx_order_expr", "idx_ship_amount_key", "idx_customers_active"
				ExpectedIndexes: []string{"idx_orders_cust_id", "primary", "idx_regions_enabled"},
			},
		},
	},
	{
		Name: "Indexed Expressions: four table join with additional filter on indexed expression side",
		SetUpScript: []string{
			"CREATE TABLE a (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO a VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500), (6, 600), (7, 700), (8, 800), (9, 900), (10, 1000), (11, 1100);",
			"CREATE UNIQUE INDEX idx_a_expr ON a ((c1*10));",

			"CREATE TABLE b (pk INT PRIMARY KEY, expr_key INT, c_pk INT);",
			"INSERT INTO b VALUES (1, 1000, 1), (2, 2000, 1), (3, 3000, 2), (4, 8000, 3), (5, 11000, 4);",
			"CREATE INDEX idx_b_expr_key ON b (expr_key);",
			"CREATE INDEX idx_b_c_pk ON b (c_pk);",

			"CREATE TABLE c (pk INT PRIMARY KEY, d_pk INT, status VARCHAR(20));",
			"INSERT INTO c VALUES (1, 1, 'open'), (2, 2, 'closed'), (3, 3, 'open'), (4, 4, 'open');",
			"CREATE INDEX idx_c_d_pk ON c (d_pk);",
			"CREATE INDEX idx_c_status ON c (status);",

			"CREATE TABLE d (pk INT PRIMARY KEY, keep_flag INT);",
			"INSERT INTO d VALUES (1, 1), (2, 0), (3, 1), (4, 1);",
			"CREATE INDEX idx_d_keep_flag ON d (keep_flag);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
				SELECT a.pk, b.pk, c.pk, d.pk
				FROM a
				JOIN b ON a.c1*10 = b.expr_key
				JOIN c ON b.c_pk = c.pk
				JOIN d ON c.d_pk = d.pk
				WHERE c.status = 'open'
				  AND d.keep_flag = 1
				  AND a.c1*10 >= 3000
				ORDER BY a.pk;
			`,
				Expected: []sql.Row{
					{8, 4, 3, 3},
					{11, 5, 4, 4},
				},
				// TODO: The expected plan should also use idx_b_expr_key for the join
				// a.c1*10 = b.expr_key, but the optimizer currently picks a hash join there.
				ExpectedIndexes: []string{"idx_a_expr", "idx_d_keep_flag", "idx_c_status"},
			},
		},
	},

	// More ChatGPT tests...

	{
		Name: "Indexed Expressions: IN subquery uses functional index",
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
		Name: "Indexed Expressions: correlated EXISTS subquery",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500), (6, 600), (7, 700), (8, 800), (9, 900), (10, 1000), (11, 1100), (12, 1200), (13, 1300), (14, 1400), (15, 1500);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",

			"CREATE TABLE t2 (id INT PRIMARY KEY, val INT);",
			"INSERT INTO t2 VALUES (1, 1000), (2, 3000), (3, 12345);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// TODO: The index is NOT used here, why?
				// TODO: if we add c1 to our index, will that cause the index to be used?
				//       shit... we can't actually do that, because we only support a single expression for functional indexes
				//       actually... even after removing c1, and just using pk in the indexed function expression, we still don't get our index selected
				//       but... what if we remove the c1 column completely... (i.e. as if the problem is that the virtual table didn't get pruned?)
				//       nope, that didn't fix it either...
				//       it looks like the replaceIndexedExpressions rule isn't doing anything? Why would that be?
				Query: `
				SELECT pk
				FROM test
				WHERE EXISTS (
					SELECT 1
					FROM t2
					WHERE t2.val = test.c1*10
				)
				ORDER BY pk
			`,
				// TODO: We aren't executing this query correctly anymore...
				Expected: []sql.Row{{1}, {3}},
				// TODO: The changes we
				//ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "Indexed Expressions: scalar subquery comparison",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500), (6, 600), (7, 700), (8, 800), (9, 900), (10, 1000), (11, 1100), (12, 1200), (13, 1300), (14, 1400), (15, 1500), (16, 1600), (17, 1700), (18, 1800), (19, 1900), (20, 2000), (21, 2100), (22, 2200), (23, 2300), (24, 2400), (25, 2500), (26, 2600), (27, 2700), (28, 2800), (29, 2900), (30, 3000);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",

			"CREATE TABLE single_val (id INT PRIMARY KEY, v INT);",
			"INSERT INTO single_val VALUES (1, 2000), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16), (17, 17), (18, 18), (19, 19), (20, 20), (21, 21), (22, 22), (23, 23), (24, 24), (25, 25), (26, 26), (27, 27), (28, 28), (29, 29), (30, 30);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
				SELECT /*+ lookup_join(single_val,test) */ pk
				FROM test
				WHERE c1*10 = (SELECT v FROM single_val WHERE id = 1)
			`,
				Expected: []sql.Row{{2}},
				// TODO: Do we know why primary is also mentioned here, but not in other tests?
				ExpectedIndexes: []string{"primary", "idx1"},
			},
		},
	},
	{
		Name: "Indexed Expressions: derived table projects expression without leaking hidden column",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, 300);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				// The hidden system column needs to be projected from the source table (that works) but with a subquery
				// like this, it also needs to be projected through the VIRTUAL COLUMN TABLE node in the plan!
				// When we replace the projected expression, we do NOT currently update the VirtualColumnTable, possibly
				// because it has another wrapper around it? Seems like maybe we don't traverse down into that or something?
				//
				// error:
				// column "!hidden!idx1!0!0" could not be found in any table in scope
				Query: `
				SELECT dt.pk, dt.expr_col
				FROM (
					SELECT pk, c1*10 AS expr_col
					FROM test
				) AS dt
				WHERE dt.expr_col = 2000
			`,
				Expected: []sql.Row{
					{2, 2000},
				},
			},
		},
	},
	{
		Name: "Indexed Expressions: inner subquery filter uses functional index",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, 300);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				// TODO: this test is still failing...
				// error:
				// column "!hidden!idx1!0!0" could not be found in any table in scope
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
		Name: "Indexed Expressions: correlated aggregate subquery correctness",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, 300);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",

			"CREATE TABLE t2 (id INT PRIMARY KEY, val INT);",
			"INSERT INTO t2 VALUES (1, 1000), (2, 1000), (3, 3000);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
				SELECT pk
				FROM test
				WHERE (
					SELECT COUNT(*)
					FROM t2
					WHERE t2.val = test.c1*10
				) > 0
				ORDER BY pk;
			`,
				Expected: []sql.Row{
					{1},
					{3},
				},
				// TODO: Should we be using the secondary index here?
				//       Can we check this with MySQL? Currently, this fails – we get the correct
				//       results, but don't use the idx1 index.
				//       Actually... MySQL does NOT use an index here either...
				//       On closer inspection of this query... it seems that it isn't a good candidate
				//       for using the index, so probably isn't a good test either.
				//ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	// TODO: When we update a column that is used in a functional expression
	//       we need to make sure the secondary index gets updated with the
	//       new, computed value. How can we test this? Manually inspect indexes?
	// TODO: When we update the c1 column, the secondary index MUST be updated, too
	//       How can we test that? query that the index was used after an update and
	//       that we get correct results?
	{
		Name: "Indexed Expressions: UPDATE with subquery on functional expression",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT, c2 INT);",
			"INSERT INTO test VALUES (1, 100, 0), (2, 200, 0), (3, 300, 0);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",

			"CREATE TABLE targets (id INT PRIMARY KEY, val INT);",
			"INSERT INTO targets VALUES (1, 1000), (2, 3000);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
				UPDATE test
				SET c2 = 1
				WHERE c1*10 IN (SELECT val FROM targets)
			`,
				Expected: []sql.Row{{gmstypes.OkResult{RowsAffected: 2, Info: plan.UpdateInfo{
					Matched: 2,
					Updated: 2,
				}}}},
				// TODO: Why isn't the secondary index used? Should it be?
				//ExpectedIndexes: []string{"idx1"},
			},
			{
				Query: "SELECT pk, c2 FROM test ORDER BY pk;",
				Expected: []sql.Row{
					{1, 1},
					{2, 0},
					{3, 1},
				},
			},
		},
	},
	{
		Name: "Indexed Expressions: DELETE with correlated EXISTS subquery",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, 300), (4, 400);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",

			"CREATE TABLE t2 (id INT PRIMARY KEY, val INT);",
			"INSERT INTO t2 VALUES (1, 1000), (2, 4000);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
				DELETE FROM test
				WHERE EXISTS (
					SELECT 1
					FROM t2
					WHERE t2.val = test.c1*10
				)
			`,
				Expected: []sql.Row{{gmstypes.NewOkResult(2)}},
			},
			{
				Query: "SELECT pk FROM test ORDER BY pk;",
				Expected: []sql.Row{
					{2},
					{3},
				},
			},
		},
	},
	{
		Name: "Indexed Expressions: NOT IN subquery with NULL semantics",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, 300);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",

			"CREATE TABLE lookup (id INT PRIMARY KEY, val INT);",
			"INSERT INTO lookup VALUES (1, 1000), (2, NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// TODO: This is a bug in GMS/Dolt where we don't handle NOT IN filtering correctly when
				//       a subquery returns NULL.
				//       https://github.com/dolthub/dolt/issues/10699
				// TODO: This was just fixed and we should integrate it and remove this skip!
				Skip: true,
				Query: `
				SELECT pk
				FROM test
				WHERE c1*10 NOT IN (SELECT val FROM lookup)
				ORDER BY pk
			`,
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "Indexed Expressions: derived table column naming uses alias not hidden column",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO test VALUES (1, 100), (2, 200), (3, 300);",
			"CREATE UNIQUE INDEX idx1 ON test ((c1*10));",
		},
		Assertions: []ScriptTestAssertion{
			// TODO: This test only fails because of our validation rule that asserts hidden system columns aren't
			//       referenced directly.
			{
				Query: `
				SELECT expr_col
				FROM (
					SELECT c1*10 AS expr_col
					FROM test
				) AS dt;
			`,
				Expected: []sql.Row{{1000}, {2000}, {3000}},
				// NOTE: We don't end up using the index here... MySQL won't use the index here either
				//       This query is identical to: select c1*10 as expr_col from test;
				//       Instead of reading the computed value from the index, we read the table and
				//       compute the value for each row. In theory, the index could be used, but there's
				//       not a big benefit, especially since the row count for this table is so low.
				//ExpectedIndexes: []string{"idx1"},
			},
		},
	},
	{
		Name: "Indexed Expressions: subquery compares matching functional expressions",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk INT PRIMARY KEY, c1 INT);",
			"INSERT INTO t1 VALUES (1, 100), (2, 200), (3, 300);",
			"CREATE UNIQUE INDEX idx1 ON t1 ((c1*10));",

			"CREATE TABLE t2 (pk INT PRIMARY KEY, c2 INT);",
			"INSERT INTO t2 VALUES (1, 100), (2, 300), (3, 999);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
				SELECT pk
				FROM t1
				WHERE c1*10 IN (
					SELECT c2*10
					FROM t2
				)
				ORDER BY pk
			`,
				Expected: []sql.Row{
					{1},
					{3},
				},
			},
		},
	},
}
