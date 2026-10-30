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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// InvalidGeneratedExpressionScripts test tables whose stored generated column or functional index expressions could not
// be created through DDL, e.g. because they contain a subquery that reads their own table. Such schemas predate DDL
// validation, or arrive some other way, like a merge. memory_set_generated_expression writes the expression into the
// stored schema without validating it, so these scripts require the memory engine's external procedures.
var InvalidGeneratedExpressionScripts = []ScriptTest{
	{
		Name: "generated column reads its own table",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"INSERT INTO t (pk, a) VALUES (1, 1);",
			"CALL memory_set_generated_expression('t', 'b', '(select 1 from t sq)');",
		},
		Assertions: []ScriptTestAssertion{
			{Query: "SELECT * FROM t;", ExpectedErr: sql.ErrGeneratedColumnRecursion},
			{Query: "SELECT pk FROM t;", ExpectedErr: sql.ErrGeneratedColumnRecursion},
			{Query: "INSERT INTO t (pk, a) VALUES (2, 2);", ExpectedErr: sql.ErrGeneratedColumnRecursion},
			{Query: "UPDATE t SET a = 3;", ExpectedErr: sql.ErrGeneratedColumnRecursion},
			{Query: "SELECT * FROM t x JOIN t y;", ExpectedErr: sql.ErrGeneratedColumnRecursion},
		},
	},
	{
		Name: "generated columns read each other's tables",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"CREATE TABLE t2 (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"CALL memory_set_generated_expression('t1', 'b', '(select 1 from t2)');",
			"CALL memory_set_generated_expression('t2', 'b', '(select 1 from t1)');",
		},
		Assertions: []ScriptTestAssertion{
			{Query: "SELECT * FROM t1;", ExpectedErr: sql.ErrGeneratedColumnRecursion},
			{Query: "SELECT * FROM t2;", ExpectedErr: sql.ErrGeneratedColumnRecursion},
		},
	},
	{
		Name: "functional index expression reads its own table",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, a INT);",
			"INSERT INTO t VALUES (1, 1);",
			"CREATE INDEX idx ON t ((a + 1));",
			"CALL memory_set_generated_expression('t', '!hidden!idx!0!0', '(select 1 from t sq)');",
		},
		Assertions: []ScriptTestAssertion{
			{Query: "SELECT * FROM t;", ExpectedErr: sql.ErrGeneratedColumnRecursion},
		},
	},
	{
		Name: "non-recursive references to tables with generated columns are not recursion",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"CREATE TABLE t2 (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"INSERT INTO t2 (pk, a) VALUES (1, 10), (2, 20);",
			// Resolving t1's generated column resolves t2's generated column, which is not a cycle.
			"CALL memory_set_generated_expression('t1', 'b', '(select max(b) from t2)');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM t1;",
				Expected: []sql.Row{},
			},
			{
				// Resolving the same table's generated columns more than once in a query is not a cycle.
				Query:    "SELECT x.pk, y.pk, x.b, y.b FROM t2 x JOIN t2 y ON x.pk = y.pk ORDER BY x.pk;",
				Expected: []sql.Row{{1, 1, 11, 11}, {2, 2, 21, 21}},
			},
			{
				Query:    "SELECT pk, b, (SELECT max(b) FROM t2) FROM t2 ORDER BY pk;",
				Expected: []sql.Row{{1, 11, 21}, {2, 21, 21}},
			},
		},
	},
	{
		Name: "SHOW and information_schema do not resolve generated expressions",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"INSERT INTO t (pk, a) VALUES (1, 1), (2, 2);",
			"CREATE INDEX idx ON t ((a + 10));",
			"CALL memory_set_generated_expression('t', 'b', '(select 1 from t sq)');",
			"CALL memory_set_generated_expression('t', '!hidden!idx!0!0', '(select 1 from t sq)');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW CREATE TABLE t;",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n" +
					"  `pk` int NOT NULL,\n" +
					"  `a` int,\n" +
					"  `b` int GENERATED ALWAYS AS (select 1 from t as sq),\n" +
					"  PRIMARY KEY (`pk`),\n" +
					"  KEY `idx` ((select 1 from t sq))\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "SHOW COLUMNS FROM t;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"a", "int", "YES", "", nil, ""},
					{"b", "int", "YES", "", nil, "VIRTUAL GENERATED"},
				},
			},
			{
				Query: "SHOW FULL COLUMNS FROM t;",
				Expected: []sql.Row{
					{"pk", "int", nil, "NO", "PRI", nil, "", "", ""},
					{"a", "int", nil, "YES", "", nil, "", "", ""},
					{"b", "int", nil, "YES", "", nil, "VIRTUAL GENERATED", "", ""},
				},
			},
			{
				Query: "SHOW EXTENDED COLUMNS FROM t;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"a", "int", "YES", "", nil, ""},
					{"b", "int", "YES", "", nil, "VIRTUAL GENERATED"},
					{"!hidden!idx!0!0", "bigint", "YES", "MUL", nil, "VIRTUAL GENERATED"},
				},
			},
			{
				Query:    "SHOW COLUMNS FROM t LIKE 'b';",
				Expected: []sql.Row{{"b", "int", "YES", "", nil, "VIRTUAL GENERATED"}},
			},
			{
				Query:    "SHOW COLUMNS FROM t WHERE Field = 'b';",
				Expected: []sql.Row{{"b", "int", "YES", "", nil, "VIRTUAL GENERATED"}},
			},
			{
				Query: "DESCRIBE t;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", nil, ""},
					{"a", "int", "YES", "", nil, ""},
					{"b", "int", "YES", "", nil, "VIRTUAL GENERATED"},
				},
			},
			{
				Query: "SHOW INDEXES FROM t;",
				Expected: []sql.Row{
					{"t", 0, "PRIMARY", 1, "pk", "A", 0, nil, nil, "", "BTREE", "", "", "YES", nil},
					{"t", 1, "idx", 1, nil, "A", 0, nil, nil, "YES", "BTREE", "", "", "YES", "(select 1 from t sq)"},
				},
			},
			{
				// TODO: generation_expression should be "(select 1 from t as sq)". See https://github.com/dolthub/go-mysql-server/issues/4091.
				Query:    "SELECT column_name, generation_expression FROM information_schema.columns WHERE table_name = 't' ORDER BY ordinal_position;",
				Expected: []sql.Row{{"pk", ""}, {"a", ""}, {"b", ""}},
			},
		},
	},
	{
		Name: "SHOW COLUMNS filters still resolve generated expressions",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"INSERT INTO t (pk, a) VALUES (1, 1), (2, 2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW COLUMNS FROM t WHERE Field IN (SELECT 'b' FROM t WHERE b = 3);",
				Expected: []sql.Row{{"b", "int", "YES", "", nil, "VIRTUAL GENERATED"}},
			},
		},
	},
	{
		Name: "DROP INDEX removes a functional index that reads its own table",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"INSERT INTO t (pk, a) VALUES (1, 1), (2, 2);",
			"CREATE INDEX idx ON t ((a + 10));",
			"CALL memory_set_generated_expression('t', '!hidden!idx!0!0', '(select 1 from t sq)');",
		},
		Assertions: []ScriptTestAssertion{
			{Query: "SELECT * FROM t;", ExpectedErr: sql.ErrGeneratedColumnRecursion},
			{
				Query:    "DROP INDEX idx ON t;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY pk;",
				Expected: []sql.Row{{1, 1, 2}, {2, 2, 3}},
			},
		},
	},
	{
		Name: "ALTER TABLE DROP INDEX removes a functional index that reads its own table",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"INSERT INTO t (pk, a) VALUES (1, 1), (2, 2);",
			"CREATE INDEX idx ON t ((a + 10));",
			"CALL memory_set_generated_expression('t', '!hidden!idx!0!0', '(select 1 from t sq)');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t DROP INDEX idx;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY pk;",
				Expected: []sql.Row{{1, 1, 2}, {2, 2, 3}},
			},
		},
	},
	{
		Name: "ALTER TABLE DROP COLUMN removes a generated column that reads its own table",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"INSERT INTO t (pk, a) VALUES (1, 1), (2, 2);",
			"CREATE INDEX idx ON t ((a + 10));",
			"CALL memory_set_generated_expression('t', 'b', '(select 1 from t sq)');",
		},
		Assertions: []ScriptTestAssertion{
			{Query: "SELECT * FROM t;", ExpectedErr: sql.ErrGeneratedColumnRecursion},
			{
				Query:    "ALTER TABLE t DROP COLUMN b;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT * FROM t ORDER BY pk;",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
		},
	},
	{
		Name: "dropping one of several invalid generated expressions fails without crashing",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"INSERT INTO t (pk, a) VALUES (1, 1), (2, 2);",
			"CREATE INDEX idx ON t ((a + 10));",
			"CALL memory_set_generated_expression('t', 'b', '(select 1 from t sq)');",
			"CALL memory_set_generated_expression('t', '!hidden!idx!0!0', '(select 1 from t sq)');",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Rewriting the table must evaluate the remaining generated expression, which can't be resolved.
				Query:       "DROP INDEX idx ON t;",
				ExpectedErr: sql.ErrDatabaseNotFound,
			},
			{
				Query:       "ALTER TABLE t DROP COLUMN b;",
				ExpectedErr: sql.ErrColumnDefaultSubquery,
			},
		},
	},
	{
		Name: "DROP TABLE removes a table whose generated expressions read it",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, a INT, b INT AS (a + 1) VIRTUAL);",
			"CREATE INDEX idx ON t ((a + 10));",
			"CALL memory_set_generated_expression('t', 'b', '(select 1 from t sq)');",
			"CALL memory_set_generated_expression('t', '!hidden!idx!0!0', '(select 1 from t sq)');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DROP TABLE t;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SHOW TABLES LIKE 't';",
				Expected: []sql.Row{},
			},
		},
	},
}
