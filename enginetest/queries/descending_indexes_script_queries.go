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

// DescendingIndexesScriptTests contains self-contained descending indexes script tests.
var DescendingIndexesScriptTests = []ScriptTest{
	{
		Name: "descending index columns",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY, a INT, b INT, c VARCHAR(20), INDEX ab (a DESC, b));",
			"INSERT INTO t VALUES (1, 1, 1, 'x'), (2, 1, 2, 'y'), (3, 2, 1, NULL), (4, NULL, 3, 'z'), (5, 3, NULL, 'w');",
			"ALTER TABLE t ADD INDEX bc (b, c(10) DESC);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW CREATE TABLE t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n" +
					"  `pk` int NOT NULL,\n" +
					"  `a` int,\n" +
					"  `b` int,\n" +
					"  `c` varchar(20),\n" +
					"  PRIMARY KEY (`pk`),\n" +
					"  KEY `ab` (`a` DESC,`b`),\n" +
					"  KEY `bc` (`b`,`c`(10) DESC)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "SELECT index_name, seq_in_index, column_name, collation, sub_part FROM information_schema.statistics WHERE table_name = 't' AND index_name <> 'PRIMARY' ORDER BY index_name, seq_in_index",
				Expected: []sql.Row{
					{"ab", 1, "a", "D", nil},
					{"ab", 2, "b", "A", nil},
					{"bc", 1, "b", "A", nil},
					{"bc", 2, "c", "D", 10},
				},
			},
			{
				Query: "SHOW INDEX FROM t",
				Expected: []sql.Row{
					{"t", 0, "PRIMARY", 1, "pk", "A", 0, nil, nil, "", "BTREE", "", "", "YES", nil},
					{"t", 1, "ab", 1, "a", "D", 0, nil, nil, "YES", "BTREE", "", "", "YES", nil},
					{"t", 1, "ab", 2, "b", "A", 0, nil, nil, "YES", "BTREE", "", "", "YES", nil},
					{"t", 1, "bc", 1, "b", "A", 0, nil, nil, "YES", "BTREE", "", "", "YES", nil},
					{"t", 1, "bc", 2, "c", "D", 0, nil, nil, "YES", "BTREE", "", "", "YES", nil},
				},
			},
			{
				Query:    "SELECT pk FROM t WHERE a >= 1 ORDER BY a DESC, b",
				Expected: []sql.Row{{5}, {3}, {1}, {2}},
			},
			{
				Query:    "SELECT pk FROM t WHERE a = 1 ORDER BY b DESC",
				Expected: []sql.Row{{2}, {1}},
			},
			{
				Query:    "SELECT pk FROM t WHERE a IS NULL",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "SELECT pk FROM t WHERE a < 3 ORDER BY pk",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				Query:    "SELECT pk FROM t ORDER BY a DESC, b",
				Expected: []sql.Row{{5}, {3}, {1}, {2}, {4}},
			},
			{
				Query:    "SELECT pk FROM t ORDER BY a, b",
				Expected: []sql.Row{{4}, {1}, {2}, {3}, {5}},
			},
			{
				Query:    "SELECT pk FROM t WHERE b = 1 AND c > 'a'",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT pk FROM t WHERE b >= 2 ORDER BY b DESC, c",
				Expected: []sql.Row{{4}, {2}},
			},
			{
				Query: "ALTER TABLE t MODIFY COLUMN a BIGINT",
			},
			{
				Query: "SHOW CREATE TABLE t",
				Expected: []sql.Row{{"t", "CREATE TABLE `t` (\n" +
					"  `pk` int NOT NULL,\n" +
					"  `a` bigint,\n" +
					"  `b` int,\n" +
					"  `c` varchar(20),\n" +
					"  PRIMARY KEY (`pk`),\n" +
					"  KEY `ab` (`a` DESC,`b`),\n" +
					"  KEY `bc` (`b`,`c`(10) DESC)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "SELECT pk FROM t WHERE a = 2",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT pk FROM t WHERE a > 1 ORDER BY a DESC",
				Expected: []sql.Row{{5}, {3}},
			},
			{
				Query: "ALTER TABLE t DROP INDEX ab",
			},
			{
				Query:    "SELECT pk FROM t WHERE a = 1 ORDER BY pk",
				Expected: []sql.Row{{1}, {2}},
			},
		},
	},
	{
		Name: "descending index lookups and ordering",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk INT PRIMARY KEY, a INT, b INT, c VARCHAR(10), INDEX ab (a DESC, b), INDEX cb (c DESC, b DESC));",
			"INSERT INTO t1 VALUES (1, 1, 1, 'x'), (2, 1, 2, 'y'), (3, 2, 1, NULL), (4, NULL, 3, 'x'), (5, 3, 4, 'z'), (6, 2, NULL, 'y'), (7, NULL, NULL, NULL);",
			"CREATE TABLE t2 (id INT PRIMARY KEY, a INT);",
			"INSERT INTO t2 VALUES (10, 1), (20, 2), (30, NULL), (40, 3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT pk FROM t1 WHERE a = 1 ORDER BY b",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE a = 1 ORDER BY b DESC",
				Expected: []sql.Row{{2}, {1}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE a > 1 ORDER BY a DESC, b",
				Expected: []sql.Row{{5}, {6}, {3}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE a >= 2 ORDER BY a, b DESC",
				Expected: []sql.Row{{3}, {6}, {5}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE a BETWEEN 1 AND 2 ORDER BY pk",
				Expected: []sql.Row{{1}, {2}, {3}, {6}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE a IN (1, 3) ORDER BY pk",
				Expected: []sql.Row{{1}, {2}, {5}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE a IS NULL ORDER BY pk",
				Expected: []sql.Row{{4}, {7}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE a IS NOT NULL ORDER BY a DESC, b, pk",
				Expected: []sql.Row{{5}, {6}, {3}, {1}, {2}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE a = 2 AND b IS NULL",
				Expected: []sql.Row{{6}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE a = 2 AND b > 0",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE a < 3 AND a > 0 ORDER BY pk",
				Expected: []sql.Row{{1}, {2}, {3}, {6}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE a <= 1 ORDER BY pk",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE c = 'y' ORDER BY b DESC",
				Expected: []sql.Row{{2}, {6}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE c > 'x' ORDER BY c DESC, b DESC",
				Expected: []sql.Row{{5}, {2}, {6}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE c >= 'x' AND c < 'z' ORDER BY pk",
				Expected: []sql.Row{{1}, {2}, {4}, {6}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE c LIKE 'x%' ORDER BY pk",
				Expected: []sql.Row{{1}, {4}},
			},
			{
				Query:    "SELECT pk, a, b FROM t1 ORDER BY a DESC, b LIMIT 3",
				Expected: []sql.Row{{5, 3, 4}, {6, 2, nil}, {3, 2, 1}},
			},
			{
				Query:    "SELECT pk, a, b FROM t1 ORDER BY a DESC, b",
				Expected: []sql.Row{{5, 3, 4}, {6, 2, nil}, {3, 2, 1}, {1, 1, 1}, {2, 1, 2}, {7, nil, nil}, {4, nil, 3}},
			},
			{
				Query:    "SELECT pk, a, b FROM t1 ORDER BY a, b DESC",
				Expected: []sql.Row{{4, nil, 3}, {7, nil, nil}, {2, 1, 2}, {1, 1, 1}, {3, 2, 1}, {6, 2, nil}, {5, 3, 4}},
			},
			{
				Query:    "SELECT a, COUNT(*) FROM t1 GROUP BY a ORDER BY a DESC",
				Expected: []sql.Row{{3, 1}, {2, 2}, {1, 2}, {nil, 2}},
			},
			{
				Query:    "SELECT MAX(a), MIN(a) FROM t1",
				Expected: []sql.Row{{3, 1}},
			},
			{
				Query:    "SELECT MAX(b) FROM t1 WHERE a = 1",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT t2.id, t1.pk FROM t2 JOIN t1 ON t1.a = t2.a ORDER BY t2.id, t1.pk",
				Expected: []sql.Row{{10, 1}, {10, 2}, {20, 3}, {20, 6}, {40, 5}},
			},
			{
				Query:    "SELECT t2.id, t1.pk FROM t2 LEFT JOIN t1 ON t1.a = t2.a AND t1.b = 1 ORDER BY t2.id, t1.pk",
				Expected: []sql.Row{{10, 1}, {20, 3}, {30, nil}, {40, nil}},
			},
			{
				Query:    "UPDATE t1 SET b = b + 10 WHERE a = 1",
				Expected: []sql.Row{{NewUpdateResult(2, 2)}},
			},
			{
				Query:    "SELECT pk, a, b FROM t1 WHERE a = 1 ORDER BY b",
				Expected: []sql.Row{{1, 1, 11}, {2, 1, 12}},
			},
			{
				Query:    "UPDATE t1 SET a = 5 WHERE pk = 5",
				Expected: []sql.Row{{NewUpdateResult(1, 1)}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE a > 2 ORDER BY a DESC",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "DELETE FROM t1 WHERE a = 2 AND b IS NULL",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT pk FROM t1 WHERE a = 2",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "DELETE FROM t1 WHERE a IS NULL",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "SELECT pk FROM t1 ORDER BY a DESC, b",
				Expected: []sql.Row{{5}, {3}, {1}, {2}},
			},
		},
	},
	{
		Name: "descending unique indexes",
		SetUpScript: []string{
			"CREATE TABLE u (pk INT PRIMARY KEY, a INT, b INT, UNIQUE KEY ua (a DESC), UNIQUE KEY uab (b DESC, a));",
			"INSERT INTO u VALUES (1, 1, 1), (2, 2, 1), (3, NULL, 2), (4, NULL, 2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW CREATE TABLE u",
				Expected: []sql.Row{{"u", "CREATE TABLE `u` (\n" +
					"  `pk` int NOT NULL,\n" +
					"  `a` int,\n" +
					"  `b` int,\n" +
					"  PRIMARY KEY (`pk`),\n" +
					"  UNIQUE KEY `ua` (`a` DESC),\n" +
					"  UNIQUE KEY `uab` (`b` DESC,`a`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "INSERT INTO u VALUES (5, 2, 5)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "INSERT INTO u VALUES (5, 5, 1)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:       "INSERT INTO u VALUES (6, 5, 1)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query:    "INSERT INTO u VALUES (7, 6, 1) ON DUPLICATE KEY UPDATE b = 9",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "INSERT INTO u VALUES (8, 6, 2) ON DUPLICATE KEY UPDATE b = 9",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "SELECT * FROM u ORDER BY pk",
				Expected: []sql.Row{{1, 1, 1}, {2, 2, 1}, {3, nil, 2}, {4, nil, 2}, {5, 5, 1}, {7, 6, 9}},
			},
			{
				Query:    "SELECT pk FROM u WHERE a = 6",
				Expected: []sql.Row{{7}},
			},
			{
				Query:    "SELECT pk FROM u WHERE b = 2 ORDER BY a, pk",
				Expected: []sql.Row{{3}, {4}},
			},
			{
				Query:    "REPLACE INTO u VALUES (9, 5, 7)",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "SELECT * FROM u ORDER BY pk",
				Expected: []sql.Row{{1, 1, 1}, {2, 2, 1}, {3, nil, 2}, {4, nil, 2}, {7, 6, 9}, {9, 5, 7}},
			},
			{
				Query:       "UPDATE u SET a = 1 WHERE pk = 9",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
		},
	},
	{
		Name: "descending prefix and expression indexes",
		SetUpScript: []string{
			"CREATE TABLE p (pk INT PRIMARY KEY, s VARCHAR(50), n INT, INDEX idx1 (s(3) DESC));",
			"ALTER TABLE p ADD INDEX idx2 ((n * -1) DESC);",
			"INSERT INTO p VALUES (1, 'apple', 1), (2, 'apricot', 2), (3, 'banana', 3), (4, NULL, NULL), (5, 'app', 5);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW CREATE TABLE p",
				Expected: []sql.Row{{"p", "CREATE TABLE `p` (\n" +
					"  `pk` int NOT NULL,\n" +
					"  `s` varchar(50),\n" +
					"  `n` int,\n" +
					"  PRIMARY KEY (`pk`),\n" +
					"  KEY `idx1` (`s`(3) DESC),\n" +
					"  KEY `idx2` (((n * -1)) DESC)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "SELECT index_name, seq_in_index, column_name, collation, sub_part FROM information_schema.statistics WHERE table_name = 'p' AND index_name <> 'PRIMARY' ORDER BY index_name, seq_in_index",
				Expected: []sql.Row{
					{"idx1", 1, "s", "D", 3},
					{"idx2", 1, nil, "D", nil},
				},
			},
			{
				Query:    "SELECT pk FROM p WHERE s LIKE 'ap%' ORDER BY pk",
				Expected: []sql.Row{{1}, {2}, {5}},
			},
			{
				Query:    "SELECT pk FROM p WHERE s = 'app'",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "SELECT pk FROM p WHERE s > 'apple' ORDER BY pk",
				Expected: []sql.Row{{2}, {3}},
			},
			{
				Query:    "SELECT pk FROM p WHERE (n * -1) = -2",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT pk FROM p WHERE (n * -1) > -3 ORDER BY pk",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				Query:    "SELECT pk FROM p ORDER BY (n * -1) DESC, pk",
				Expected: []sql.Row{{1}, {2}, {3}, {5}, {4}},
			},
			{
				Query: "ALTER TABLE p RENAME INDEX idx1 TO idx1_renamed",
			},
			{
				Query: "ALTER TABLE p DROP INDEX idx2",
			},
			{
				Query: "CREATE INDEX idx3 ON p ((n + 1), s(2) DESC)",
			},
			{
				Query: "SHOW CREATE TABLE p",
				Expected: []sql.Row{{"p", "CREATE TABLE `p` (\n" +
					"  `pk` int NOT NULL,\n" +
					"  `s` varchar(50),\n" +
					"  `n` int,\n" +
					"  PRIMARY KEY (`pk`),\n" +
					"  KEY `idx1_renamed` (`s`(3) DESC),\n" +
					"  KEY `idx3` (((n + 1)),`s`(2) DESC)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "SELECT pk FROM p WHERE (n + 1) = 3",
				Expected: []sql.Row{{2}},
			},
			{
				Query: "ALTER TABLE p ADD INDEX idx4 (n DESC, s)",
			},
			{
				Query: "CREATE INDEX idx5 ON p (n, pk DESC)",
			},
			{
				Query: "ALTER TABLE p ADD COLUMN m INT",
			},
			{
				Query: "ALTER TABLE p MODIFY COLUMN n BIGINT",
			},
			{
				Query:       "ALTER TABLE p RENAME COLUMN n TO n2",
				ExpectedErr: sql.ErrColumnFunctionalIndexDependency,
			},
			{
				Query: "SELECT index_name, seq_in_index, column_name, collation, sub_part FROM information_schema.statistics WHERE table_name = 'p' AND index_name <> 'PRIMARY' ORDER BY index_name, seq_in_index",
				Expected: []sql.Row{
					{"idx1_renamed", 1, "s", "D", 3},
					{"idx3", 1, nil, "A", nil},
					{"idx3", 2, "s", "D", 2},
					{"idx4", 1, "n", "D", nil},
					{"idx4", 2, "s", "A", nil},
					{"idx5", 1, "n", "A", nil},
					{"idx5", 2, "pk", "D", nil},
				},
			},
			{
				Query: "ALTER TABLE p DROP INDEX idx3",
			},
			{
				Query: "ALTER TABLE p RENAME COLUMN n TO n2",
			},
			{
				Query: "ALTER TABLE p DROP INDEX idx1_renamed",
			},
			{
				Query: "SHOW CREATE TABLE p",
				Expected: []sql.Row{{"p", "CREATE TABLE `p` (\n" +
					"  `pk` int NOT NULL,\n" +
					"  `s` varchar(50),\n" +
					"  `n2` bigint,\n" +
					"  `m` int,\n" +
					"  PRIMARY KEY (`pk`),\n" +
					"  KEY `idx4` (`n2` DESC,`s`),\n" +
					"  KEY `idx5` (`n2`,`pk` DESC)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "CREATE TABLE p2 LIKE p",
			},
			{
				Query: "SHOW CREATE TABLE p2",
				Expected: []sql.Row{{"p2", "CREATE TABLE `p2` (\n" +
					"  `pk` int NOT NULL,\n" +
					"  `s` varchar(50),\n" +
					"  `n2` bigint,\n" +
					"  `m` int,\n" +
					"  PRIMARY KEY (`pk`),\n" +
					"  KEY `idx4` (`n2` DESC,`s`),\n" +
					"  KEY `idx5` (`n2`,`pk` DESC)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "SELECT pk FROM p WHERE n2 > 1 ORDER BY n2 DESC",
				Expected: []sql.Row{{5}, {3}, {2}},
			},
		},
	},
	{
		Name: "descending index on a keyless table",
		SetUpScript: []string{
			"CREATE TABLE k (a INT, b INT, INDEX kab (a DESC, b DESC));",
			"INSERT INTO k VALUES (1, 1), (1, 2), (2, 1), (NULL, 5), (1, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW CREATE TABLE k",
				Expected: []sql.Row{{"k", "CREATE TABLE `k` (\n" +
					"  `a` int,\n" +
					"  `b` int,\n" +
					"  KEY `kab` (`a` DESC,`b` DESC)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "SELECT a, b FROM k WHERE a = 1 ORDER BY b DESC",
				Expected: []sql.Row{{1, 2}, {1, 1}, {1, 1}},
			},
			{
				Query:    "SELECT a, b FROM k ORDER BY a DESC, b DESC",
				Expected: []sql.Row{{2, 1}, {1, 2}, {1, 1}, {1, 1}, {nil, 5}},
			},
			{
				Query:    "SELECT a, b FROM k WHERE a IS NULL",
				Expected: []sql.Row{{nil, 5}},
			},
			{
				Query:    "SELECT COUNT(*) FROM k WHERE a = 1 AND b = 1",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "DELETE FROM k WHERE a = 1 AND b = 1",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "SELECT a, b FROM k ORDER BY a, b",
				Expected: []sql.Row{{nil, 5}, {1, 2}, {2, 1}},
			},
		},
	},
	{
		Name: "descending indexes backing foreign keys",
		SetUpScript: []string{
			"CREATE TABLE parent (id INT, v INT, UNIQUE KEY pid (id DESC));",
			"CREATE TABLE child (id INT PRIMARY KEY, parent_id INT, INDEX cpi (parent_id DESC), FOREIGN KEY (parent_id) REFERENCES parent(id));",
			"INSERT INTO parent VALUES (1, 1), (2, 2);",
			"INSERT INTO child VALUES (1, 1), (2, 2), (3, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "INSERT INTO child VALUES (4, 3)",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:       "DELETE FROM parent WHERE id = 1",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "SELECT c.id FROM child c JOIN parent p ON c.parent_id = p.id WHERE p.id = 1 ORDER BY c.id",
				Expected: []sql.Row{{1}, {3}},
			},
		},
	},
	{
		Name: "descending indexes on assorted types",
		SetUpScript: []string{
			"CREATE TABLE ty (pk INT PRIMARY KEY, d DATETIME, dc DECIMAL(10,2), f DOUBLE, bt BIGINT, e ENUM('a','b','c'), INDEX dd (d DESC), INDEX decd (dc DESC), INDEX fd (f DESC), INDEX btd (bt DESC), INDEX ed (e DESC));",
			"INSERT INTO ty VALUES (1, '2024-01-01 00:00:00', 1.50, 1.5, -1, 'a'), (2, '2024-01-03 00:00:00', 10.25, -2.5, 9223372036854775807, 'c'), (3, NULL, NULL, NULL, NULL, NULL), (4, '2024-01-02 00:00:00', -3.00, 0, 0, 'b');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT pk FROM ty WHERE d > '2024-01-01 12:00:00' ORDER BY d DESC",
				Expected: []sql.Row{{2}, {4}},
			},
			{
				Query:    "SELECT pk FROM ty WHERE dc < 5 ORDER BY dc DESC",
				Expected: []sql.Row{{1}, {4}},
			},
			{
				Query:    "SELECT pk FROM ty WHERE f >= 0 ORDER BY f DESC",
				Expected: []sql.Row{{1}, {4}},
			},
			{
				Query:    "SELECT pk FROM ty WHERE bt > -5 ORDER BY bt DESC",
				Expected: []sql.Row{{2}, {4}, {1}},
			},
			{
				Query:    "SELECT pk FROM ty WHERE e > 'a' ORDER BY e DESC",
				Expected: []sql.Row{{2}, {4}},
			},
			{
				Query:    "SELECT pk FROM ty ORDER BY d DESC",
				Expected: []sql.Row{{2}, {4}, {1}, {3}},
			},
			{
				Query:    "SELECT pk FROM ty WHERE d IS NULL",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT pk FROM ty WHERE dc = 10.25",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT pk FROM ty WHERE bt = 9223372036854775807",
				Expected: []sql.Row{{2}},
			},
		},
	},
}
