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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ForeignKeyTests will run the following statements BEFORE the SetUpScript:
// CREATE TABLE parent (id INT PRIMARY KEY, v1 INT, v2 INT, INDEX v1 (v1), INDEX v2 (v2));
// CREATE TABLE child (id INT PRIMARY KEY, v1 INT, v2 INT);
var ForeignKeyTests = []ScriptTest{
	{
		Name: "ALTER TABLE Single Named FOREIGN KEY",
		SetUpScript: []string{
			"ALTER TABLE child ADD CONSTRAINT fk_named FOREIGN KEY (v1) REFERENCES parent(v1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW CREATE TABLE child;",
				Expected: []sql.UntypedSqlRow{{"child", "CREATE TABLE `child` (\n  `id` int NOT NULL,\n  `v1` int,\n  `v2` int,\n  PRIMARY KEY (`id`),\n  KEY `fk_named` (`v1`),\n  CONSTRAINT `fk_named` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "CREATE TABLE Single Named FOREIGN KEY",
		SetUpScript: []string{
			"CREATE TABLE sibling (id int PRIMARY KEY, v1 int, CONSTRAINT fk_named FOREIGN KEY (v1) REFERENCES parent(v1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW CREATE TABLE sibling;",
				Expected: []sql.UntypedSqlRow{{"sibling", "CREATE TABLE `sibling` (\n  `id` int NOT NULL,\n  `v1` int,\n  PRIMARY KEY (`id`),\n  KEY `fk_named` (`v1`),\n  CONSTRAINT `fk_named` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "Parent table index required",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1,v2) REFERENCES parent(v1,v2);",
				ExpectedErr: sql.ErrForeignKeyMissingReferenceIndex,
			},
			{
				Query:    "ALTER TABLE child ADD CONSTRAINT fk_id FOREIGN KEY (v1) REFERENCES parent(id);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "indexes with prefix lengths are ignored for foreign keys",
		SetUpScript: []string{
			"create table prefixParent(v varchar(100), index(v(1)))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "create table prefixChild(v varchar(100), foreign key (v) references prefixParent(v))",
				ExpectedErr: sql.ErrForeignKeyMissingReferenceIndex,
			},
		},
	},
	{
		Name: "CREATE TABLE Name Collision",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE child2 (id INT PRIMARY KEY, v1 INT, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1), CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1));",
				ExpectedErr: sql.ErrForeignKeyDuplicateName,
			},
		},
	},
	{
		Name: "CREATE TABLE Type Mismatch",
		SetUpScript: []string{
			"CREATE TABLE sibling (pk INT PRIMARY KEY, v1 TIME);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE sibling ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1);",
				ExpectedErr: sql.ErrForeignKeyColumnTypeMismatch,
			},
		},
	},
	{
		Name: "CREATE TABLE Type Mismatch special case for strings",
		SetUpScript: []string{
			"CREATE TABLE parent1 (pk BIGINT PRIMARY KEY, v1 CHAR(20), INDEX (v1));",
			"CREATE TABLE parent2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(20), INDEX (v1));",
			"CREATE TABLE parent3 (pk BIGINT PRIMARY KEY, v1 BINARY(20), INDEX (v1));",
			"CREATE TABLE parent4 (pk BIGINT PRIMARY KEY, v1 VARBINARY(20), INDEX (v1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE TABLE child1 (pk BIGINT PRIMARY KEY, v1 CHAR(30), CONSTRAINT fk_child1 FOREIGN KEY (v1) REFERENCES parent1 (v1));",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "CREATE TABLE child2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(30), CONSTRAINT fk_child2 FOREIGN KEY (v1) REFERENCES parent2 (v1));",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "CREATE TABLE child3 (pk BIGINT PRIMARY KEY, v1 BINARY(30), CONSTRAINT fk_child3 FOREIGN KEY (v1) REFERENCES parent3 (v1));",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			}, {
				Query:    "CREATE TABLE child4 (pk BIGINT PRIMARY KEY, v1 VARBINARY(30), CONSTRAINT fk_child4 FOREIGN KEY (v1) REFERENCES parent4 (v1));",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "CREATE TABLE Key Count Mismatch",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1, v2);",
				ExpectedErr: sql.ErrForeignKeyColumnCountMismatch,
			},
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1, v2) REFERENCES parent(v1);",
				ExpectedErr: sql.ErrForeignKeyColumnCountMismatch,
			},
		},
	},
	{
		Name: "SET DEFAULT not supported",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE SET DEFAULT;",
				ExpectedErr: sql.ErrForeignKeySetDefault,
			},
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1) ON UPDATE SET DEFAULT;",
				ExpectedErr: sql.ErrForeignKeySetDefault,
			},
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1) ON UPDATE SET DEFAULT ON DELETE SET DEFAULT;",
				ExpectedErr: sql.ErrForeignKeySetDefault,
			},
		},
	},
	{
		Name: "CREATE TABLE Disallow TEXT/BLOB",
		SetUpScript: []string{
			"CREATE TABLE parent1 (id INT PRIMARY KEY, v1 TINYTEXT, v2 TEXT, v3 MEDIUMTEXT, v4 LONGTEXT);",
			"CREATE TABLE parent2 (id INT PRIMARY KEY, v1 TINYBLOB, v2 BLOB, v3 MEDIUMBLOB, v4 LONGBLOB);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE child11 (id INT PRIMARY KEY, parent_v1 TINYTEXT, FOREIGN KEY (parent_v1) REFERENCES parent1(v1));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
			{
				Query:       "CREATE TABLE child12 (id INT PRIMARY KEY, parent_v2 TEXT, FOREIGN KEY (parent_v2) REFERENCES parent1(v2));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
			{
				Query:       "CREATE TABLE child13 (id INT PRIMARY KEY, parent_v3 MEDIUMTEXT, FOREIGN KEY (parent_v3) REFERENCES parent1(v3));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
			{
				Query:       "CREATE TABLE child14 (id INT PRIMARY KEY, parent_v4 LONGTEXT, FOREIGN KEY (parent_v4) REFERENCES parent1(v4));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
			{
				Query:       "CREATE TABLE child21 (id INT PRIMARY KEY, parent_v1 TINYBLOB, FOREIGN KEY (parent_v1) REFERENCES parent2(v1));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
			{
				Query:       "CREATE TABLE child22 (id INT PRIMARY KEY, parent_v2 BLOB, FOREIGN KEY (parent_v2) REFERENCES parent2(v2));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
			{
				Query:       "CREATE TABLE child23 (id INT PRIMARY KEY, parent_v3 MEDIUMBLOB, FOREIGN KEY (parent_v3) REFERENCES parent2(v3));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
			{
				Query:       "CREATE TABLE child24 (id INT PRIMARY KEY, parent_v4 LONGBLOB, FOREIGN KEY (parent_v4) REFERENCES parent2(v4));",
				ExpectedErr: sql.ErrForeignKeyTextBlob,
			},
		},
	},
	{
		Name: "CREATE TABLE Non-existent Table",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES father(v1);",
				ExpectedErr: sql.ErrTableNotFound,
			},
		},
	},
	{
		Name: "CREATE TABLE Non-existent Columns",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (random) REFERENCES parent(v1);",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(random);",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
		},
	},
	{
		Name: "ALTER TABLE Foreign Key Name Collision",
		SetUpScript: []string{
			"ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1);",
				ExpectedErr: sql.ErrForeignKeyDuplicateName,
			},
		},
	},
	{
		Name: "ALTER TABLE DROP FOREIGN KEY",
		SetUpScript: []string{
			"ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW CREATE TABLE child;",
				Expected: []sql.UntypedSqlRow{{"child", "CREATE TABLE `child` (\n  `id` int NOT NULL,\n  `v1` int,\n  `v2` int,\n  PRIMARY KEY (`id`),\n  KEY `fk_name` (`v1`),\n  CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "ALTER TABLE child DROP FOREIGN KEY fk_name;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "SHOW CREATE TABLE child;",
				Expected: []sql.UntypedSqlRow{{"child", "CREATE TABLE `child` (\n  `id` int NOT NULL,\n  `v1` int,\n  `v2` int,\n  PRIMARY KEY (`id`),\n  KEY `fk_name` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:       "ALTER TABLE child DROP FOREIGN KEY fk_name;",
				ExpectedErr: sql.ErrForeignKeyNotFound,
			},
		},
	},
	{
		Name: "ALTER TABLE SET NULL on non-nullable column",
		SetUpScript: []string{
			"ALTER TABLE child MODIFY v1 int NOT NULL;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE SET NULL;",
				ExpectedErr: sql.ErrForeignKeySetNullNonNullable,
			},
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1) ON UPDATE SET NULL;",
				ExpectedErr: sql.ErrForeignKeySetNullNonNullable,
			},
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE SET NULL ON UPDATE SET NULL;",
				ExpectedErr: sql.ErrForeignKeySetNullNonNullable,
			},
		},
	},
	{
		Name: "ADD FOREIGN KEY fails on existing table when data would cause violation",
		SetUpScript: []string{
			"INSERT INTO parent VALUES (1, 1, 1), (2, 2, 2);",
			"INSERT INTO child VALUES (1, 1, 1), (2, 3, 2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1)",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
		},
	},
	{
		Name: "RENAME TABLE",
		SetUpScript: []string{
			"ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1);",
			"RENAME TABLE parent TO new_parent;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW CREATE TABLE child;",
				Expected: []sql.UntypedSqlRow{{"child", "CREATE TABLE `child` (\n  `id` int NOT NULL,\n  `v1` int,\n  `v2` int,\n  PRIMARY KEY (`id`),\n  KEY `fk_name` (`v1`),\n  CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `new_parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "RENAME TABLE child TO new_child;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "SHOW CREATE TABLE new_child;",
				Expected: []sql.UntypedSqlRow{{"new_child", "CREATE TABLE `new_child` (\n  `id` int NOT NULL,\n  `v1` int,\n  `v2` int,\n  PRIMARY KEY (`id`),\n  KEY `fk_name` (`v1`),\n  CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `new_parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/7959
		Name: "RENAME TABLE with autogenerated FK name",
		SetUpScript: []string{
			`CREATE TABLE a_test (id INT PRIMARY KEY);`,
			`CREATE TABLE b_test(
					a_id INT,
					FOREIGN KEY (a_id) REFERENCES a_test (id)
				);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "RENAME TABLE b_test TO c_test;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SHOW CREATE TABLE c_test;",
				Expected: []sql.UntypedSqlRow{{"c_test", "CREATE TABLE `c_test` (\n  `a_id` int,\n  KEY `a_id` (`a_id`),\n  " +
					"CONSTRAINT `c_test_ibfk_1` FOREIGN KEY (`a_id`) REFERENCES `a_test` (`id`)\n) " +
					"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "RENAME TABLE with primary key indexes",
		SetUpScript: []string{
			"CREATE TABLE parent1 (pk BIGINT PRIMARY KEY);",
			"CREATE TABLE child1 (pk BIGINT PRIMARY KEY, CONSTRAINT `fk` FOREIGN KEY (pk) REFERENCES parent1(pk))",
			"RENAME TABLE parent1 TO new_parent1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW CREATE TABLE child1;",
				Expected: []sql.UntypedSqlRow{{"child1", "CREATE TABLE `child1` (\n  `pk` bigint NOT NULL,\n  PRIMARY KEY (`pk`),\n  CONSTRAINT `fk` FOREIGN KEY (`pk`) REFERENCES `new_parent1` (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "RENAME TABLE child1 TO new_child1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "SHOW CREATE TABLE new_child1;",
				Expected: []sql.UntypedSqlRow{{"new_child1", "CREATE TABLE `new_child1` (\n  `pk` bigint NOT NULL,\n  PRIMARY KEY (`pk`),\n  CONSTRAINT `fk` FOREIGN KEY (`pk`) REFERENCES `new_parent1` (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "DROP TABLE",
		SetUpScript: []string{
			"ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "DROP TABLE parent;",
				ExpectedErr: sql.ErrForeignKeyDropTable,
			},
			{
				Query:    "DROP TABLE child;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "DROP TABLE parent;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "DROP SELF REFERENCED TABLE",
		SetUpScript: []string{
			"create table t ( i int primary key, j int, index(j), foreign key (j) references t(i));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DROP TABLE t;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Indexes used by foreign keys can't be dropped",
		SetUpScript: []string{
			"ALTER TABLE child ADD INDEX v1 (v1);",
			"ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child DROP INDEX v1;",
				ExpectedErr: sql.ErrForeignKeyDropIndex,
			},
			{
				Query:       "ALTER TABLE parent DROP INDEX v1;",
				ExpectedErr: sql.ErrForeignKeyDropIndex,
			},
			{
				Query:    "ALTER TABLE child DROP FOREIGN KEY fk_name;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child DROP INDEX v1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE parent DROP INDEX v1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "ALTER TABLE RENAME COLUMN",
		SetUpScript: []string{
			"ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1);",
			"ALTER TABLE parent RENAME COLUMN v1 TO v1_new;",
			"ALTER TABLE child RENAME COLUMN v1 TO v1_new;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW CREATE TABLE child;",
				Expected: []sql.UntypedSqlRow{{"child", "CREATE TABLE `child` (\n  `id` int NOT NULL,\n  `v1_new` int,\n  `v2` int,\n  PRIMARY KEY (`id`),\n  KEY `fk1` (`v1_new`),\n  CONSTRAINT `fk1` FOREIGN KEY (`v1_new`) REFERENCES `parent` (`v1_new`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "ALTER TABLE MODIFY COLUMN type change not allowed",
		SetUpScript: []string{
			"ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES parent(v1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE parent MODIFY v1 MEDIUMINT;",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE child MODIFY v1 MEDIUMINT;",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
		},
	},
	{
		Name: "ALTER TABLE MODIFY COLUMN type change allowed when lengthening string",
		SetUpScript: []string{
			"CREATE TABLE parent1 (pk BIGINT PRIMARY KEY, v1 CHAR(20), INDEX (v1));",
			"CREATE TABLE parent2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(20), INDEX (v1));",
			"CREATE TABLE parent3 (pk BIGINT PRIMARY KEY, v1 BINARY(20), INDEX (v1));",
			"CREATE TABLE parent4 (pk BIGINT PRIMARY KEY, v1 VARBINARY(20), INDEX (v1));",
			"CREATE TABLE child1 (pk BIGINT PRIMARY KEY, v1 CHAR(20), CONSTRAINT fk_child1 FOREIGN KEY (v1) REFERENCES parent1 (v1));",
			"CREATE TABLE child2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(20), CONSTRAINT fk_child2 FOREIGN KEY (v1) REFERENCES parent2 (v1));",
			"CREATE TABLE child3 (pk BIGINT PRIMARY KEY, v1 BINARY(20), CONSTRAINT fk_child3 FOREIGN KEY (v1) REFERENCES parent3 (v1));",
			"CREATE TABLE child4 (pk BIGINT PRIMARY KEY, v1 VARBINARY(20), CONSTRAINT fk_child4 FOREIGN KEY (v1) REFERENCES parent4 (v1));",
			"INSERT INTO parent2 VALUES (1, 'aa'), (2, 'bb');",
			"INSERT INTO child2 VALUES (1, 'aa');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE parent1 MODIFY v1 CHAR(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE child1 MODIFY v1 CHAR(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE parent2 MODIFY v1 VARCHAR(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE child2 MODIFY v1 VARCHAR(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE parent3 MODIFY v1 BINARY(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE child3 MODIFY v1 BINARY(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE parent4 MODIFY v1 VARBINARY(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE child4 MODIFY v1 VARBINARY(10);",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:    "ALTER TABLE parent1 MODIFY v1 CHAR(30);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child1 MODIFY v1 CHAR(30);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE parent2 MODIFY v1 VARCHAR(30);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child2 MODIFY v1 VARCHAR(30);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE parent3 MODIFY v1 BINARY(30);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child3 MODIFY v1 BINARY(30);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE parent4 MODIFY v1 VARBINARY(30);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child4 MODIFY v1 VARBINARY(30);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{ // Make sure the type change didn't cause INSERTs to break or some other strange behavior
				Query:    "INSERT INTO child2 VALUES (2, 'bb');",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:       "INSERT INTO child2 VALUES (3, 'cc');",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
		},
	},
	{
		Name: "ALTER TABLE MODIFY COLUMN type change only cares about foreign key columns",
		SetUpScript: []string{
			"CREATE TABLE parent1 (pk INT PRIMARY KEY, v1 INT UNSIGNED, v2 INT UNSIGNED, INDEX (v1));",
			"CREATE TABLE child1 (pk INT PRIMARY KEY, v1 INT UNSIGNED, v2 INT UNSIGNED, CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent1(v1));",
			"INSERT INTO parent1 VALUES (1, 2, 3), (4, 5, 6);",
			"INSERT INTO child1 VALUES (7, 2, 9);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE parent1 MODIFY v1 BIGINT;",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:       "ALTER TABLE child1 MODIFY v1 BIGINT;",
				ExpectedErr: sql.ErrForeignKeyTypeChange,
			},
			{
				Query:    "ALTER TABLE parent1 MODIFY v2 BIGINT;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child1 MODIFY v2 BIGINT;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "DROP COLUMN parent",
		SetUpScript: []string{
			"ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE parent DROP COLUMN v1;",
				ExpectedErr: sql.ErrForeignKeyDropColumn,
			},
			{
				Query:    "ALTER TABLE child DROP FOREIGN KEY fk_name;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE parent DROP COLUMN v1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "DROP COLUMN child",
		SetUpScript: []string{
			"ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child DROP COLUMN v1;",
				ExpectedErr: sql.ErrForeignKeyDropColumn,
			},
			{
				Query:    "ALTER TABLE child DROP FOREIGN KEY fk_name;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child DROP COLUMN v1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Disallow change column to nullable with ON UPDATE SET NULL",
		SetUpScript: []string{
			"ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON UPDATE SET NULL",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child CHANGE COLUMN v1 v1 INT NOT NULL;",
				ExpectedErr: sql.ErrForeignKeyTypeChangeSetNull,
			},
		},
	},
	{
		Name: "Disallow change column to nullable with ON DELETE SET NULL",
		SetUpScript: []string{
			"ALTER TABLE child ADD CONSTRAINT fk_name FOREIGN KEY (v1) REFERENCES parent(v1) ON DELETE SET NULL",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child CHANGE COLUMN v1 v1 INT NOT NULL;",
				ExpectedErr: sql.ErrForeignKeyTypeChangeSetNull,
			},
		},
	},
	{
		Name: "SQL CASCADE",
		SetUpScript: []string{
			"CREATE TABLE one (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX v1 (v1));",
			"CREATE TABLE two (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX v1v2 (v1, v2), CONSTRAINT fk_name_1 FOREIGN KEY (v1) REFERENCES one(v1) ON DELETE CASCADE ON UPDATE CASCADE);",
			"CREATE TABLE three (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, CONSTRAINT fk_name_2 FOREIGN KEY (v1, v2) REFERENCES two(v1, v2) ON DELETE CASCADE ON UPDATE CASCADE);",
			"INSERT INTO one VALUES (1, 1, 4), (2, 2, 5), (3, 3, 6), (4, 4, 5);",
			"INSERT INTO two VALUES (2, 1, 1), (3, 2, 2), (4, 3, 3), (5, 4, 4);",
			"INSERT INTO three VALUES (3, 1, 1), (4, 2, 2), (5, 3, 3), (6, 4, 4);",
			"UPDATE one SET v1 = v1 + v2;",
			"DELETE one FROM one WHERE pk = 3;",
			"UPDATE two SET v2 = v1 - 2;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM one;",
				Expected: []sql.UntypedSqlRow{{1, 5, 4}, {2, 7, 5}, {4, 9, 5}},
			},
			{
				Query:    "SELECT * FROM two;",
				Expected: []sql.UntypedSqlRow{{2, 5, 3}, {3, 7, 5}},
			},
			{
				Query:    "SELECT * FROM three;",
				Expected: []sql.UntypedSqlRow{{3, 5, 3}, {4, 7, 5}},
			},
		},
	},
	{
		Name: "SQL SET NULL",
		SetUpScript: []string{
			"CREATE TABLE one (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX v1 (v1));",
			"CREATE TABLE two (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, CONSTRAINT fk_name_1 FOREIGN KEY (v1) REFERENCES one(v1) ON DELETE SET NULL ON UPDATE SET NULL);",
			"INSERT INTO one VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);",
			"INSERT INTO two VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);",
			"UPDATE one SET v1 = v1 * v2;",
			"INSERT INTO one VALUES (4, 4, 4);",
			"INSERT INTO two VALUES (4, 4, 4);",
			"UPDATE one SET v2 = v1 * v2;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM one;",
				Expected: []sql.UntypedSqlRow{{1, 1, 1}, {2, 4, 8}, {3, 9, 27}, {4, 4, 16}},
			},
			{
				Query:    "SELECT * FROM two;",
				Expected: []sql.UntypedSqlRow{{1, 1, 1}, {2, nil, 2}, {3, nil, 3}, {4, 4, 4}},
			},
			{
				Query:    "DELETE one FROM one inner join two on one.pk=two.pk;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(4)}},
			},
			{
				Query:    "select * from two;",
				Expected: []sql.UntypedSqlRow{{1, nil, 1}, {2, nil, 2}, {3, nil, 3}, {4, nil, 4}},
			},
		},
	},
	{
		Name: "SQL RESTRICT",
		SetUpScript: []string{
			"CREATE TABLE one (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX v1 (v1));",
			"CREATE TABLE two (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, CONSTRAINT fk_name_1 FOREIGN KEY (v1) REFERENCES one(v1) ON DELETE RESTRICT ON UPDATE RESTRICT);",
			"INSERT INTO one VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);",
			"INSERT INTO two VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "UPDATE one SET v1 = v1 + v2;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "UPDATE one SET v1 = v1;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{Info: plan.UpdateInfo{Matched: 3}}}},
			},
			{
				Query:       "DELETE FROM one;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:       "DELETE one FROM one inner join two on one.pk=two.pk;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:       "DELETE one, two FROM one inner join two on one.pk=two.pk;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
		},
	},
	{
		Name: "Multi-table DELETE FROM JOIN with multiple foreign keys",
		SetUpScript: []string{
			"CREATE TABLE one (pk int PRIMARY KEY);",
			"CREATE TABLE two (pk int PRIMARY KEY);",
			"CREATE TABLE three (pk int PRIMARY KEY, fk3 int, CONSTRAINT fk_3 FOREIGN KEY (fk3) REFERENCES one(pk) ON DELETE CASCADE);",
			"CREATE TABLE four (pk int PRIMARY KEY, fk4 int, CONSTRAINT fk_4 FOREIGN KEY (fk4) REFERENCES two(pk) ON DELETE CASCADE);",
			"INSERT INTO one VALUES (1), (2), (3);",
			"INSERT INTO two VALUES (1), (2), (3);",
			"INSERT INTO three VALUES (1, 1), (2, 2), (3, 3);",
			"INSERT INTO four VALUES (1, 1), (2, 2), (3, 3);",
			"DELETE one, two FROM one inner join two on one.pk=two.pk",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * from three union all select * from four;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "Single-table DELETE FROM JOIN with multiple foreign keys",
		SetUpScript: []string{
			"CREATE TABLE one (pk int PRIMARY KEY);",
			"CREATE TABLE two (pk int PRIMARY KEY);",
			"CREATE TABLE three (pk int PRIMARY KEY, fk3 int, CONSTRAINT fk_3 FOREIGN KEY (fk3) REFERENCES one(pk) ON DELETE CASCADE);",
			"CREATE TABLE four (pk int PRIMARY KEY, fk4 int, CONSTRAINT fk_4 FOREIGN KEY (fk4) REFERENCES two(pk) ON DELETE CASCADE);",
			"INSERT INTO one VALUES (1), (2), (3);",
			"INSERT INTO two VALUES (1), (2), (3);",
			"INSERT INTO three VALUES (1, 1), (2, 2), (3, 3);",
			"INSERT INTO four VALUES (1, 1), (2, 2), (3, 3);",
			"DELETE t1 FROM one t1 inner join two t2 on t1.pk=t2.pk",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * from three;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "select * from four;",
				Expected: []sql.UntypedSqlRow{{1, 1}, {2, 2}, {3, 3}},
			},
		},
	},
	{
		Name: "SQL no reference options",
		SetUpScript: []string{
			"CREATE TABLE one (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX v1 (v1));",
			"CREATE TABLE two (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, CONSTRAINT fk_name_1 FOREIGN KEY (v1) REFERENCES one(v1));",
			"INSERT INTO one VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);",
			"INSERT INTO two VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "UPDATE one SET v1 = v1 + v2;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "UPDATE one SET v1 = v1;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{Info: plan.UpdateInfo{Matched: 3}}}},
			},
			{
				Query:       "DELETE FROM one;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
		},
	},
	{
		Name: "SQL INSERT multiple keys violates only one",
		SetUpScript: []string{
			"CREATE TABLE one (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX v1 (v1), INDEX v2 (v2));",
			"CREATE TABLE two (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, CONSTRAINT fk_name_1 FOREIGN KEY (v1) REFERENCES one(v1), CONSTRAINT fk_name_2 FOREIGN KEY (v2) REFERENCES one(v2));",
			"INSERT INTO one VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);",
			"INSERT INTO two VALUES (1, NULL, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "INSERT INTO two VALUES (2, NULL, 4);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:       "INSERT INTO two VALUES (3, 4, NULL);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:    "INSERT INTO two VALUES (4, NULL, NULL);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
		},
	},
	{
		// We differ from MySQL here as we do not allow duplicate indexes (required in MySQL to reference the same
		// column in self-referential) but we do reuse existing indexes (MySQL requires unique indexes for parent and
		// child rows).
		Name: "Self-referential same column(s)",
		SetUpScript: []string{
			"CREATE INDEX v1v2 ON parent(v1, v2);",
			"CREATE TABLE parent2 (id INT PRIMARY KEY, v1 INT, v2 INT, INDEX v1v2 (v1, v2));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE parent ADD CONSTRAINT fk_name1 FOREIGN KEY (v1) REFERENCES parent(v1);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE parent ADD CONSTRAINT fk_name2 FOREIGN KEY (v1, v2) REFERENCES parent(v1, v2);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Self-referential child column follows parent RESTRICT",
		SetUpScript: []string{
			"ALTER TABLE parent ADD CONSTRAINT fk_named FOREIGN KEY (v2) REFERENCES parent(v1);", // default reference option is RESTRICT
			"INSERT INTO parent VALUES (1, 1, 1), (2, 2, 1), (3, 3, NULL);",
			"UPDATE parent SET v1 = 1 WHERE id = 1;",
			"UPDATE parent SET v1 = 4 WHERE id = 3;",
			"DELETE FROM parent WHERE id = 3;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.UntypedSqlRow{{1, 1, 1}, {2, 2, 1}},
			},
			{
				Query:       "DELETE FROM parent WHERE v1 = 1;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:       "UPDATE parent SET v1 = 2;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:       "REPLACE INTO parent VALUES (1, 1, 1);",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
		},
	},
	{
		Name: "Self-referential child column follows parent CASCADE",
		SetUpScript: []string{
			"ALTER TABLE parent ADD CONSTRAINT fk_named FOREIGN KEY (v2) REFERENCES parent(v1) ON UPDATE CASCADE ON DELETE CASCADE;",
			"INSERT INTO parent VALUES (1, 1, 1), (2, 2, 1), (3, 3, NULL);",
			"UPDATE parent SET v1 = 1 WHERE id = 1;",
			"UPDATE parent SET v1 = 4 WHERE id = 3;",
			"DELETE FROM parent WHERE id = 3;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "UPDATE parent SET v1 = 2;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "REPLACE INTO parent VALUES (1, 1, 1), (2, 2, 2);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(3)}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.UntypedSqlRow{{1, 1, 1}, {2, 2, 2}},
			},
			{
				Query:       "UPDATE parent SET v1 = 2;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "SELECT * FROM parent order by v1;",
				Expected: []sql.UntypedSqlRow{{1, 1, 1}, {2, 2, 2}},
			},
			{
				Query:       "UPDATE parent SET v1 = 2 WHERE id = 1;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "SELECT * FROM parent order by v1;",
				Expected: []sql.UntypedSqlRow{{1, 1, 1}, {2, 2, 2}},
			},
			{
				Query:       "REPLACE INTO parent VALUES (1, 1, 2), (2, 2, 1);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:    "SELECT * FROM parent order by v1;",
				Expected: []sql.UntypedSqlRow{{1, 1, 1}, {2, 2, 2}},
			},
			{
				Query:    "UPDATE parent SET v2 = 2 WHERE id = 1;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "UPDATE parent SET v2 = 1 WHERE id = 2;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "SELECT * FROM parent order by v1;",
				Expected: []sql.UntypedSqlRow{{1, 1, 2}, {2, 2, 1}},
			},
			{
				Query:       "UPDATE parent SET v1 = 2;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:       "UPDATE parent SET v1 = 2 WHERE id = 1;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "DELETE FROM parent WHERE v1 = 1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "Self-referential child column follows parent SET NULL",
		SetUpScript: []string{
			"ALTER TABLE parent ADD CONSTRAINT fk_named FOREIGN KEY (v2) REFERENCES parent(v1) ON UPDATE SET NULL ON DELETE SET NULL;",
			"INSERT INTO parent VALUES (1,1,1), (2, 2, 1), (3, 3, NULL);",
			"UPDATE parent SET v1 = 1 WHERE id = 1;",
			"UPDATE parent SET v1 = 4 WHERE id = 3;",
			"DELETE FROM parent WHERE id = 3;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "UPDATE parent SET v1 = 2;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "REPLACE INTO parent VALUES (1, 1, 1), (2, 2, 2);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(4)}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.UntypedSqlRow{{1, 1, 1}, {2, 2, 2}},
			},
			{
				Query:       "UPDATE parent SET v1 = 2;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:       "UPDATE parent SET v1 = 2 WHERE id = 1;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "REPLACE INTO parent VALUES (1,1,2), (2,2,1);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(4)}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.UntypedSqlRow{{1, 1, nil}, {2, 2, 1}},
			},
			{
				Query:    "UPDATE parent SET v2 = 2 WHERE id = 1;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "UPDATE parent SET v2 = 1 WHERE id = 2;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 1}}}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.UntypedSqlRow{{1, 1, 2}, {2, 2, 1}},
			},
			{
				Query:       "UPDATE parent SET v1 = 2;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:       "UPDATE parent SET v1 = 2 WHERE id = 1;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "DELETE FROM parent WHERE v1 = 1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.UntypedSqlRow{{2, 2, nil}},
			},
		},
	},
	{
		// Self-referential foreign key analysis time used to take an exponential amount of time, roughly equivalent to:
		// number_of_foreign_keys ^ 15, so this verifies that it no longer does this (as the test would take years to run)
		Name: "Multiple self-referential foreign keys without data",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT UNIQUE, v2 BIGINT UNIQUE, v3 BIGINT UNIQUE, v4 BIGINT UNIQUE," +
				"v5 BIGINT UNIQUE, v6 BIGINT UNIQUE, v7 BIGINT UNIQUE," +
				"CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES test (pk)," +
				"CONSTRAINT fk2 FOREIGN KEY (v2) REFERENCES test (pk)," +
				"CONSTRAINT fk3 FOREIGN KEY (v3) REFERENCES test (pk)," +
				"CONSTRAINT fk4 FOREIGN KEY (v4) REFERENCES test (pk)," +
				"CONSTRAINT fk5 FOREIGN KEY (v5) REFERENCES test (pk)," +
				"CONSTRAINT fk6 FOREIGN KEY (v6) REFERENCES test (pk)," +
				"CONSTRAINT fk7 FOREIGN KEY (v7) REFERENCES test (pk));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `UPDATE test SET v1 = NULL, v2 = NULL WHERE test.pk = 0;`,
				Expected: []sql.UntypedSqlRow{{types.OkResult{
					RowsAffected: 0,
					InsertID:     0,
					Info: plan.UpdateInfo{
						Matched:  0,
						Updated:  0,
						Warnings: 0,
					},
				}}},
			},
		},
	},
	{
		Name: "Self-referential delete cascade depth limit",
		SetUpScript: []string{
			"CREATE TABLE under_limit(pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX idx_v1(v1));",
			"CREATE TABLE over_limit(pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX idx_v1(v1));",
			"INSERT INTO under_limit VALUES (1,2),(2,3),(3,4),(4,5),(5,6),(6,7),(7,8),(8,9),(9,10),(10,11),(11,12),(12,13),(13,14),(14,1);",
			"INSERT INTO over_limit VALUES (1,2),(2,3),(3,4),(4,5),(5,6),(6,7),(7,8),(8,9),(9,10),(10,11),(11,12),(12,13),(13,14),(14,15),(15,1);",
			"ALTER TABLE under_limit ADD CONSTRAINT fk_under FOREIGN KEY (v1) REFERENCES under_limit(pk) ON UPDATE CASCADE ON DELETE CASCADE;",
			"ALTER TABLE over_limit ADD CONSTRAINT fk_over FOREIGN KEY (v1) REFERENCES over_limit(pk) ON UPDATE CASCADE ON DELETE CASCADE;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DELETE FROM under_limit WHERE pk = 1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:       "DELETE FROM over_limit WHERE pk = 1;",
				ExpectedErr: sql.ErrForeignKeyDepthLimit,
			},
			{
				Query:    "DELETE FROM over_limit WHERE pk = 0;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "UPDATE over_limit SET pk = 1 WHERE pk = 1;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{
					RowsAffected: 0,
					InsertID:     0,
					Info: plan.UpdateInfo{
						Matched:  1,
						Updated:  0,
						Warnings: 0,
					},
				}}},
			},
			{
				Query:       "UPDATE over_limit SET pk = 2 WHERE pk = 1;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
		},
	},
	{
		Name: "Cyclic 2-table delete cascade depth limit",
		SetUpScript: []string{
			"CREATE TABLE under_cycle1(pk BIGINT PRIMARY KEY, v1 BIGINT UNIQUE);",
			"CREATE TABLE under_cycle2(pk BIGINT PRIMARY KEY, v1 BIGINT UNIQUE);",
			"INSERT INTO under_cycle1 VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7);",
			"INSERT INTO under_cycle2 VALUES (1,2),(2,3),(3,4),(4,5),(5,6),(6,7),(7,1);",
			"ALTER TABLE under_cycle1 ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES under_cycle2(pk) ON UPDATE CASCADE ON DELETE CASCADE;",
			"ALTER TABLE under_cycle2 ADD CONSTRAINT fk2 FOREIGN KEY (v1) REFERENCES under_cycle1(pk) ON UPDATE CASCADE ON DELETE CASCADE;",
			"CREATE TABLE over_cycle1(pk BIGINT PRIMARY KEY, v1 BIGINT UNIQUE);",
			"CREATE TABLE over_cycle2(pk BIGINT PRIMARY KEY, v1 BIGINT UNIQUE);",
			"INSERT INTO over_cycle1 VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8);",
			"INSERT INTO over_cycle2 VALUES (1,2),(2,3),(3,4),(4,5),(5,6),(6,7),(7,8),(8,1);",
			"ALTER TABLE over_cycle1 ADD CONSTRAINT fk3 FOREIGN KEY (v1) REFERENCES over_cycle2(pk) ON UPDATE CASCADE ON DELETE CASCADE;",
			"ALTER TABLE over_cycle2 ADD CONSTRAINT fk4 FOREIGN KEY (v1) REFERENCES over_cycle1(pk) ON UPDATE CASCADE ON DELETE CASCADE;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DELETE FROM under_cycle1 WHERE pk = 1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:       "DELETE FROM over_cycle1 WHERE pk = 1;",
				ExpectedErr: sql.ErrForeignKeyDepthLimit,
			},
		},
	},
	{
		Name: "Cyclic 3-table delete cascade depth limit",
		SetUpScript: []string{
			"CREATE TABLE under_cycle1(pk BIGINT PRIMARY KEY, v1 BIGINT UNIQUE);",
			"CREATE TABLE under_cycle2(pk BIGINT PRIMARY KEY, v1 BIGINT UNIQUE);",
			"CREATE TABLE under_cycle3(pk BIGINT PRIMARY KEY, v1 BIGINT UNIQUE);",
			"INSERT INTO under_cycle1 VALUES (1,1),(2,2),(3,3),(4,4);",
			"INSERT INTO under_cycle2 VALUES (1,1),(2,2),(3,3),(4,4);",
			"INSERT INTO under_cycle3 VALUES (1,2),(2,3),(3,4),(4,1);",
			"ALTER TABLE under_cycle1 ADD CONSTRAINT fk1 FOREIGN KEY (v1) REFERENCES under_cycle2(pk) ON UPDATE CASCADE ON DELETE CASCADE;",
			"ALTER TABLE under_cycle2 ADD CONSTRAINT fk2 FOREIGN KEY (v1) REFERENCES under_cycle3(pk) ON UPDATE CASCADE ON DELETE CASCADE;",
			"ALTER TABLE under_cycle3 ADD CONSTRAINT fk3 FOREIGN KEY (v1) REFERENCES under_cycle1(pk) ON UPDATE CASCADE ON DELETE CASCADE;",
			"CREATE TABLE over_cycle1(pk BIGINT PRIMARY KEY, v1 BIGINT UNIQUE);",
			"CREATE TABLE over_cycle2(pk BIGINT PRIMARY KEY, v1 BIGINT UNIQUE);",
			"CREATE TABLE over_cycle3(pk BIGINT PRIMARY KEY, v1 BIGINT UNIQUE);",
			"INSERT INTO over_cycle1 VALUES (1,1),(2,2),(3,3),(4,4),(5,5);",
			"INSERT INTO over_cycle2 VALUES (1,1),(2,2),(3,3),(4,4),(5,5);",
			"INSERT INTO over_cycle3 VALUES (1,2),(2,3),(3,4),(4,5),(5,1);",
			"ALTER TABLE over_cycle1 ADD CONSTRAINT fk4 FOREIGN KEY (v1) REFERENCES over_cycle2(pk) ON UPDATE CASCADE ON DELETE CASCADE;",
			"ALTER TABLE over_cycle2 ADD CONSTRAINT fk5 FOREIGN KEY (v1) REFERENCES over_cycle3(pk) ON UPDATE CASCADE ON DELETE CASCADE;",
			"ALTER TABLE over_cycle3 ADD CONSTRAINT fk6 FOREIGN KEY (v1) REFERENCES over_cycle1(pk) ON UPDATE CASCADE ON DELETE CASCADE;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DELETE FROM under_cycle1 WHERE pk = 1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:       "DELETE FROM over_cycle1 WHERE pk = 1;",
				ExpectedErr: sql.ErrForeignKeyDepthLimit,
			},
		},
	},
	{
		Name: "Acyclic delete cascade depth limit",
		SetUpScript: []string{
			"CREATE TABLE t1(pk BIGINT PRIMARY KEY);",
			"CREATE TABLE t2(pk BIGINT PRIMARY KEY, CONSTRAINT fk1 FOREIGN KEY (pk) REFERENCES t1(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t3(pk BIGINT PRIMARY KEY, CONSTRAINT fk2 FOREIGN KEY (pk) REFERENCES t2(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t4(pk BIGINT PRIMARY KEY, CONSTRAINT fk3 FOREIGN KEY (pk) REFERENCES t3(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t5(pk BIGINT PRIMARY KEY, CONSTRAINT fk4 FOREIGN KEY (pk) REFERENCES t4(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t6(pk BIGINT PRIMARY KEY, CONSTRAINT fk5 FOREIGN KEY (pk) REFERENCES t5(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t7(pk BIGINT PRIMARY KEY, CONSTRAINT fk6 FOREIGN KEY (pk) REFERENCES t6(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t8(pk BIGINT PRIMARY KEY, CONSTRAINT fk7 FOREIGN KEY (pk) REFERENCES t7(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t9(pk BIGINT PRIMARY KEY, CONSTRAINT fk8 FOREIGN KEY (pk) REFERENCES t8(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t10(pk BIGINT PRIMARY KEY, CONSTRAINT fk9 FOREIGN KEY (pk) REFERENCES t9(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t11(pk BIGINT PRIMARY KEY, CONSTRAINT fk10 FOREIGN KEY (pk) REFERENCES t10(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t12(pk BIGINT PRIMARY KEY, CONSTRAINT fk11 FOREIGN KEY (pk) REFERENCES t11(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t13(pk BIGINT PRIMARY KEY, CONSTRAINT fk12 FOREIGN KEY (pk) REFERENCES t12(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t14(pk BIGINT PRIMARY KEY, CONSTRAINT fk13 FOREIGN KEY (pk) REFERENCES t13(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t15(pk BIGINT PRIMARY KEY, CONSTRAINT fk14 FOREIGN KEY (pk) REFERENCES t14(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t16(pk BIGINT PRIMARY KEY, CONSTRAINT fk15 FOREIGN KEY (pk) REFERENCES t15(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"INSERT INTO t1 VALUES (1);",
			"INSERT INTO t2 VALUES (1);",
			"INSERT INTO t3 VALUES (1);",
			"INSERT INTO t4 VALUES (1);",
			"INSERT INTO t5 VALUES (1);",
			"INSERT INTO t6 VALUES (1);",
			"INSERT INTO t7 VALUES (1);",
			"INSERT INTO t8 VALUES (1);",
			"INSERT INTO t9 VALUES (1);",
			"INSERT INTO t10 VALUES (1);",
			"INSERT INTO t11 VALUES (1);",
			"INSERT INTO t12 VALUES (1);",
			"INSERT INTO t13 VALUES (1);",
			"INSERT INTO t14 VALUES (1);",
			"INSERT INTO t15 VALUES (1);",
			"INSERT INTO t16 VALUES (1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "DELETE FROM t1;",
				ExpectedErr: sql.ErrForeignKeyDepthLimit,
			},
			{
				Query:    "DELETE FROM t16;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "DELETE FROM t1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "Acyclic update cascade depth limit",
		SetUpScript: []string{
			"CREATE TABLE t1(pk BIGINT PRIMARY KEY);",
			"CREATE TABLE t2(pk BIGINT PRIMARY KEY, CONSTRAINT fk1 FOREIGN KEY (pk) REFERENCES t1(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t3(pk BIGINT PRIMARY KEY, CONSTRAINT fk2 FOREIGN KEY (pk) REFERENCES t2(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t4(pk BIGINT PRIMARY KEY, CONSTRAINT fk3 FOREIGN KEY (pk) REFERENCES t3(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t5(pk BIGINT PRIMARY KEY, CONSTRAINT fk4 FOREIGN KEY (pk) REFERENCES t4(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t6(pk BIGINT PRIMARY KEY, CONSTRAINT fk5 FOREIGN KEY (pk) REFERENCES t5(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t7(pk BIGINT PRIMARY KEY, CONSTRAINT fk6 FOREIGN KEY (pk) REFERENCES t6(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t8(pk BIGINT PRIMARY KEY, CONSTRAINT fk7 FOREIGN KEY (pk) REFERENCES t7(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t9(pk BIGINT PRIMARY KEY, CONSTRAINT fk8 FOREIGN KEY (pk) REFERENCES t8(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t10(pk BIGINT PRIMARY KEY, CONSTRAINT fk9 FOREIGN KEY (pk) REFERENCES t9(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t11(pk BIGINT PRIMARY KEY, CONSTRAINT fk10 FOREIGN KEY (pk) REFERENCES t10(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t12(pk BIGINT PRIMARY KEY, CONSTRAINT fk11 FOREIGN KEY (pk) REFERENCES t11(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t13(pk BIGINT PRIMARY KEY, CONSTRAINT fk12 FOREIGN KEY (pk) REFERENCES t12(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t14(pk BIGINT PRIMARY KEY, CONSTRAINT fk13 FOREIGN KEY (pk) REFERENCES t13(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t15(pk BIGINT PRIMARY KEY, CONSTRAINT fk14 FOREIGN KEY (pk) REFERENCES t14(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"CREATE TABLE t16(pk BIGINT PRIMARY KEY, CONSTRAINT fk15 FOREIGN KEY (pk) REFERENCES t15(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"INSERT INTO t1 VALUES (1);",
			"INSERT INTO t2 VALUES (1);",
			"INSERT INTO t3 VALUES (1);",
			"INSERT INTO t4 VALUES (1);",
			"INSERT INTO t5 VALUES (1);",
			"INSERT INTO t6 VALUES (1);",
			"INSERT INTO t7 VALUES (1);",
			"INSERT INTO t8 VALUES (1);",
			"INSERT INTO t9 VALUES (1);",
			"INSERT INTO t10 VALUES (1);",
			"INSERT INTO t11 VALUES (1);",
			"INSERT INTO t12 VALUES (1);",
			"INSERT INTO t13 VALUES (1);",
			"INSERT INTO t14 VALUES (1);",
			"INSERT INTO t15 VALUES (1);",
			"INSERT INTO t16 VALUES (1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "UPDATE t1 SET pk = 2;",
				ExpectedErr: sql.ErrForeignKeyDepthLimit,
			},
			{
				Query:    "DELETE FROM t16;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query: "UPDATE t1 SET pk = 2;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{
					RowsAffected: 1,
					InsertID:     0,
					Info: plan.UpdateInfo{
						Matched:  1,
						Updated:  1,
						Warnings: 0,
					},
				}}},
			},
		},
	},
	{
		Name: "VARCHAR child violation detection",
		SetUpScript: []string{
			"CREATE TABLE colors (id INT NOT NULL, color VARCHAR(32) NOT NULL, PRIMARY KEY (id), INDEX color_index(color));",
			"CREATE TABLE objects (id INT NOT NULL, name VARCHAR(64) NOT NULL, color VARCHAR(32), PRIMARY KEY(id), CONSTRAINT color_fk FOREIGN KEY (color) REFERENCES colors(color));",
			"INSERT INTO colors (id, color) VALUES (1, 'red'), (2, 'green'), (3, 'blue'), (4, 'purple');",
			"INSERT INTO objects (id, name, color) VALUES (1, 'truck', 'red'), (2, 'ball', 'green'), (3, 'shoe', 'blue');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "DELETE FROM colors where color='green';",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "SELECT * FROM colors;",
				Expected: []sql.UntypedSqlRow{{1, "red"}, {2, "green"}, {3, "blue"}, {4, "purple"}},
			},
		},
	},
	{
		Name: "INSERT IGNORE INTO works correctly with foreign key violations",
		SetUpScript: []string{
			"CREATE TABLE colors (id INT NOT NULL, color VARCHAR(32) NOT NULL, PRIMARY KEY (id), INDEX color_index(color));",
			"CREATE TABLE objects (id INT NOT NULL, name VARCHAR(64) NOT NULL, color VARCHAR(32), PRIMARY KEY(id), CONSTRAINT color_fk FOREIGN KEY (color) REFERENCES colors(color));",
			"INSERT INTO colors (id, color) VALUES (1, 'red'), (2, 'green'), (3, 'blue'), (4, 'purple');",
			"INSERT INTO objects (id, name, color) VALUES (1, 'truck', 'red'), (2, 'ball', 'green'), (3, 'shoe', 'blue');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT IGNORE INTO objects (id, name, color) VALUES (5, 'hi', 'yellow');",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT * FROM objects;",
				Expected: []sql.UntypedSqlRow{{1, "truck", "red"}, {2, "ball", "green"}, {3, "shoe", "blue"}},
			},
		},
	},
	{
		Name: "Delayed foreign key resolution: update",
		SetUpScript: []string{
			"set foreign_key_checks=0;",
			"create table delayed_parent(pk int primary key);",
			"create table delayed_child(pk int primary key, foreign key(pk) references delayed_parent(pk));",
			"insert into delayed_parent values (10), (20);",
			"insert into delayed_child values (1), (20);",
			"set foreign_key_checks=1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				// No-op update bad to bad should not cause constraint violation
				Skip:  true,
				Query: "update delayed_child set pk=1 where pk=1;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 1, Updated: 0}}},
				},
			},
			{
				// Update on non-existent row should not cause constraint violation
				Query: "update delayed_child set pk=3 where pk=3;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 0, Updated: 0}}},
				},
			},
			{
				// No-op update good to good should not cause constraint violation
				Query: "update delayed_child set pk=20 where pk=20;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 1, Updated: 0}}},
				},
			},
			{
				// Updating bad value to good value still fails
				Query: "update delayed_child set pk=10 where pk=1;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
		},
	},
	{
		Name: "Delayed foreign key resolution: delete",
		SetUpScript: []string{
			"set foreign_key_checks=0;",
			"create table delayed_parent(pk int primary key);",
			"create table delayed_child(pk int primary key, foreign key(pk) references delayed_parent(pk));",
			"insert into delayed_parent values (10), (20);",
			"insert into delayed_child values (1), (20);",
			"set foreign_key_checks=1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				// No-op update good to good should not cause constraint violation
				Query: "delete from delayed_child where false;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 0}},
				},
			},
			{
				Query: "delete from delayed_child where pk = 20;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "delete from delayed_child where pk = 1;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1}},
				},
			},
		},
	},
	{
		Name: "Delayed foreign key resolution insert",
		SetUpScript: []string{
			"SET FOREIGN_KEY_CHECKS=0;",
			"CREATE TABLE delayed_child (pk INT PRIMARY KEY, v1 INT, CONSTRAINT fk_delayed FOREIGN KEY (v1) REFERENCES delayed_parent(v1));",
			"CREATE TABLE delayed_parent (pk INT PRIMARY KEY, v1 INT, INDEX (v1));",
			"INSERT INTO delayed_child VALUES (1, 2);",
			"SET FOREIGN_KEY_CHECKS=1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW CREATE TABLE delayed_child;",
				Expected: []sql.UntypedSqlRow{{"delayed_child", "CREATE TABLE `delayed_child` (\n  `pk` int NOT NULL,\n  `v1` int,\n  PRIMARY KEY (`pk`),\n  KEY `fk_delayed` (`v1`),\n  CONSTRAINT `fk_delayed` FOREIGN KEY (`v1`) REFERENCES `delayed_parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "SELECT * FROM delayed_parent;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT * FROM delayed_child;",
				Expected: []sql.UntypedSqlRow{{1, 2}},
			},
			{
				Query:       "INSERT INTO delayed_child VALUES (2, 3);",
				ExpectedErr: sql.ErrForeignKeyNotResolved,
			},
			{
				Query:    "INSERT INTO delayed_parent VALUES (1, 2), (2, 3);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query:    "INSERT INTO delayed_child VALUES (2, 3);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM delayed_child;",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}},
			},
		},
	},
	{
		Name: "Delayed foreign key still does some validation",
		SetUpScript: []string{
			"SET FOREIGN_KEY_CHECKS=0;",
			"CREATE TABLE valid_delayed_child (i INT, CONSTRAINT valid_fk FOREIGN KEY (i) REFERENCES delayed_parent(i))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE delayed_child1(i int, CONSTRAINT fk_delayed1 FOREIGN KEY (badcolumn) REFERENCES delayed_parent(i));",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query:       "CREATE TABLE delayed_child2(i int, CONSTRAINT fk_delayed2 FOREIGN KEY (i) REFERENCES delayed_parent(c1, c2, c3));",
				ExpectedErr: sql.ErrForeignKeyColumnCountMismatch,
			},
			{
				Query:       "CREATE TABLE delayed_child3(i int, j int, CONSTRAINT fk_i FOREIGN KEY (i) REFERENCES delayed_parent(i), CONSTRAINT fk_i FOREIGN KEY (j) REFERENCES delayed_parent(j));",
				ExpectedErr: sql.ErrForeignKeyDuplicateName,
			},
			{
				Query:       "CREATE TABLE delayed_child4(i int, CONSTRAINT fk_delayed4 FOREIGN KEY (i,i,i) REFERENCES delayed_parent(c1, c2, c3));",
				ExpectedErr: sql.ErrAddForeignKeyDuplicateColumn,
			},
			{
				Query:       "ALTER TABLE valid_delayed_child drop index valid_fk",
				ExpectedErr: sql.ErrForeignKeyDropIndex,
			},
		},
	},
	{
		Name: "Delayed foreign key resolution resetting FOREIGN_KEY_CHECKS",
		SetUpScript: []string{
			"SET FOREIGN_KEY_CHECKS=0;",
			"CREATE TABLE delayed_child (pk INT PRIMARY KEY, v1 INT, CONSTRAINT fk_delayed FOREIGN KEY (v1) REFERENCES delayed_parent(v1));",
			"INSERT INTO delayed_child VALUES (1, 2);",
			"SET FOREIGN_KEY_CHECKS=1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW CREATE TABLE delayed_child;",
				Expected: []sql.UntypedSqlRow{{"delayed_child", "CREATE TABLE `delayed_child` (\n  `pk` int NOT NULL,\n  `v1` int,\n  PRIMARY KEY (`pk`),\n  KEY `fk_delayed` (`v1`),\n  CONSTRAINT `fk_delayed` FOREIGN KEY (`v1`) REFERENCES `delayed_parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "SELECT * FROM delayed_child;",
				Expected: []sql.UntypedSqlRow{{1, 2}},
			},
			{
				Query:       "INSERT INTO delayed_child VALUES (2, 3);",
				ExpectedErr: sql.ErrForeignKeyNotResolved,
			},
			{
				Query:    "CREATE TABLE delayed_parent (pk INT PRIMARY KEY, v1 INT, INDEX (v1));",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO delayed_parent VALUES (1, 2), (2, 3);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query:    "INSERT INTO delayed_child VALUES (2, 3);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM delayed_child;",
				Expected: []sql.UntypedSqlRow{{1, 2}, {2, 3}},
			},
		},
	},
	{
		Name: "DROP TABLE with FOREIGN_KEY_CHECKS=0",
		SetUpScript: []string{
			"ALTER TABLE child ADD CONSTRAINT fk_dropped FOREIGN KEY (v1) REFERENCES parent(v1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "TRUNCATE parent;",
				ExpectedErr: sql.ErrTruncateReferencedFromForeignKey,
			},
			{
				Query:       "DROP TABLE parent;",
				ExpectedErr: sql.ErrForeignKeyDropTable,
			},
			{
				Query:    "SET FOREIGN_KEY_CHECKS=0;",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "TRUNCATE parent;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "DROP TABLE parent;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "SET FOREIGN_KEY_CHECKS=1;",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:       "INSERT INTO child VALUES (4, 5, 6);",
				ExpectedErr: sql.ErrForeignKeyNotResolved,
			},
			{
				Query:    "CREATE TABLE parent (pk INT PRIMARY KEY, v1 INT, INDEX (v1));",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO parent VALUES (1, 5);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "INSERT INTO child VALUES (4, 5, 6);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.UntypedSqlRow{{1, 5}},
			},
			{
				Query:    "SELECT * FROM child;",
				Expected: []sql.UntypedSqlRow{{4, 5, 6}},
			},
		},
	},
	{
		Name: "ALTER TABLE ADD CONSTRAINT for different database",
		SetUpScript: []string{
			"CREATE DATABASE public;",
			"CREATE TABLE public.cities (pk INT PRIMARY KEY, city VARCHAR(255), state VARCHAR(2));",
			"CREATE TABLE public.states (state_id INT PRIMARY KEY, state VARCHAR(2));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE public.cities ADD CONSTRAINT foreign_key1 FOREIGN KEY (state) REFERENCES public.states(state);",
				ExpectedErr: sql.ErrForeignKeyMissingReferenceIndex,
			},
			{
				Query:    "CREATE INDEX foreign_key1 ON public.states(state);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE public.cities ADD CONSTRAINT foreign_key1 FOREIGN KEY (state) REFERENCES public.states(state);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Creating a foreign key on a table with an unsupported type works",
		SetUpScript: []string{
			"CREATE TABLE IF NOT EXISTS restaurants (id INT PRIMARY KEY, coordinate POINT);",
			"CREATE TABLE IF NOT EXISTS hours (restaurant_id INT PRIMARY KEY AUTO_INCREMENT, CONSTRAINT fk_name FOREIGN KEY (restaurant_id) REFERENCES restaurants(id));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW CREATE TABLE hours;",
				Expected: []sql.UntypedSqlRow{{"hours", "CREATE TABLE `hours` (\n  `restaurant_id` int NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`restaurant_id`),\n  CONSTRAINT `fk_name` FOREIGN KEY (`restaurant_id`) REFERENCES `restaurants` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "Create foreign key onto primary key",
		SetUpScript: []string{
			"DROP TABLE child;",
			"DROP TABLE parent;",
			"CREATE TABLE parent (a INT, b INT, c INT, PRIMARY KEY (b, a));",
			"CREATE TABLE child (a INT PRIMARY KEY, b INT);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE child ADD CONSTRAINT fk1 FOREIGN KEY (b) REFERENCES parent (b);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child ADD CONSTRAINT fk2 FOREIGN KEY (a) REFERENCES parent (b);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk3 FOREIGN KEY (a, b) REFERENCES parent (a, b);",
				ExpectedErr: sql.ErrForeignKeyMissingReferenceIndex,
			},
			{
				Query:    "ALTER TABLE child ADD CONSTRAINT fk4 FOREIGN KEY (b, a) REFERENCES parent (b, a);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Reordered foreign key columns do match",
		SetUpScript: []string{
			"DROP TABLE child;",
			"DROP TABLE parent;",
			"CREATE TABLE parent(fk1 int, fk2 int, primary key(fk1, fk2));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE TABLE child(id int unique, fk1 int, fk2 int, primary key(fk2, fk1, id), constraint `fk` foreign key(fk1, fk2) references parent (fk1, fk2));",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "Show create table child;",
				Expected: []sql.UntypedSqlRow{
					{"child", "CREATE TABLE `child` (\n" +
						"  `id` int NOT NULL,\n" +
						"  `fk1` int NOT NULL,\n" +
						"  `fk2` int NOT NULL,\n" +
						"  PRIMARY KEY (`fk2`,`fk1`,`id`),\n" +
						"  KEY `fk` (`fk1`,`fk2`),\n" +
						"  UNIQUE KEY `id` (`id`),\n" +
						"  CONSTRAINT `fk` FOREIGN KEY (`fk1`,`fk2`) REFERENCES `parent` (`fk1`,`fk2`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name: "Reordered foreign key columns do not match",
		SetUpScript: []string{
			"DROP TABLE child;",
			"DROP TABLE parent;",
			"CREATE TABLE parent(pk DOUBLE PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX(v1, v2, pk));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE child(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, CONSTRAINT fk_child FOREIGN KEY (v2, v1) REFERENCES parent(v2, v1));",
				ExpectedErr: sql.ErrForeignKeyMissingReferenceIndex,
			},
		},
	},
	{
		Name: "Reordered foreign key columns match an index's prefix, ALTER TABLE ADD FOREIGN KEY fails check",
		SetUpScript: []string{
			"DROP TABLE child;",
			"DROP TABLE parent;",
			"CREATE TABLE parent(pk DOUBLE PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX(v1, v2, pk));",
			"INSERT INTO parent VALUES (1, 1, 1), (2, 1, 2);",
			"CREATE TABLE child(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT);",
			"INSERT INTO child VALUES (1, 2, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child ADD CONSTRAINT fk_child FOREIGN KEY (v2, v1) REFERENCES parent(v2, v1);",
				ExpectedErr: sql.ErrForeignKeyMissingReferenceIndex,
			},
		},
	},
	{
		Name: "Self-referential deletion with ON UPDATE CASCADE",
		SetUpScript: []string{
			"CREATE TABLE self(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX(v1), CONSTRAINT fk_self FOREIGN KEY(v2) REFERENCES self(v1) ON UPDATE CASCADE);",
			"INSERT INTO self VALUES (0, 1, 1), (1, 2, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "DELETE FROM self WHERE v1 = 1;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "DELETE FROM self WHERE v1 = 2;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "Self-referential deletion with ON DELETE CASCADE",
		SetUpScript: []string{
			"CREATE TABLE self(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX(v1), CONSTRAINT fk_self FOREIGN KEY(v2) REFERENCES self(v1) ON DELETE CASCADE);",
			"INSERT INTO self VALUES (0, 1, 1), (1, 2, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DELETE FROM self WHERE v1 = 1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}}, // Cascading deletions do not count
			},
			{
				Query:    "SELECT * FROM self;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "Self-referential foreign key is not case sensitive",
		SetUpScript: []string{
			"create table t1 (i int primary key, J int, constraint fk1 foreign key (J) references t1(i));",
			"create table t2 (I int primary key, j int, constraint fk2 foreign key (j) references t2(I));",
			"create table t3 (i int primary key, j int, constraint fk3 foreign key (J) references t3(I));",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Casing is preserved in show create table statements
				Query: "show create table t1;",
				Expected: []sql.UntypedSqlRow{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `i` int NOT NULL,\n  `J` int,\n" +
						"  PRIMARY KEY (`i`),\n" +
						"  KEY `fk1` (`J`),\n" +
						"  CONSTRAINT `fk1` FOREIGN KEY (`J`) REFERENCES `t1` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into t1 values (1, 1);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:       "insert into t1 values (2, 3);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				// Casing is preserved in show create table statements
				Query: "show create table t2;",
				Expected: []sql.UntypedSqlRow{
					{"t2", "CREATE TABLE `t2` (\n" +
						"  `I` int NOT NULL,\n" +
						"  `j` int,\n" +
						"  PRIMARY KEY (`I`),\n" +
						"  KEY `fk2` (`j`),\n" +
						"  CONSTRAINT `fk2` FOREIGN KEY (`j`) REFERENCES `t2` (`I`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into t2 values (1, 1);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:       "insert into t2 values (2, 3);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "show create table t3;",
				Expected: []sql.UntypedSqlRow{
					{"t3", "CREATE TABLE `t3` (\n" +
						"  `i` int NOT NULL,\n  `j` int,\n" +
						"  PRIMARY KEY (`i`),\n" +
						"  KEY `fk3` (`j`),\n" +
						"  CONSTRAINT `fk3` FOREIGN KEY (`j`) REFERENCES `t3` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into t3 values (1, 1);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:       "insert into t3 values (2, 3);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
		},
	},
	{
		Name: "Cascaded DELETE becomes cascading UPDATE after first child, using ON DELETE for second child",
		SetUpScript: []string{
			"DROP TABLE child;",
			"DROP TABLE parent;",
			"CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX (v1), INDEX (v2), INDEX (v1, v2));",
			"CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, CONSTRAINT fk_child FOREIGN KEY (v1, v2) REFERENCES parent (v1, v2) ON DELETE SET NULL);",
			"CREATE TABLE child2 (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, CONSTRAINT fk_child2 FOREIGN KEY (v1, v2) REFERENCES child (v1, v2) ON DELETE SET NULL);",
			"INSERT INTO parent VALUES (1,1,1), (2,2,2), (3,3,3);",
			"INSERT INTO child VALUES (1,1,1), (2,2,2), (3,3,3);",
			"INSERT INTO child2 VALUES (1,1,1), (2,2,2), (3,3,3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "DELETE FROM parent WHERE pk = 1;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
		},
	},
	{
		Name: "Cascaded DELETE becomes cascading UPDATE after first child, using ON UPDATE for second child",
		SetUpScript: []string{
			"DROP TABLE child;",
			"DROP TABLE parent;",
			"CREATE TABLE parent (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX (v1), INDEX (v2), INDEX (v1, v2));",
			"CREATE TABLE child (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, CONSTRAINT fk_child FOREIGN KEY (v1, v2) REFERENCES parent (v1, v2) ON DELETE SET NULL);",
			"CREATE TABLE child2 (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, CONSTRAINT fk_child2 FOREIGN KEY (v1, v2) REFERENCES child (v1, v2) ON UPDATE CASCADE);",
			"INSERT INTO parent VALUES (1,1,1), (2,2,2), (3,3,3);",
			"INSERT INTO child VALUES (1,1,1), (2,2,2), (3,3,3);",
			"INSERT INTO child2 VALUES (1,1,1), (2,2,2), (3,3,3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DELETE FROM parent WHERE pk = 1;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.UntypedSqlRow{{2, 2, 2}, {3, 3, 3}},
			},
			{
				Query:    "SELECT * FROM child;",
				Expected: []sql.UntypedSqlRow{{1, nil, nil}, {2, 2, 2}, {3, 3, 3}},
			},
			{
				Query:    "SELECT * FROM child2;",
				Expected: []sql.UntypedSqlRow{{1, nil, nil}, {2, 2, 2}, {3, 3, 3}},
			},
		},
	},
	{
		Name: "INSERT on DUPLICATE correctly works with FKs",
		SetUpScript: []string{
			"INSERT INTO parent values (1,1,1),(2,2,2),(3,3,3)",
			"ALTER TABLE child ADD CONSTRAINT fk_named FOREIGN KEY (v1) REFERENCES parent(v1);",
			"INSERT into child values (1, 1, 1)",
			"CREATE TABLE one (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX v1 (v1));",
			"CREATE TABLE two (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX v1v2 (v1, v2), CONSTRAINT fk_name_1 FOREIGN KEY (v1) REFERENCES one(v1) ON DELETE CASCADE ON UPDATE CASCADE);",
			"INSERT INTO one VALUES (1, 1, 4), (2, 2, 5), (3, 3, 6), (4, 4, 5);",
			"INSERT INTO two VALUES (2, 1, 1), (3, 2, 2), (4, 3, 3), (5, 4, 4);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "INSERT INTO parent VALUES (1,200,1) ON DUPLICATE KEY UPDATE v1 = values(v1)",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "INSERT INTO one VALUES (1, 2, 4) on duplicate key update v1 = VALUES(v1)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query:    "SELECT * FROM two where pk = 2",
				Expected: []sql.UntypedSqlRow{{2, 2, 1}},
			},
		},
	},
	{
		Name: "Referencing Primary Key",
		SetUpScript: []string{
			"CREATE table parent1 (pk BIGINT PRIMARY KEY, v1 BIGINT);",
			"CREATE table child1 (pk BIGINT PRIMARY KEY, v1 BIGINT, FOREIGN KEY (v1) REFERENCES parent1(pk) ON UPDATE CASCADE ON DELETE CASCADE);",
			"INSERT INTO parent1 VALUES (1, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO child1 VALUES (1, 1);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM child1;",
				Expected: []sql.UntypedSqlRow{{1, 1}},
			},
			{
				Query:    "UPDATE parent1 SET pk = 2;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "SELECT * FROM child1;",
				Expected: []sql.UntypedSqlRow{{1, 2}},
			},
		},
	},
	{
		Name: "Referencing Composite Primary Key",
		SetUpScript: []string{
			"CREATE table parent1 (pk1 BIGINT, pk2 BIGINT, v1 BIGINT, PRIMARY KEY(pk1, pk2));",
			"CREATE table child1 (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, FOREIGN KEY (v1, v2) REFERENCES parent1(pk1, pk2) ON UPDATE CASCADE ON DELETE CASCADE);",
			"INSERT INTO parent1 VALUES (1, 2, 3), (4, 5, 6);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO child1 VALUES (1, 1, 2), (2, 4, 5);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(2)}},
			},
			{
				Query:    "SELECT * FROM child1;",
				Expected: []sql.UntypedSqlRow{{1, 1, 2}, {2, 4, 5}},
			},
			{
				Query:    "UPDATE parent1 SET pk2 = pk1 + pk2;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 2, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}}},
			},
			{
				Query:    "SELECT * FROM child1;",
				Expected: []sql.UntypedSqlRow{{1, 1, 3}, {2, 4, 9}},
			},
		},
	},
	{
		Name: "Keyless CASCADE deleting all rows",
		SetUpScript: []string{
			"CREATE TABLE one (v0 BIGINT, v1 BIGINT, INDEX one_v0 (v0), INDEX one_v1 (v1));",
			"CREATE TABLE two (v1 BIGINT, CONSTRAINT fk_name_1 FOREIGN KEY (v1) REFERENCES one(v1) ON DELETE CASCADE ON UPDATE CASCADE);",
			"INSERT INTO one VALUES (1, 2);",
			"INSERT INTO two VALUES (2);",
			"UPDATE one SET v1 = v0 + v1;",
			"DELETE FROM one WHERE v0 = 1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM one;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT * FROM two;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "Keyless CASCADE over three tables",
		SetUpScript: []string{
			"CREATE TABLE one (v0 BIGINT, v1 BIGINT, v2 BIGINT, INDEX idx (v0));",
			"ALTER TABLE one ADD INDEX v1 (v1);",
			"CREATE TABLE two (v0 BIGINT, v1 BIGINT, v2 BIGINT, INDEX idx (v0), CONSTRAINT fk_name_1 FOREIGN KEY (v1) REFERENCES one(v1) ON DELETE CASCADE ON UPDATE CASCADE);",
			"ALTER TABLE two ADD INDEX v1v2 (v1, v2);",
			"CREATE TABLE three (v0 BIGINT, v1 BIGINT, v2 BIGINT, INDEX idx (v0), CONSTRAINT fk_name_2 FOREIGN KEY (v1, v2) REFERENCES two(v1, v2) ON DELETE CASCADE ON UPDATE CASCADE);",
			"INSERT INTO one VALUES (1, 1, 4), (2, 2, 5), (3, 3, 6), (4, 4, 5);",
			"INSERT INTO two VALUES (2, 1, 1), (3, 2, 2), (4, 3, 3), (5, 4, 4);",
			"INSERT INTO three VALUES (3, 1, 1), (4, 2, 2), (5, 3, 3), (6, 4, 4);",
			"UPDATE one SET v1 = v1 + v2;",
			"DELETE FROM one WHERE v0 = 3;",
			"UPDATE two SET v2 = v1 - 2;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM one;",
				Expected: []sql.UntypedSqlRow{{1, 5, 4}, {2, 7, 5}, {4, 9, 5}},
			},
			{
				Query:    "SELECT * FROM two;",
				Expected: []sql.UntypedSqlRow{{2, 5, 3}, {3, 7, 5}},
			},
			{
				Query:    "SELECT * FROM three;",
				Expected: []sql.UntypedSqlRow{{3, 5, 3}, {4, 7, 5}},
			},
		},
	},
	{
		Name: "Table with inverted primary key referencing another table can insert rows",
		SetUpScript: []string{
			"create table a (x int, y int, primary key (x,y), INDEX `a_y_idx` (y));",
			"create table b (x int, y int, primary key (y,x), foreign key (y) references a(y) on update cascade on delete cascade);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT into a (x, y) VALUES (1, 3);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "INSERT into b (x, y) VALUES (2, 3);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT x, y from a;",
				Expected: []sql.UntypedSqlRow{{1, 3}},
			},
			{
				Query:    "SELECT x, y  from b;",
				Expected: []sql.UntypedSqlRow{{2, 3}},
			},
			{
				Query:       "INSERT into b (x, y) VALUES (3, 5);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
		},
	},
	{
		Name: "Table with inverted primary key referencing another table with inverted primary keys can be inserted",
		SetUpScript: []string{
			"create table a (x int, y int, primary key (y,x));",
			"create table b (x int, y int, primary key (y,x), foreign key (y) references a(y) on update cascade on delete cascade);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT into a (x, y) VALUES (1, 3);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "INSERT into b (x, y) VALUES (2, 3);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT x, y from a;",
				Expected: []sql.UntypedSqlRow{{1, 3}},
			},
			{
				Query:    "SELECT x, y from b;",
				Expected: []sql.UntypedSqlRow{{2, 3}},
			},
			{
				Query:       "INSERT into b (x, y) VALUES (3, 5);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
		},
	},
	{
		Name: "Table with inverted primary key referencing another table can be updated",
		SetUpScript: []string{
			"create table a (x int, y int, primary key (x,y), INDEX `a_y_idx` (y));",
			"create table b (x int, y int, primary key (y,x), foreign key (y) references a(y) on update cascade on delete cascade);",
			"INSERT into a VALUES (1, 3);",
			"INSERT into b VALUES (2, 3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "UPDATE a SET y = 4 where y = 3;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "SELECT x, y from a;",
				Expected: []sql.UntypedSqlRow{{1, 4}},
			},
			{
				Query:    "SELECT x, y from b;",
				Expected: []sql.UntypedSqlRow{{2, 4}},
			},
		},
	},
	{
		Name: "Table with inverted primary key referencing another table with inverted primary keys can be updated",
		SetUpScript: []string{
			"create table a (x int, y int, primary key (y,x));",
			"create table b (x int, y int, primary key (y,x), foreign key (y) references a(y) on update cascade on delete cascade);",
			"INSERT into a VALUES (1, 3)",
			"INSERT into b VALUES (2, 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "UPDATE a SET y = 4 where y = 3;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "SELECT x, y from a;",
				Expected: []sql.UntypedSqlRow{{1, 4}},
			},
			{
				Query:    "SELECT x, y from b;",
				Expected: []sql.UntypedSqlRow{{2, 4}},
			},
		},
	},
	{
		Name: "Table with inverted primary key referencing another table can be deleted",
		SetUpScript: []string{
			"create table a (x int, y int, primary key (x,y), INDEX `a_y_idx` (y));",
			"create table b (x int, y int, primary key (y,x), foreign key (y) references a(y) on update cascade on delete cascade);",
			"INSERT into a VALUES (1, 3);",
			"INSERT into b VALUES (2, 3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DELETE from a where x = 1 AND y = 3;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * from a;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT * from b;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "Table with inverted primary key referencing another table with inverted primary keys can be deleted",
		SetUpScript: []string{
			"create table a (x int, y int, primary key (y,x));",
			"create table b (x int, y int, primary key (y,x), foreign key (y) references a(y) on update cascade on delete cascade);",
			"INSERT into a VALUES (1, 3)",
			"INSERT into b VALUES (2, 3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DELETE from a where x = 1 AND y = 3;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * from a;",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT * from b;",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "May use different collations as long as the character sets are equivalent",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk char(32) COLLATE utf8mb4_0900_ai_ci PRIMARY KEY);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE TABLE t2 (pk char(32) COLLATE utf8mb4_0900_bin PRIMARY KEY, CONSTRAINT fk_1 FOREIGN KEY (pk) REFERENCES t1 (pk));",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Referenced index includes implicit primary key columns",
		SetUpScript: []string{
			"create table parent1 (fk1 int, pk1 int, pk2 int, pk3 int, primary key(pk1, pk2, pk3), index (fk1, pk2));",
			"insert into parent1 values (0, 1, 2, 3);",
			"create table child1 (fk1 int, pk1 int, pk2 int, pk3 int, primary key (pk1, pk2, pk3));",
			"create table child2 (fk1 int, pk1 int, pk2 int, pk3 int, primary key (pk1, pk2, pk3));",
			"create table child3 (fk1 int, pk1 int, pk2 int, pk3 int, primary key (pk1, pk2, pk3));",
			"create table child4 (fk1 int, pk1 int, pk2 int, pk3 int, primary key (pk1, pk2, pk3));",
			"create index idx4 on child4 (fk1, pk2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table child1 add foreign key (fk1, pk1) references parent1 (fk1, pk1);",
				ExpectedErr: sql.ErrForeignKeyMissingReferenceIndex,
			},
			{
				Query:       "alter table child1 add foreign key (fk1, pk1, pk2) references parent1 (fk1, pk1, pk2);",
				ExpectedErr: sql.ErrForeignKeyMissingReferenceIndex,
			},
			{
				Query:       "alter table child1 add foreign key (fk1, pk2, pk3, pk1) references parent1 (fk1, pk2, pk3, pk1);",
				ExpectedErr: sql.ErrForeignKeyMissingReferenceIndex,
			},
			{
				Query: "alter table child1 add constraint fk1 foreign key (fk1, pk2) references parent1 (fk1, pk2);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table child1",
				Expected: []sql.UntypedSqlRow{
					{"child1", "CREATE TABLE `child1` (\n" +
						"  `fk1` int,\n" +
						"  `pk1` int NOT NULL,\n" +
						"  `pk2` int NOT NULL,\n" +
						"  `pk3` int NOT NULL,\n" +
						"  PRIMARY KEY (`pk1`,`pk2`,`pk3`),\n" +
						"  KEY `fk1` (`fk1`,`pk2`),\n" +
						"  CONSTRAINT `fk1` FOREIGN KEY (`fk1`,`pk2`) REFERENCES `parent1` (`fk1`,`pk2`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into child1 values (0, 1, 2, 3);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "insert into child1 values (0, 99, 2, 99);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:       "insert into child1 values (0, 99, 99, 99);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "alter table child2 add constraint fk2 foreign key (fk1, pk2, pk1) references parent1 (fk1, pk2, pk1);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table child2",
				Expected: []sql.UntypedSqlRow{
					{"child2", "CREATE TABLE `child2` (\n" +
						"  `fk1` int,\n" +
						"  `pk1` int NOT NULL,\n" +
						"  `pk2` int NOT NULL,\n" +
						"  `pk3` int NOT NULL,\n" +
						"  PRIMARY KEY (`pk1`,`pk2`,`pk3`),\n" +
						"  KEY `fk2` (`fk1`,`pk2`,`pk1`),\n" +
						"  CONSTRAINT `fk2` FOREIGN KEY (`fk1`,`pk2`,`pk1`) REFERENCES `parent1` (`fk1`,`pk2`,`pk1`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "insert into child2 values (0, 1, 2, 3);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "insert into child2 values (0, 1, 2, 99);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query:       "insert into child2 values (0, 99, 2, 99);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query: "alter table child3 add constraint fk3 foreign key (fk1, pk2, pk1, pk3) references parent1 (fk1, pk2, pk1, pk3);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "insert into child3 values (0, 1, 2, 3);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "show create table child3",
				Expected: []sql.UntypedSqlRow{
					{"child3", "CREATE TABLE `child3` (\n" +
						"  `fk1` int,\n" +
						"  `pk1` int NOT NULL,\n" +
						"  `pk2` int NOT NULL,\n" +
						"  `pk3` int NOT NULL,\n" +
						"  PRIMARY KEY (`pk1`,`pk2`,`pk3`),\n" +
						"  KEY `fk3` (`fk1`,`pk2`,`pk1`,`pk3`),\n" +
						"  CONSTRAINT `fk3` FOREIGN KEY (`fk1`,`pk2`,`pk1`,`pk3`) REFERENCES `parent1` (`fk1`,`pk2`,`pk1`,`pk3`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query:       "insert into child3 values (0, 1, 2, 99);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{ // although idx4 would be a valid index, it is not used for the foreign key fk4
				Query: "alter table child4 add constraint fk4 foreign key (fk1, pk2, pk1, pk3) references parent1 (fk1, pk2, pk1, pk3);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table child4",
				Expected: []sql.UntypedSqlRow{
					{"child4", "CREATE TABLE `child4` (\n" +
						"  `fk1` int,\n" +
						"  `pk1` int NOT NULL,\n" +
						"  `pk2` int NOT NULL,\n" +
						"  `pk3` int NOT NULL,\n" +
						"  PRIMARY KEY (`pk1`,`pk2`,`pk3`),\n" +
						"  KEY `fk4` (`fk1`,`pk2`,`pk1`,`pk3`),\n" +
						"  KEY `idx4` (`fk1`,`pk2`),\n" +
						"  CONSTRAINT `fk4` FOREIGN KEY (`fk1`,`pk2`,`pk1`,`pk3`) REFERENCES `parent1` (`fk1`,`pk2`,`pk1`,`pk3`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{ // idx4 satisfies the foreign key fk5
				Query: "alter table child4 add constraint fk5 foreign key (fk1) references parent1 (fk1);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table child4",
				Expected: []sql.UntypedSqlRow{
					{"child4", "CREATE TABLE `child4` (\n" +
						"  `fk1` int,\n" +
						"  `pk1` int NOT NULL,\n" +
						"  `pk2` int NOT NULL,\n" +
						"  `pk3` int NOT NULL,\n" +
						"  PRIMARY KEY (`pk1`,`pk2`,`pk3`),\n" +
						"  KEY `fk4` (`fk1`,`pk2`,`pk1`,`pk3`),\n" +
						"  KEY `idx4` (`fk1`,`pk2`),\n" +
						"  CONSTRAINT `fk4` FOREIGN KEY (`fk1`,`pk2`,`pk1`,`pk3`) REFERENCES `parent1` (`fk1`,`pk2`,`pk1`,`pk3`),\n" +
						"  CONSTRAINT `fk5` FOREIGN KEY (`fk1`) REFERENCES `parent1` (`fk1`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name: "rename foreign key constraints",
		SetUpScript: []string{
			"create table myparent (i int primary key)",
			"create table mychild (j int primary key)",
			"alter table mychild add constraint `myfk` foreign key (j) references myparent (i)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table mychild;",
				Expected: []sql.UntypedSqlRow{
					{"mychild", "CREATE TABLE `mychild` (\n" +
						"  `j` int NOT NULL,\n" +
						"  PRIMARY KEY (`j`),\n" +
						"  CONSTRAINT `myfk` FOREIGN KEY (`j`) REFERENCES `myparent` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "alter table mychild rename constraint foreign key myfk to newfk;",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table mychild;",
				Expected: []sql.UntypedSqlRow{
					{"mychild", "CREATE TABLE `mychild` (\n" +
						"  `j` int NOT NULL,\n" +
						"  PRIMARY KEY (`j`),\n" +
						"  CONSTRAINT `newfk` FOREIGN KEY (`j`) REFERENCES `myparent` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			// case insensitive rename
			{
				Query: "alter table mychild rename constraint foreign key NeWfK to NewNewFk;",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table mychild;",
				Expected: []sql.UntypedSqlRow{
					{"mychild", "CREATE TABLE `mychild` (\n" +
						"  `j` int NOT NULL,\n" +
						"  PRIMARY KEY (`j`),\n" +
						"  CONSTRAINT `NewNewFk` FOREIGN KEY (`j`) REFERENCES `myparent` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name: "rename check constraints",
		SetUpScript: []string{
			"create table t (i int, constraint `mychk` check (i > 0))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table t rename constraint check mychk to newchk;",
				ExpectedErr: sql.ErrUnsupportedFeature,
			},
			{
				Query:       "alter table t rename constraint mychk to newchk;",
				ExpectedErr: sql.ErrUnsupportedFeature,
			},
		},
	},
	{
		Name: "foreign key naming",
		SetUpScript: []string{
			"create table theparent (i int primary key);",
			"create table child1 (j int primary key);",
			"create table child2 (j int primary key);",
			"create table child3 (j int primary key);",
			"create table child4 (j int primary key);",
			"create table child5 (j int primary key);",
			"create table child6 (j int primary key);",
			"create table child7 (j int primary key);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "alter table child1 add foreign key (j) references theparent (i);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "alter table child1 add foreign key (j) references theparent (i);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "alter table child1 add foreign key (j) references theparent (i);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table child1;",
				Expected: []sql.UntypedSqlRow{
					{"child1", "CREATE TABLE `child1` (\n" +
						"  `j` int NOT NULL,\n" +
						"  PRIMARY KEY (`j`),\n" +
						"  CONSTRAINT `child1_ibfk_1` FOREIGN KEY (`j`) REFERENCES `theparent` (`i`),\n" +
						"  CONSTRAINT `child1_ibfk_2` FOREIGN KEY (`j`) REFERENCES `theparent` (`i`),\n" +
						"  CONSTRAINT `child1_ibfk_3` FOREIGN KEY (`j`) REFERENCES `theparent` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query: "alter table child2 add constraint `child2_ibfk_1` foreign key (j) references theparent (i);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				// If generated name collides with existing, then a new name will be generated
				Query: "alter table child2 add foreign key (j) references theparent (i);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table child2;",
				Expected: []sql.UntypedSqlRow{
					{"child2", "CREATE TABLE `child2` (\n" +
						"  `j` int NOT NULL,\n" +
						"  PRIMARY KEY (`j`),\n" +
						"  CONSTRAINT `child2_ibfk_1` FOREIGN KEY (`j`) REFERENCES `theparent` (`i`),\n" +
						"  CONSTRAINT `child2_ibfk_2` FOREIGN KEY (`j`) REFERENCES `theparent` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				// explicit name collisions still error
				Query:       "alter table child2 add constraint `child2_ibfk_2` foreign key (j) references theparent (i);",
				ExpectedErr: sql.ErrForeignKeyDuplicateName,
			},

			{
				// unlike secondary index naming, constraints will find highest existing index and increment from there
				Query: "alter table child3 add constraint `child3_ibfk_100` foreign key (j) references theparent (i);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "alter table child3 add foreign key (j) references theparent (i);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table child3;",
				Expected: []sql.UntypedSqlRow{
					{"child3", "CREATE TABLE `child3` (\n" +
						"  `j` int NOT NULL,\n" +
						"  PRIMARY KEY (`j`),\n" +
						"  CONSTRAINT `child3_ibfk_100` FOREIGN KEY (`j`) REFERENCES `theparent` (`i`),\n" +
						"  CONSTRAINT `child3_ibfk_101` FOREIGN KEY (`j`) REFERENCES `theparent` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				// Name generation is case-sensitive
				Query: "alter table child4 add constraint `CHILD4_IBFK_1` foreign key (j) references theparent (i);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				// the name collision check is case-insensitive
				Query:       "alter table child4 add foreign key (j) references theparent (i);",
				ExpectedErr: sql.ErrForeignKeyDuplicateName,
			},
			{
				Query: "show create table child4;",
				Expected: []sql.UntypedSqlRow{
					{"child4", "CREATE TABLE `child4` (\n" +
						"  `j` int NOT NULL,\n" +
						"  PRIMARY KEY (`j`),\n" +
						"  CONSTRAINT `CHILD4_IBFK_1` FOREIGN KEY (`j`) REFERENCES `theparent` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query: "alter table child5 add constraint `child5_ibfk_-2` foreign key (j) references theparent (i);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				// This adds -1, which is interpreted as 4294967295
				Query: "alter table child5 add foreign key (j) references theparent (i);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				// This adds 4294967296, which overflows back to 0
				Query: "alter table child5 add foreign key (j) references theparent (i);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				// This attempts to add 4294967296 again, which throws a duplicate name error
				Query:       "alter table child5 add foreign key (j) references theparent (i);",
				ExpectedErr: sql.ErrForeignKeyDuplicateName,
			},
			{
				// foreign keys are sorted by name
				Query: "show create table child5;",
				Expected: []sql.UntypedSqlRow{
					{"child5", "CREATE TABLE `child5` (\n" +
						"  `j` int NOT NULL,\n" +
						"  PRIMARY KEY (`j`),\n" +
						"  CONSTRAINT `child5_ibfk_-2` FOREIGN KEY (`j`) REFERENCES `theparent` (`i`),\n" +
						"  CONSTRAINT `child5_ibfk_0` FOREIGN KEY (`j`) REFERENCES `theparent` (`i`),\n" +
						"  CONSTRAINT `child5_ibfk_4294967295` FOREIGN KEY (`j`) REFERENCES `theparent` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			// empty string constraint names are allowed if specified explicitly
			{
				Query: "alter table child6 add constraint `` foreign key (j) references theparent (i);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "alter table child6 add foreign key (j) references theparent (i);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				Skip:  true, // we need parser changes to tell the difference between an empty string and a NULL
				Query: "show create table child6;",
				Expected: []sql.UntypedSqlRow{
					{"child6", "CREATE TABLE `child6` (\n" +
						"  `j` int NOT NULL,\n" +
						"  PRIMARY KEY (`j`),\n" +
						"  CONSTRAINT `child6_ibfk_1` FOREIGN KEY (`j`) REFERENCES `theparent` (`i`),\n" +
						"  CONSTRAINT `child6_ibfk_2` FOREIGN KEY (`j`) REFERENCES `theparent` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query: "alter table child1 add constraint `child7_ibfk_1` foreign key (j) references theparent (i);",
				Expected: []sql.UntypedSqlRow{
					{types.NewOkResult(0)},
				},
			},
			{
				// foreign key names are kept unique across tables
				Query:       "alter table child7 add foreign key (j) references theparent (i);",
				ExpectedErr: sql.ErrForeignKeyDuplicateName,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/7960
		Name: "Naming automatically created FK indexes",
		SetUpScript: []string{
			`CREATE TABLE child1 (
				id int NOT NULL,
				v1 int DEFAULT NULL,
				v2 int DEFAULT NULL,
				PRIMARY KEY (id),
				KEY fk_name (v1),
				KEY (id, v1)
			);`,
			`CREATE TABLE parent1(
					a_id1 INT,
					a_id2 INT,
					a_id3 INT,
					CONSTRAINT fk_b_a FOREIGN KEY (a_id2, a_id3) REFERENCES child1 (id, v1)
				);`,
			`CREATE TABLE parent2 (
				v1 int NOT NULL,
				v2 int NOT NULL,
				v3 int NOT NULL,
				v4 int NOT NULL,
				v5 int NOT NULL,
				PRIMARY KEY (v1),
				KEY fk1 (v4),
				KEY v2 (v5)
			);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				// When an explicit name is provided for a foreign key, the same name is used for the generated index
				Query: "SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX FROM information_schema.STATISTICS WHERE TABLE_NAME='parent1' ORDER BY INDEX_NAME, SEQ_IN_INDEX;",
				Expected: []sql.UntypedSqlRow{
					{"parent1", "fk_b_a", "a_id2", 1},
					{"parent1", "fk_b_a", "a_id3", 2},
				},
			},
			{
				// When an explicit name is provided for a foreign key, and that name is already used for a key in the
				// table, MySQL throws an error saying that the index name is already in use.
				Query:       "ALTER TABLE parent2 ADD CONSTRAINT `fk1` FOREIGN KEY (v2) REFERENCES child1(v1);",
				ExpectedErr: sql.ErrDuplicateKey,
			},
			{
				// When no name is provided for a foreign key, the index created automaticaly will be named after the
				// first column in the key. If that name is already in use, MySQL appends a number to the name.
				Query:    "ALTER TABLE parent2 ADD FOREIGN KEY (v2) REFERENCES child1(v1);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX FROM information_schema.STATISTICS WHERE TABLE_NAME='parent2' ORDER BY INDEX_NAME, SEQ_IN_INDEX;",
				Expected: []sql.UntypedSqlRow{
					{"parent2", "fk1", "v4", 1},
					{"parent2", "PRIMARY", "v1", 1},
					{"parent2", "v2", "v5", 1},
					{"parent2", "v2_2", "v2", 1},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/7857
		Name: "partial foreign key update",
		SetUpScript: []string{
			"create table parent1 (i int primary key);",
			"create table child1 (" +
				"i int primary key, " +
				"j int, " +
				"k int, " +
				"index(j), " +
				"index(k), " +
				"foreign key (j) references parent1 (i)," +
				"foreign key (k) references parent1 (i));",
			"insert into parent1 values (1);",
			"insert into child1 values (100, 1, 1);",
			"set foreign_key_checks = 0;",
			"insert into child1 values (101, 2, 2);",
			"set foreign_key_checks = 1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "update child1 set j = 1 where i = 101;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				Query: "select * from child1",
				Expected: []sql.UntypedSqlRow{
					{100, 1, 1},
					{101, 1, 2},
				},
			},
		},
	},
	{
		Name: "multiple foreign key refs",
		SetUpScript: []string{
			"create table parent1 (i int primary key);",
			"create table child1 (j int, k int, foreign key (j) references parent1(i) on delete cascade on update cascade, foreign key (k) references parent1 (i) on delete cascade on update cascade);",
			"insert into parent1 values (1), (2), (3);",
			"insert into child1 values (1, 2), (2, 3), (3, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from parent1;",
				Expected: []sql.UntypedSqlRow{
					{1},
					{2},
					{3},
				},
			},
			{
				Query: "select * from child1 order by j, k;",
				Expected: []sql.UntypedSqlRow{
					{1, 2},
					{2, 3},
					{3, 1},
				},
			},
			{
				Query: "update parent1 set i = 20 where i = 2;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				Query: "select * from parent1 order by i;",
				Expected: []sql.UntypedSqlRow{
					{1},
					{3},
					{20},
				},
			},
			{
				Query: "select * from child1 order by j, k;",
				Expected: []sql.UntypedSqlRow{
					{1, 20},
					{3, 1},
					{20, 3},
				},
			},
			{
				Query: "delete from parent1 where i = 1;",
				Expected: []sql.UntypedSqlRow{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "select * from parent1;",
				Expected: []sql.UntypedSqlRow{
					{3},
					{20},
				},
			},
			{
				Query: "select * from child1 order by j, k;",
				Expected: []sql.UntypedSqlRow{
					{20, 3},
				},
			},
		},
	},
}

var CreateForeignKeyTests = []ScriptTest{
	{
		Name: "basic create foreign key tests",
		SetUpScript: []string{
			"CREATE TABLE parent(a INTEGER PRIMARY KEY, b INTEGER)",
			"ALTER TABLE parent ADD INDEX pb (b)",
			`CREATE TABLE child(c INTEGER PRIMARY KEY, d INTEGER,
					CONSTRAINT fk1 FOREIGN KEY (D) REFERENCES parent(B) ON DELETE CASCADE
				)`,
			"ALTER TABLE child ADD CONSTRAINT fk4 FOREIGN KEY (D) REFERENCES child(C)",
			"CREATE TABLE child2(e INTEGER PRIMARY KEY, f INTEGER)",
			"ALTER TABLE child2 ADD CONSTRAINT fk2 FOREIGN KEY (f) REFERENCES parent(b) ON DELETE RESTRICT",
			"ALTER TABLE child2 ADD CONSTRAINT fk3 FOREIGN KEY (f) REFERENCES child(d) ON UPDATE SET NULL",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT RC.CONSTRAINT_NAME, RC.CONSTRAINT_SCHEMA, RC.TABLE_NAME, KCU.COLUMN_NAME, 
						KCU.REFERENCED_TABLE_SCHEMA, KCU.REFERENCED_TABLE_NAME, KCU.REFERENCED_COLUMN_NAME, RC.UPDATE_RULE, RC.DELETE_RULE 
						FROM information_schema.REFERENTIAL_CONSTRAINTS RC, information_schema.KEY_COLUMN_USAGE KCU 
						WHERE RC.TABLE_NAME = 'child' AND RC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME AND
						RC.TABLE_NAME = KCU.TABLE_NAME AND RC.REFERENCED_TABLE_NAME = KCU.REFERENCED_TABLE_NAME;`,
				Expected: []sql.UntypedSqlRow{
					{"fk1", "mydb", "child", "d", "mydb", "parent", "b", "NO ACTION", "CASCADE"},
					{"fk4", "mydb", "child", "d", "mydb", "child", "c", "NO ACTION", "NO ACTION"},
				},
			},
			{
				Query: `SELECT RC.CONSTRAINT_NAME, RC.CONSTRAINT_SCHEMA, RC.TABLE_NAME, KCU.COLUMN_NAME, 
						KCU.REFERENCED_TABLE_SCHEMA, KCU.REFERENCED_TABLE_NAME, KCU.REFERENCED_COLUMN_NAME, RC.UPDATE_RULE, RC.DELETE_RULE 
						FROM information_schema.REFERENTIAL_CONSTRAINTS RC, information_schema.KEY_COLUMN_USAGE KCU 
						WHERE RC.TABLE_NAME = 'child2' AND RC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME AND
						RC.TABLE_NAME = KCU.TABLE_NAME AND RC.REFERENCED_TABLE_NAME = KCU.REFERENCED_TABLE_NAME;`,
				Expected: []sql.UntypedSqlRow{
					{"fk2", "mydb", "child2", "f", "mydb", "parent", "b", "NO ACTION", "RESTRICT"},
					{"fk3", "mydb", "child2", "f", "mydb", "child", "d", "SET NULL", "NO ACTION"},
				},
			},
		},
	},
	{
		Name: "error cases",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child2 ADD CONSTRAINT fk3 FOREIGN KEY (f) REFERENCES dne(d) ON UPDATE SET NULL",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "ALTER TABLE dne ADD CONSTRAINT fk4 FOREIGN KEY (f) REFERENCES child(d) ON UPDATE SET NULL",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "ALTER TABLE child2 ADD CONSTRAINT fk5 FOREIGN KEY (f) REFERENCES child(dne) ON UPDATE SET NULL",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
		},
	},
	{
		Name: "Add a column then immediately add a foreign key",
		SetUpScript: []string{
			"CREATE TABLE parent3 (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1))",
			"CREATE TABLE child3 (pk BIGINT PRIMARY KEY);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE child3 ADD COLUMN v1 BIGINT NULL, ADD CONSTRAINT fk_child3 FOREIGN KEY (v1) REFERENCES parent3(v1);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Do not validate foreign keys if FOREIGN_KEY_CHECKS is set to zero",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SET FOREIGN_KEY_CHECKS=0;",
				Expected: []sql.UntypedSqlRow{{}},
			},
			{
				Query:    "CREATE TABLE child4 (pk BIGINT PRIMARY KEY, CONSTRAINT fk_child4 FOREIGN KEY (pk) REFERENCES delayed_parent4 (pk))",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "CREATE TABLE delayed_parent4 (pk BIGINT PRIMARY KEY)",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
}

var DropForeignKeyTests = []ScriptTest{
	{
		Name: "basic drop foreign key tests",
		SetUpScript: []string{
			"CREATE TABLE parent(a INTEGER PRIMARY KEY, b INTEGER)",
			"ALTER TABLE parent ADD INDEX pb (b)",
			`CREATE TABLE child(c INTEGER PRIMARY KEY, d INTEGER,
					CONSTRAINT fk1 FOREIGN KEY (D) REFERENCES parent(B) ON DELETE CASCADE
				)`,
			"CREATE TABLE child2(e INTEGER PRIMARY KEY, f INTEGER)",
			`ALTER TABLE child2 ADD CONSTRAINT fk2 FOREIGN KEY (f) REFERENCES parent(b) ON DELETE RESTRICT, 
			ADD CONSTRAINT fk3 FOREIGN KEY (f) REFERENCES child(d) ON UPDATE SET NULL`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE child2 DROP CONSTRAINT fk2",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: `SELECT RC.CONSTRAINT_NAME, RC.CONSTRAINT_SCHEMA, RC.TABLE_NAME, KCU.COLUMN_NAME, 
						KCU.REFERENCED_TABLE_SCHEMA, KCU.REFERENCED_TABLE_NAME, KCU.REFERENCED_COLUMN_NAME, RC.UPDATE_RULE, RC.DELETE_RULE 
						FROM information_schema.REFERENTIAL_CONSTRAINTS RC, information_schema.KEY_COLUMN_USAGE KCU 
						WHERE RC.TABLE_NAME = 'child2' AND RC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME AND
						RC.TABLE_NAME = KCU.TABLE_NAME AND RC.REFERENCED_TABLE_NAME = KCU.REFERENCED_TABLE_NAME;`,
				Expected: []sql.UntypedSqlRow{
					{"fk3", "mydb", "child2", "f", "mydb", "child", "d", "SET NULL", "NO ACTION"},
				},
			},
			{
				Query:    "ALTER TABLE child2 DROP CONSTRAINT fk3",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: `SELECT RC.CONSTRAINT_NAME, RC.CONSTRAINT_SCHEMA, RC.TABLE_NAME, KCU.COLUMN_NAME, 
						KCU.REFERENCED_TABLE_SCHEMA, KCU.REFERENCED_TABLE_NAME, KCU.REFERENCED_COLUMN_NAME, RC.UPDATE_RULE, RC.DELETE_RULE 
						FROM information_schema.REFERENTIAL_CONSTRAINTS RC, information_schema.KEY_COLUMN_USAGE KCU 
						WHERE RC.TABLE_NAME = 'child2' AND RC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME AND
						RC.TABLE_NAME = KCU.TABLE_NAME AND RC.REFERENCED_TABLE_NAME = KCU.REFERENCED_TABLE_NAME;`,
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "error cases",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE child3 DROP CONSTRAINT dne",
				ExpectedErr: sql.ErrTableNotFound,
			},
			{
				Query:       "ALTER TABLE child2 DROP CONSTRAINT fk3",
				ExpectedErr: sql.ErrUnknownConstraint,
			},
			{
				Query:       "ALTER TABLE child2 DROP FOREIGN KEY fk3",
				ExpectedErr: sql.ErrForeignKeyNotFound,
			},
		},
	},
}
