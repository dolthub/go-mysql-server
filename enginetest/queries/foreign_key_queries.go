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
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n  `id` int NOT NULL,\n  `v1` int,\n  `v2` int,\n  PRIMARY KEY (`id`),\n  KEY `v1` (`v1`),\n  CONSTRAINT `fk_named` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
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
				Expected: []sql.Row{{"sibling", "CREATE TABLE `sibling` (\n  `id` int NOT NULL,\n  `v1` int,\n  PRIMARY KEY (`id`),\n  KEY `v1` (`v1`),\n  CONSTRAINT `fk_named` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "CREATE TABLE child2 (pk BIGINT PRIMARY KEY, v1 VARCHAR(30), CONSTRAINT fk_child2 FOREIGN KEY (v1) REFERENCES parent2 (v1));",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "CREATE TABLE child3 (pk BIGINT PRIMARY KEY, v1 BINARY(30), CONSTRAINT fk_child3 FOREIGN KEY (v1) REFERENCES parent3 (v1));",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			}, {
				Query:    "CREATE TABLE child4 (pk BIGINT PRIMARY KEY, v1 VARBINARY(30), CONSTRAINT fk_child4 FOREIGN KEY (v1) REFERENCES parent4 (v1));",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n  `id` int NOT NULL,\n  `v1` int,\n  `v2` int,\n  PRIMARY KEY (`id`),\n  KEY `v1` (`v1`),\n  CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "ALTER TABLE child DROP FOREIGN KEY fk_name;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "SHOW CREATE TABLE child;",
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n  `id` int NOT NULL,\n  `v1` int,\n  `v2` int,\n  PRIMARY KEY (`id`),\n  KEY `v1` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
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
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n  `id` int NOT NULL,\n  `v1` int,\n  `v2` int,\n  PRIMARY KEY (`id`),\n  KEY `v1` (`v1`),\n  CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `new_parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "RENAME TABLE child TO new_child;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "SHOW CREATE TABLE new_child;",
				Expected: []sql.Row{{"new_child", "CREATE TABLE `new_child` (\n  `id` int NOT NULL,\n  `v1` int,\n  `v2` int,\n  PRIMARY KEY (`id`),\n  KEY `v1` (`v1`),\n  CONSTRAINT `fk_name` FOREIGN KEY (`v1`) REFERENCES `new_parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "DROP TABLE parent;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child DROP INDEX v1;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE parent DROP INDEX v1;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{"child", "CREATE TABLE `child` (\n  `id` int NOT NULL,\n  `v1_new` int,\n  `v2` int,\n  PRIMARY KEY (`id`),\n  KEY `v1` (`v1_new`),\n  CONSTRAINT `fk1` FOREIGN KEY (`v1_new`) REFERENCES `parent` (`v1_new`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child1 MODIFY v1 CHAR(30);",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE parent2 MODIFY v1 VARCHAR(30);",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child2 MODIFY v1 VARCHAR(30);",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE parent3 MODIFY v1 BINARY(30);",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child3 MODIFY v1 BINARY(30);",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE parent4 MODIFY v1 VARBINARY(30);",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child4 MODIFY v1 VARBINARY(30);",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{ // Make sure the type change didn't cause INSERTs to break or some other strange behavior
				Query:    "INSERT INTO child2 VALUES (2, 'bb');",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child1 MODIFY v2 BIGINT;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE parent DROP COLUMN v1;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child DROP COLUMN v1;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
			"DELETE FROM one WHERE pk = 3;",
			"UPDATE two SET v2 = v1 - 2;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM one;",
				Expected: []sql.Row{{1, 5, 4}, {2, 7, 5}, {4, 9, 5}},
			},
			{
				Query:    "SELECT * FROM two;",
				Expected: []sql.Row{{2, 5, 3}, {3, 7, 5}},
			},
			{
				Query:    "SELECT * FROM three;",
				Expected: []sql.Row{{3, 5, 3}, {4, 7, 5}},
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
				Expected: []sql.Row{{1, 1, 1}, {2, 4, 8}, {3, 9, 27}, {4, 4, 16}},
			},
			{
				Query:    "SELECT * FROM two;",
				Expected: []sql.Row{{1, 1, 1}, {2, nil, 2}, {3, nil, 3}, {4, 4, 4}},
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
				Expected: []sql.Row{{sql.OkResult{Info: plan.UpdateInfo{Matched: 3}}}},
			},
			{
				Query:       "DELETE FROM one;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
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
				Expected: []sql.Row{{sql.OkResult{Info: plan.UpdateInfo{Matched: 3}}}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE parent ADD CONSTRAINT fk_name2 FOREIGN KEY (v1, v2) REFERENCES parent(v1, v2);",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{1, 1, 1}, {2, 2, 1}},
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
				Expected: []sql.Row{{sql.NewOkResult(3)}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.Row{{1, 1, 1}, {2, 2, 2}},
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
				Query:       "REPLACE INTO parent VALUES (1, 1, 2), (2, 2, 1);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:    "UPDATE parent SET v2 = 2 WHERE id = 1;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "UPDATE parent SET v2 = 1 WHERE id = 2;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.Row{{1, 1, 2}, {2, 2, 1}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.Row{},
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
				Expected: []sql.Row{{sql.NewOkResult(4)}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.Row{{1, 1, 1}, {2, 2, 2}},
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
				Expected: []sql.Row{{sql.NewOkResult(4)}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.Row{{1, 1, nil}, {2, 2, 1}},
			},
			{
				Query:    "UPDATE parent SET v2 = 2 WHERE id = 1;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "UPDATE parent SET v2 = 1 WHERE id = 2;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 1}}}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.Row{{1, 1, 2}, {2, 2, 1}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.Row{{2, 2, nil}},
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
				Expected: []sql.Row{{1, "red"}, {2, "green"}, {3, "blue"}, {4, "purple"}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "SELECT * FROM objects;",
				Expected: []sql.Row{{1, "truck", "red"}, {2, "ball", "green"}, {3, "shoe", "blue"}},
			},
		},
	},
	{
		Name: "Delayed foreign key resolution",
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
				Expected: []sql.Row{{"delayed_child", "CREATE TABLE `delayed_child` (\n  `pk` int NOT NULL,\n  `v1` int,\n  PRIMARY KEY (`pk`),\n  CONSTRAINT `fk_delayed` FOREIGN KEY (`v1`) REFERENCES `delayed_parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "SELECT * FROM delayed_parent;",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM delayed_child;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:       "INSERT INTO delayed_child VALUES (2, 3);",
				ExpectedErr: sql.ErrForeignKeyNotResolved,
			},
			{
				Query:    "INSERT INTO delayed_parent VALUES (1, 2), (2, 3);",
				Expected: []sql.Row{{sql.NewOkResult(2)}},
			},
			{
				Query:    "INSERT INTO delayed_child VALUES (2, 3);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM delayed_child;",
				Expected: []sql.Row{{1, 2}, {2, 3}},
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
				Expected: []sql.Row{{"delayed_child", "CREATE TABLE `delayed_child` (\n  `pk` int NOT NULL,\n  `v1` int,\n  PRIMARY KEY (`pk`),\n  CONSTRAINT `fk_delayed` FOREIGN KEY (`v1`) REFERENCES `delayed_parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "SELECT * FROM delayed_child;",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:       "INSERT INTO delayed_child VALUES (2, 3);",
				ExpectedErr: sql.ErrForeignKeyNotResolved,
			},
			{
				Query:    "CREATE TABLE delayed_parent (pk INT PRIMARY KEY, v1 INT, INDEX (v1));",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO delayed_parent VALUES (1, 2), (2, 3);",
				Expected: []sql.Row{{sql.NewOkResult(2)}},
			},
			{
				Query:    "INSERT INTO delayed_child VALUES (2, 3);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM delayed_child;",
				Expected: []sql.Row{{1, 2}, {2, 3}},
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
				Query:       "DROP TABLE parent;",
				ExpectedErr: sql.ErrForeignKeyDropTable,
			},
			{
				Query:    "SET FOREIGN_KEY_CHECKS=0;",
				Expected: []sql.Row{{}},
			},
			{
				Query:    "DROP TABLE parent;",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "SET FOREIGN_KEY_CHECKS=1;",
				Expected: []sql.Row{{}},
			},
			{
				Query:       "INSERT INTO child VALUES (4, 5, 6);",
				ExpectedErr: sql.ErrForeignKeyNotResolved,
			},
			{
				Query:    "CREATE TABLE parent (pk INT PRIMARY KEY, v1 INT, INDEX (v1));",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO parent VALUES (1, 5);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "INSERT INTO child VALUES (4, 5, 6);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.Row{{1, 5}},
			},
			{
				Query:    "SELECT * FROM child;",
				Expected: []sql.Row{{4, 5, 6}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE public.cities ADD CONSTRAINT foreign_key1 FOREIGN KEY (state) REFERENCES public.states(state);",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
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
				Expected: []sql.Row{{"hours", "CREATE TABLE `hours` (\n  `restaurant_id` int NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`restaurant_id`),\n  CONSTRAINT `fk_name` FOREIGN KEY (`restaurant_id`) REFERENCES `restaurants` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
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
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child ADD CONSTRAINT fk2 FOREIGN KEY (a) REFERENCES parent (b);",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child ADD CONSTRAINT fk3 FOREIGN KEY (a, b) REFERENCES parent (a, b);",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE child ADD CONSTRAINT fk4 FOREIGN KEY (b, a) REFERENCES parent (b, a);",
				Expected: []sql.Row{{sql.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Reordered foreign key columns match an index's prefix, INSERT values",
		SetUpScript: []string{
			"DROP TABLE child;",
			"DROP TABLE parent;",
			"CREATE TABLE parent(pk DOUBLE PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX(v1, v2, pk));",
			"INSERT INTO parent VALUES (1, 1, 1), (2, 1, 2);",
			"CREATE TABLE child(pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, CONSTRAINT fk_child FOREIGN KEY (v2, v1) REFERENCES parent(v2, v1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO child VALUES (1, 1, 1);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:       "INSERT INTO child VALUES (2, 2, 2);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:       "INSERT INTO child VALUES (3, 2, 1);",
				ExpectedErr: sql.ErrForeignKeyChildViolation,
			},
			{
				Query:    "INSERT INTO child VALUES (4, 1, 2);",
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				ExpectedErr: sql.ErrForeignKeyChildViolation,
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}}, // Cascading deletions do not count
			},
			{
				Query:    "SELECT * FROM self;",
				Expected: []sql.Row{},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM parent;",
				Expected: []sql.Row{{2, 2, 2}, {3, 3, 3}},
			},
			{
				Query:    "SELECT * FROM child;",
				Expected: []sql.Row{{1, nil, nil}, {2, 2, 2}, {3, 3, 3}},
			},
			{
				Query:    "SELECT * FROM child2;",
				Expected: []sql.Row{{1, nil, nil}, {2, 2, 2}, {3, 3, 3}},
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
				Expected: []sql.Row{{sql.NewOkResult(2)}},
			},
			{
				Query:    "SELECT * FROM two where pk = 2",
				Expected: []sql.Row{{2, 2, 1}},
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
				Expected: []sql.Row{{sql.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM child1;",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "UPDATE parent1 SET pk = 2;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "SELECT * FROM child1;",
				Expected: []sql.Row{{1, 2}},
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
				Expected: []sql.Row{{sql.NewOkResult(2)}},
			},
			{
				Query:    "SELECT * FROM child1;",
				Expected: []sql.Row{{1, 1, 2}, {2, 4, 5}},
			},
			{
				Query:    "UPDATE parent1 SET pk2 = pk1 + pk2;",
				Expected: []sql.Row{{sql.OkResult{RowsAffected: 2, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}}},
			},
			{
				Query:    "SELECT * FROM child1;",
				Expected: []sql.Row{{1, 1, 3}, {2, 4, 9}},
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
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM two;",
				Expected: []sql.Row{},
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
				Expected: []sql.Row{{1, 5, 4}, {2, 7, 5}, {4, 9, 5}},
			},
			{
				Query:    "SELECT * FROM two;",
				Expected: []sql.Row{{2, 5, 3}, {3, 7, 5}},
			},
			{
				Query:    "SELECT * FROM three;",
				Expected: []sql.Row{{3, 5, 3}, {4, 7, 5}},
			},
		},
	},
}
