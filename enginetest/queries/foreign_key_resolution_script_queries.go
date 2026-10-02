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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ForeignKeyResolutionScriptTests contains self-contained foreign key resolution script tests.
var ForeignKeyResolutionScriptTests = []ScriptTest{
	{
		Name:    "resolve foreign key on indexed update",
		Dialect: "mysql", // no way to disable foreign keys in doltgres yet
		SetUpScript: []string{
			"set foreign_key_checks=0;",
			"create table parent (i int primary key);",
			"create table child (i int primary key, foreign key (i) references parent(i));",
			"set foreign_key_checks=1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "update child set i = 1 where i = 1;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 0, Updated: 0}}},
				},
			},
		},
	},
}

var ForeignKeyResolutionTests = []ScriptTest{
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
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 1, Updated: 0}}},
				},
			},
			{
				// Update on non-existent row should not cause constraint violation
				Query: "update delayed_child set pk=3 where pk=3;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 0, Updated: 0}}},
				},
			},
			{
				// No-op update good to good should not cause constraint violation
				Query: "update delayed_child set pk=20 where pk=20;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 1, Updated: 0}}},
				},
			},
			{
				// Updating bad value to good value still fails
				Query: "update delayed_child set pk=10 where pk=1;",
				Expected: []sql.Row{
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
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 0}},
				},
			},
			{
				Query: "delete from delayed_child where pk = 20;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "delete from delayed_child where pk = 1;",
				Expected: []sql.Row{
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
				Expected: []sql.Row{{"delayed_child", "CREATE TABLE `delayed_child` (\n  `pk` int NOT NULL,\n  `v1` int,\n  PRIMARY KEY (`pk`),\n  KEY `fk_delayed` (`v1`),\n  CONSTRAINT `fk_delayed` FOREIGN KEY (`v1`) REFERENCES `delayed_parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
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
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "INSERT INTO delayed_child VALUES (2, 3);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT * FROM delayed_child;",
				Expected: []sql.Row{{1, 2}, {2, 3}},
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
				Expected: []sql.Row{{"delayed_child", "CREATE TABLE `delayed_child` (\n  `pk` int NOT NULL,\n  `v1` int,\n  PRIMARY KEY (`pk`),\n  KEY `fk_delayed` (`v1`),\n  CONSTRAINT `fk_delayed` FOREIGN KEY (`v1`) REFERENCES `delayed_parent` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
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
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO delayed_parent VALUES (1, 2), (2, 3);",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "INSERT INTO delayed_child VALUES (2, 3);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
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
				Query:       "TRUNCATE parent;",
				ExpectedErr: sql.ErrTruncateReferencedFromForeignKey,
			},
			{
				Query:       "DROP TABLE parent;",
				ExpectedErr: sql.ErrForeignKeyDropTable,
			},
			{
				Query:    "SET FOREIGN_KEY_CHECKS=0;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "TRUNCATE parent;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "DROP TABLE parent;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SET FOREIGN_KEY_CHECKS=1;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:       "INSERT INTO child VALUES (4, 5, 6);",
				ExpectedErr: sql.ErrForeignKeyNotResolved,
			},
			{
				Query:    "CREATE TABLE parent (pk INT PRIMARY KEY, v1 INT, INDEX (v1));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO parent VALUES (1, 5);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "INSERT INTO child VALUES (4, 5, 6);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
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
}
