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
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"math"
	"time"
)

// WritesScriptTests contains self-contained script tests for writes.
var WritesScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/9873
		// TODO: `FOR UPDATE OF` (`FOR UPDATE` in general) is currently a no-op: https://www.dolthub.com/blog/2023-10-23-hold-my-beer/
		Name:    "FOR UPDATE OF syntax support tests",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE task_instance (id INT PRIMARY KEY, task_id VARCHAR(255), dag_id VARCHAR(255), run_id VARCHAR(255), state VARCHAR(50), queued_by_job_id INT)",
			"CREATE TABLE job (id INT PRIMARY KEY, state VARCHAR(50))",
			"CREATE TABLE dag_run (dag_id VARCHAR(255), run_id VARCHAR(255), state VARCHAR(50))",
			"CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50))",
			"INSERT INTO task_instance VALUES (1, 'task1', 'dag1', 'run1', 'running', 1)",
			"INSERT INTO job VALUES (1, 'running')",
			"INSERT INTO dag_run VALUES ('dag1', 'run1', 'running')",
			"INSERT INTO t VALUES (1, 'test')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT task_instance.id, task_instance.task_id, task_instance.dag_id, task_instance.run_id
FROM task_instance INNER JOIN job ON job.id = task_instance.queued_by_job_id INNER JOIN dag_run ON dag_run.dag_id = task_instance.dag_id AND dag_run.run_id = task_instance.run_id
 WHERE task_instance.state IN ('running', 'queued', 'scheduled') AND NOT (job.state <=> 'running') AND dag_run.state = 'running' FOR UPDATE OF task_instance SKIP LOCKED`,
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT * FROM t FOR UPDATE",
				Expected: []sql.Row{{1, "test"}},
			},
			{
				Query:    "SELECT * FROM t FOR UPDATE OF t",
				Expected: []sql.Row{{1, "test"}},
			},
			{
				Query:    "SELECT * FROM t FOR UPDATE OF t SKIP LOCKED",
				Expected: []sql.Row{{1, "test"}},
			},
			{
				Query:    "SELECT * FROM t FOR UPDATE OF t NOWAIT",
				Expected: []sql.Row{{1, "test"}},
			},
			{
				Query:    "SELECT * FROM task_instance t1, job t2 FOR UPDATE OF t1, t2",
				Expected: []sql.Row{{1, "task1", "dag1", "run1", "running", 1, 1, "running"}},
			},
			{
				Query:       "SELECT * FROM t FOR UPDATE OF nonexistent_table",
				ExpectedErr: sql.ErrUnresolvedTableLock,
			},
			{
				Query:          "SELECT * FROM t FOR UPDATE OF t, nonexistent_table",
				ExpectedErr:    sql.ErrUnresolvedTableLock,
				ExpectedErrStr: fmt.Sprintf(sql.ErrUnresolvedTableLock.Message, "nonexistent_table"),
			},
			{
				Query:       "SELECT * FROM t FOR UPDATE OF",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:       "SELECT * FROM t FOR UPDATE test",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:    "SELECT * FROM t FOR UPDATE",
				Expected: []sql.Row{{1, "test"}},
			},
		},
	},
	{
		// https://github.com/dolthub/go-mysql-server/issues/2369
		Name: "auto_increment with self-referencing foreign key",
		SetUpScript: []string{
			`CREATE TABLE table1 (
	id int NOT NULL AUTO_INCREMENT,
	name text,
	parentId int DEFAULT NULL,
	PRIMARY KEY (id),
	CONSTRAINT myConstraint FOREIGN KEY (parentId) REFERENCES table1 (id) ON DELETE CASCADE
)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO table1 (name, parentId) VALUES ('tbl1 row 1', NULL);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 1}}},
			},
			{
				Query:    "INSERT INTO table1 (name, parentId) VALUES ('tbl1 row 2', 1);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 2}}},
			},
			{
				Query:    "INSERT INTO table1 (name, parentId) VALUES ('tbl1 row 3', NULL);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 3}}},
			},
			{
				Query: "select * from table1",
				Expected: []sql.Row{
					{1, "tbl1 row 1", nil},
					{2, "tbl1 row 2", 1},
					{3, "tbl1 row 3", nil},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/go-mysql-server/issues/2349
		Name: "auto_increment with foreign key",
		SetUpScript: []string{
			"CREATE TABLE table1 (id int NOT NULL AUTO_INCREMENT primary key, name text)",
			`
CREATE TABLE table2 (
	id int NOT NULL AUTO_INCREMENT,
	name text,
	fk int,
	PRIMARY KEY (id),
	CONSTRAINT myConstraint FOREIGN KEY (fk) REFERENCES table1 (id)
)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO table1 (name) VALUES ('tbl1 row 1');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 1}}},
			},
			{
				Query:    "INSERT INTO table1 (name) VALUES ('tbl1 row 2');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 2}}},
			},
		},
	},
	{
		Name: "update exponential parsing",
		SetUpScript: []string{
			"create table a (a int primary key, b double);",
			"insert into a values (0, 0.0),(1, 1.0)",
			"update a set b = 5.0E-5 where a = 0",
			"update a set b = 5.0e-5 where a = 1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from a",
				Expected: []sql.Row{{0, .00005}, {1, .00005}},
			},
		},
	},
	{
		Name: "failed statements data validation for INSERT, UPDATE",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1));",
			"INSERT INTO test VALUES (1,1), (4,4), (5,5);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "INSERT INTO test VALUES (2,2), (3,3), (1,1);",
				ExpectedErrStr: "duplicate primary key given: [1]",
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 1}, {4, 4}, {5, 5}},
			},
			{
				Query:          "UPDATE test SET pk = pk + 1 ORDER BY pk;",
				ExpectedErrStr: "duplicate primary key given: [5]",
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 1}, {4, 4}, {5, 5}},
			},
		},
	},
	{
		Name: "failed statements data validation for DELETE, REPLACE",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, INDEX (v1));",
			"INSERT INTO test VALUES (1,1), (4,4), (5,5);",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, CONSTRAINT fk_test FOREIGN KEY (pk) REFERENCES test (v1));",
			"INSERT INTO test2 VALUES (4);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "DELETE FROM test WHERE pk > 0;",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.Row{{1, 1}, {4, 4}, {5, 5}},
			},
			{
				Query:    "SELECT * FROM test2;",
				Expected: []sql.Row{{4}},
			},
			{
				Query:       "REPLACE INTO test VALUES (1,7), (4,8), (5,9);",
				Dialect:     "mysql",
				ExpectedErr: sql.ErrForeignKeyParentViolation,
			},
			{
				Query:    "SELECT * FROM test;",
				Dialect:  "mysql",
				Expected: []sql.Row{{1, 1}, {4, 4}, {5, 5}},
			},
			{
				Query:    "SELECT * FROM test2;",
				Expected: []sql.Row{{4}},
			},
		},
	},
	{
		Name:    "last_insert_uuid() behavior",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table varchar36 (pk varchar(36) primary key default (UUID()), i int);",
			"create table char36 (pk char(36) primary key default (UUID()), i int);",
			"create table varbinary16 (pk varbinary(16) primary key default (UUID_to_bin(UUID())), i int);",
			"create table binary16 (pk binary(16) primary key default (UUID_to_bin(UUID())), i int);",
			"create table binary16swap (pk binary(16) primary key default (UUID_to_bin(UUID(), true)), i int);",
			"create table invalid (pk int primary key, c1 varchar(36) default (UUID()));",
			"create table prepared (uuid char(36) default (UUID()), ai int auto_increment, c1 varchar(100), primary key (uuid, ai));",
		},
		Assertions: []ScriptTestAssertion{
			// The initial value of last_insert_uuid() is an empty string
			{
				Query:    "select last_insert_uuid()",
				Expected: []sql.Row{{""}},
			},

			// invalid table – UUID default is not a primary key, so last_insert_uuid() doesn't get udpated
			{
				Query:    "insert into invalid values (1, DEFAULT);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select last_insert_uuid()",
				Expected: []sql.Row{{""}},
			},
			{
				Query:    "insert into invalid values (2, UUID());",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select last_insert_uuid()",
				Expected: []sql.Row{{""}},
			},

			// varchar(36) test cases...
			{
				Query:    "insert into varchar36 values (DEFAULT, 1);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select pk from varchar36 where i=1);",
				Expected: []sql.Row{{true, true}},
			},
			{
				Query:    "insert into varchar36 values (UUID(), 2), (UUID(), 3);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2}}},
			},
			{
				// last_insert_uuid() reports the first UUID() generated in the last insert statement
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select pk from varchar36 where i=2);",
				Expected: []sql.Row{{true, true}},
			},
			{
				Query:    "insert into varchar36 values ('notta-uuid', 4);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				// The previous insert didn't generate a UUID, so last_insert_uuid() doesn't get updated
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select pk from varchar36 where i=2);",
				Expected: []sql.Row{{true, true}},
			},

			// char(36) test cases...
			{
				Query:    "insert into char36 values (DEFAULT, 1);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select pk from char36 where i=1);",
				Expected: []sql.Row{{true, true}},
			},
			{
				Query:    "insert into char36 values (UUID(), 2), (UUID(), 3);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2}}},
			},
			{
				// last_insert_uuid() reports the first UUID() generated in the last insert statement
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select pk from char36 where i=2);",
				Expected: []sql.Row{{true, true}},
			},
			{
				Query:    "insert into char36 values ('notta-uuid', 4);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				// The previous insert didn't generate a UUID, so last_insert_uuid() doesn't get updated
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select pk from char36 where i=2);",
				Expected: []sql.Row{{true, true}},
			},
			{
				Query:    "insert into char36 (i) values (5);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select pk from char36 where i=5);",
				Expected: []sql.Row{{true, true}},
			},

			// varbinary(16) test cases...
			{
				Query:    "insert into varbinary16 values (DEFAULT, 1);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select bin_to_uuid(pk) from varbinary16 where i=1);",
				Expected: []sql.Row{{true, true}},
			},
			{
				Query:    "insert into varbinary16 values (UUID_to_bin(UUID()), 2), (UUID_to_bin(UUID()), 3);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2}}},
			},
			{
				// last_insert_uuid() reports the first UUID() generated in the last insert statement
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select bin_to_uuid(pk) from varbinary16 where i=2);",
				Expected: []sql.Row{{true, true}},
			},
			{
				Query:    "insert into varbinary16 values ('notta-uuid', 4);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				// The previous insert didn't generate a UUID, so last_insert_uuid() doesn't get updated
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select bin_to_uuid(pk) from varbinary16 where i=2);",
				Expected: []sql.Row{{true, true}},
			},

			// binary(16) test cases...
			{
				Query:    "insert into binary16 values (DEFAULT, 1);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select bin_to_uuid(pk) from binary16 where i=1);",
				Expected: []sql.Row{{true, true}},
			},
			{
				Query:    "insert into binary16 values (UUID_to_bin(UUID()), 2), (UUID_to_bin(UUID()), 3);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2}}},
			},
			{
				// last_insert_uuid() reports the first UUID() generated in the last insert statement
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select bin_to_uuid(pk) from binary16 where i=2);",
				Expected: []sql.Row{{true, true}},
			},
			{
				Query:    "insert into binary16 values ('notta-uuid', 4);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				// The previous insert didn't generate a UUID, so last_insert_uuid() doesn't get updated
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select bin_to_uuid(pk) from binary16 where i=2);",
				Expected: []sql.Row{{true, true}},
			},
			{
				Query:    "insert into binary16 (i) values (5);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select bin_to_uuid(pk) from binary16 where i=5);",
				Expected: []sql.Row{{true, true}},
			},

			// binary(16) with UUID_to_bin swap test cases...
			{
				Query:    "insert into binary16swap values (DEFAULT, 1);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select bin_to_uuid(pk, true) from binary16swap where i=1);",
				Expected: []sql.Row{{true, true}},
			},
			{
				Query:    "insert into binary16swap values (UUID_to_bin(UUID(), true), 2), (UUID_to_bin(UUID(), true), 3);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2}}},
			},
			{
				// last_insert_uuid() reports the first UUID() generated in the last insert statement
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select bin_to_uuid(pk, true) from binary16swap where i=2);",
				Expected: []sql.Row{{true, true}},
			},
			{
				Query:    "insert into binary16swap values ('notta-uuid', 4);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				// The previous insert didn't generate a UUID, so last_insert_uuid() doesn't get updated
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select bin_to_uuid(pk, true) from binary16swap where i=2);",
				Expected: []sql.Row{{true, true}},
			},
			{
				Query:    "insert into binary16swap (i) values (5);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select bin_to_uuid(pk, true) from binary16swap where i=5);",
				Expected: []sql.Row{{true, true}},
			},

			// INSERT INTO ... SELECT ... Tests
			{
				// If we populate the UUID column (pk) with its implicit default, then it updates last_insert_uuid()
				Query:    "insert into varchar36 (i) select 42 from dual;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select pk from varchar36 where i=42);",
				Expected: []sql.Row{{true, true}},
			},
			{
				// If all values come from another table, the auto_uuid value shouldn't be generated, so last_insert_uuid() doesn't change
				Query:    "insert into varchar36 (pk, i) (select 'one', 101 from dual union all select 'two', 202);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select pk from varchar36 where i=42);",
				Expected: []sql.Row{{true, true}},
			},

			// Prepared statements
			{
				// Test with an insert statement that implicit uses the UUID column default
				Query:    `prepare stmt1 from "insert into prepared (c1) values ('odd'), ('even')";`,
				Expected: []sql.Row{{types.OkResult{Info: plan.PrepareInfo{}}}},
			},
			{
				Query:                         "execute stmt1;",
				Expected:                      []sql.Row{{types.OkResult{RowsAffected: 2, InsertID: 1}}},
				SkipResultCheckOnServerEngine: true, // Server engine returns []sql.Row{}
			},
			{
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select uuid from prepared where ai=1), last_insert_id();",
				Expected: []sql.Row{{true, true, uint64(1)}},
			},
			{
				// Executing the prepared statement a second time should refresh last_insert_uuid()
				Query:                         "execute stmt1;",
				Expected:                      []sql.Row{{types.OkResult{RowsAffected: 2, InsertID: 3}}},
				SkipResultCheckOnServerEngine: true, // Server engine returns []sql.Row{}
			},
			{
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select uuid from prepared where ai=3), last_insert_id();",
				Expected: []sql.Row{{true, true, uint64(3)}},
			},

			{
				// Test with an insert statement that explicitly uses the UUID column default
				Query:    `prepare stmt2 from "insert into prepared (uuid, c1) values (DEFAULT, 'more'), (DEFAULT, 'less')";`,
				Expected: []sql.Row{{types.OkResult{Info: plan.PrepareInfo{}}}},
			},
			{
				Query:                         "execute stmt2;",
				Expected:                      []sql.Row{{types.OkResult{RowsAffected: 2, InsertID: 5}}},
				SkipResultCheckOnServerEngine: true, // Server engine returns []sql.Row{}
			},
			{
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select uuid from prepared where ai=5), last_insert_id();",
				Expected: []sql.Row{{true, true, uint64(5)}},
			},
			{
				// Executing the prepared statement a second time should refresh last_insert_uuid()
				Query:                         "execute stmt2;",
				Expected:                      []sql.Row{{types.OkResult{RowsAffected: 2, InsertID: 7}}},
				SkipResultCheckOnServerEngine: true, // Server engine returns []sql.Row{}
			},
			{
				Query:    "select is_uuid(last_insert_uuid()), last_insert_uuid() = (select uuid from prepared where ai=7), last_insert_id();",
				Expected: []sql.Row{{true, true, uint64(7)}},
			},
		},
	},
	{
		Name:    "last_insert_id() behavior",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table a (x int primary key auto_increment, y int)",
			"create table b (x int primary key)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select last_insert_id()",
				Expected: []sql.Row{{uint64(0)}},
			},
			{
				Query:    "insert into a (x,y) values (1,1)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 1}}},
			},
			{
				Query:    "select last_insert_id()",
				Expected: []sql.Row{{uint64(0)}},
			},
			{
				Query:    "insert into a (y) values (1)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 2}}},
			},
			{
				Query:    "select last_insert_id()",
				Expected: []sql.Row{{uint64(2)}},
			},
			{
				Query:    "insert into a (y) values (2), (3)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2, InsertID: 3}}},
			},
			{
				// last_insert_id() should return the insert id of the *first* value inserted in the last statement
				Query:    "select last_insert_id()",
				Expected: []sql.Row{{uint64(3)}},
			},
			{
				Query:    "insert into b (x) values (1), (2)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2, InsertID: 0}}},
			},
			{
				// The above query doesn't have an auto increment column, so last_insert_id is unchanged
				Query:    "select last_insert_id()",
				Expected: []sql.Row{{uint64(3)}},
			},
			{
				Query: "insert into a (x, y) values (-100, 10)",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 1,
					InsertID:     math.MaxUint64 - 100 + 1,
				}}},
			},
			{
				// last_insert_id() should not update for manually inserted values
				Query:    "select last_insert_id()",
				Expected: []sql.Row{{uint64(3)}},
			},
			{
				Query: "insert into a (x, y) values (100, 10)",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 1,
					InsertID:     100,
				}}},
			},
			{
				// last_insert_id() should not update for manually inserted values
				Query:    "select last_insert_id()",
				Expected: []sql.Row{{uint64(3)}},
			},
		},
	},
	{
		Name:    "last_insert_id(expr) behavior",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table a (x int primary key auto_increment, y int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into a (y) values (1)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 1}}},
			},
			{
				Query:    "select last_insert_id()",
				Expected: []sql.Row{{uint64(1)}},
			},
			{
				Query:    "insert into a (x, y) values (1, 1) on duplicate key update y = 2, x=last_insert_id(x)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2, InsertID: 1}}},
			},
			{
				Query:    "select * from a order by x",
				Expected: []sql.Row{{1, 2}},
			},
			{
				Query:    "select last_insert_id()",
				Expected: []sql.Row{{uint64(1)}},
			},
			{
				Query:    "insert into a (y) values (100)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 2}}},
			},
			{
				Query:    "select last_insert_id()",
				Expected: []sql.Row{{uint64(2)}},
			},
		},
	},
	{
		Name:    "last_insert_id(default) behavior",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (pk int primary key auto_increment, i int default 0)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t(pk) values (default);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 1}}},
			},
			{
				Query: "select last_insert_id()",
				Expected: []sql.Row{
					{uint64(1)},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1, 0},
				},
			},

			{
				Query:    "insert into t(pk) values (default), (default), (default), (default), (default);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 5, InsertID: 2}}},
			},
			{
				Query: "select last_insert_id()",
				Expected: []sql.Row{
					{uint64(2)},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1, 0},
					{2, 0},
					{3, 0},
					{4, 0},
					{5, 0},
					{6, 0},
				},
			},

			{
				Query:    "insert into t(pk) values (10), (default);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 2, InsertID: 10}}},
			},
			{
				Query: "select last_insert_id()",
				Expected: []sql.Row{
					{uint64(11)},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1, 0},
					{2, 0},
					{3, 0},
					{4, 0},
					{5, 0},
					{6, 0},
					{10, 0},
					{11, 0},
				},
			},

			{
				Query:    "insert into t(pk) values (20), (default), (default);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 3, InsertID: 20}}},
			},
			{
				Query: "select last_insert_id()",
				Expected: []sql.Row{
					{uint64(21)},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1, 0},
					{2, 0},
					{3, 0},
					{4, 0},
					{5, 0},
					{6, 0},
					{10, 0},
					{11, 0},
					{20, 0},
					{21, 0},
					{22, 0},
				},
			},

			{
				Query:    "insert into t(i) values (100);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 23}}},
			},
			{
				Query: "select last_insert_id()",
				Expected: []sql.Row{
					{uint64(23)},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1, 0},
					{2, 0},
					{3, 0},
					{4, 0},
					{5, 0},
					{6, 0},
					{10, 0},
					{11, 0},
					{20, 0},
					{21, 0},
					{22, 0},
					{23, 100},
				},
			},

			{
				Query:    "insert into t(i, pk) values (200, default);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 24}}},
			},
			{
				Query: "select last_insert_id()",
				Expected: []sql.Row{
					{uint64(24)},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1, 0},
					{2, 0},
					{3, 0},
					{4, 0},
					{5, 0},
					{6, 0},
					{10, 0},
					{11, 0},
					{20, 0},
					{21, 0},
					{22, 0},
					{23, 100},
					{24, 200},
				},
			},

			{
				Query:    "insert into t(pk) values (null);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 25}}},
			},
			{
				Query: "select last_insert_id()",
				Expected: []sql.Row{
					{uint64(25)},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1, 0},
					{2, 0},
					{3, 0},
					{4, 0},
					{5, 0},
					{6, 0},
					{10, 0},
					{11, 0},
					{20, 0},
					{21, 0},
					{22, 0},
					{23, 100},
					{24, 200},
					{25, 0},
				},
			},

			{
				Query:    "insert into t values ();",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, InsertID: 26}}},
			},
			{
				Query: "select last_insert_id()",
				Expected: []sql.Row{
					{uint64(26)},
				},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1, 0},
					{2, 0},
					{3, 0},
					{4, 0},
					{5, 0},
					{6, 0},
					{10, 0},
					{11, 0},
					{20, 0},
					{21, 0},
					{22, 0},
					{23, 100},
					{24, 200},
					{25, 0},
					{26, 0},
				},
			},
		},
	},
	{
		Name: "INSERT INTO ... SELECT with AUTO_INCREMENT",
		SetUpScript: []string{
			"create table ai (pk int primary key auto_increment, c0 int);",
			"create table other (pk int primary key);",
			"insert into other values (1), (2), (3)",
			"insert into ai (c0) select * from other order by other.pk;",
		},
		Query: "select * from ai;",
		Expected: []sql.Row{
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Name: "table with defaults, insert with on duplicate key update",
		SetUpScript: []string{
			"create table t (a int primary key, b int default 100);",
			"insert into t values (1, 1), (2, 2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t values (1, 10) on duplicate key update b = 10",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
		},
	},
	{
		Name:    "INSERT IGNORE correctly truncates column data",
		Dialect: "mysql",
		SetUpScript: []string{
			`CREATE TABLE t (
				pk int primary key,
				col1 boolean,
				col2 integer,
				col3 tinyint,
				col4 smallint,
				col5 mediumint,
				col6 int,
				col7 bigint,
				col8 decimal,
				col9 float,
				col10 double,
				col11 date,
				col12 time,
				col13 datetime,
				col14 timestamp,
				col15 year,
				col16 ENUM('first', 'second'),
				col17 SET('a', 'b')
			);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
					INSERT IGNORE INTO t VALUES (
						1, 'val1', 'val2', 'val3', 'val4', 'val5', 'val6', 'val7', 'val8', 'val9', 'val10',
						'val11', 'val12', 'val13', 'val14', 'val15', 'val16', 'val17'
					);
				`,
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				SkipResultCheckOnServerEngine: true, // the datetime returned is not non-zero
				Query:                         "SELECT * from t",
				Expected: []sql.Row{
					{
						1,
						0,
						0,
						0,
						0,
						0,
						0,
						0,
						"0",
						float64(0),
						float64(0),
						time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC),
						types.Timespan(0),
						time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC),
						time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC),
						0,
						"",
						"",
					},
				},
			},
		},
	},
	{
		Name: "INSERT IGNORE throws an error when json is badly formatted",
		SetUpScript: []string{
			"CREATE TABLE t (pk int primary key, col1 json);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "INSERT IGNORE into t VALUES (1, 'val1');",
				ExpectedErr: sql.ErrInvalidJson,
			},
		},
	},
	{
		Name: "empty table update",
		SetUpScript: []string{
			"create table t (i int primary key)",
			"insert into t values (1), (2), (3)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "update t set i = 0 where false",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, InsertID: 0, Info: plan.UpdateInfo{Matched: 0}}}},
			},
			{
				Query: "select * from t",
				Expected: []sql.Row{
					{1},
					{2},
					{3},
				},
			},
		},
	},
	{
		Name: "update columns with default",
		SetUpScript: []string{
			"create table t (i int default 10, j varchar(128) default (concat('abc', 'def')));",
			"insert into t values (100, 'a'), (200, 'b');",
			"create table t2 (i int);",
			"insert into t2 values (1), (2), (3);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "update t set i = default where i = 100;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{10, "a"},
					{200, "b"},
				},
			},
			{
				Query: "update t set j = default where i = 200;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{10, "a"},
					{200, "abcdef"},
				},
			},
			{
				Query: "update t set i = default, j = default;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 2, Info: plan.UpdateInfo{Matched: 2, Updated: 2}}},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{10, "abcdef"},
					{10, "abcdef"},
				},
			},
			{
				Query: "update t2 set i = default",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 3, Info: plan.UpdateInfo{Matched: 3, Updated: 3}}},
				},
			},
			{
				Query: "select * from t2",
				Expected: []sql.Row{
					{nil},
					{nil},
					{nil},
				},
			},
		},
	},
	{
		Name:    "bit default value",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int primary key, b bit(2) default 2);",
			"insert into t(i) values (1);",
			"create table tt (b bit(2) default 2 primary key);",
			"insert into tt values ();",
		},
		Assertions: []ScriptTestAssertion{
			{
				Skip:  true, // this fails on server engine, even when skipped
				Query: "select * from t;",
				Expected: []sql.Row{
					{1, uint8(2)},
				},
			},
			{
				Skip:  true, // this fails on server engine, even when skipped
				Query: "select * from tt;",
				Expected: []sql.Row{
					{uint8(2)},
				},
			},
		},
	},

	// Char tests
	{
		Name:        "char with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (c char primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'c'",
			},
		},
	},

	// Varchar tests
	{
		Name:        "varchar with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (vc char(100) primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'vc'", // We throw the wrong error
			},
		},
	},

	// Binary tests
	{
		Name:        "binary with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (b binary(100) primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'b'",
			},
		},
	},

	// Varbinary tests
	{
		Name:        "varbinary with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (vb varbinary(100) primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'vb'",
			},
		},
	},

	// Blob tests
	{
		Name:        "blob with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (b blob primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'b'",
			},
			{
				Query:          "create table bad (tb tinyblob primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'tb'",
			},
			{
				Query:          "create table bad (mb mediumblob primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'mb'",
			},
			{
				Query:          "create table bad (lb longblob primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'lb'",
			},
		},
	},

	// Text Tests
	{
		Name:        "text with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table bad (t text primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 't'", // We throw the wrong error
			},
			{
				Query:          "create table bad (tt tinytext primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'tt'", // We throw the wrong error
			},
			{
				Query:          "create table bad (mt mediumtext primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'mt'", // We throw the wrong error
			},
			{
				Query:          "create table bad (lt longtext primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'lt'", // We throw the wrong error
			},
		},
	},
	{
		Name:        "MySQL default and strict SQL_MODE behavior",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// Disabling `NO_ZERO_IN_DATE` throws additional warning
				Query: "set @@sql_mode = '" +
					"STRICT_TRANS_TABLES," +
					"NO_ZERO_DATE," +
					"ERROR_FOR_DIVISION_BY_ZERO'",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
				ExpectedWarningsCount:           2,
				ExpectedWarning:                 3135,
				ExpectedWarningMessageSubstring: "Removing NO_ZERO_IN_DATE mode is not supported",
			},
			{
				Query: "select @@sql_mode",
				Expected: []sql.Row{
					{"STRICT_TRANS_TABLES,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO"},
				},
			},
			{
				// Disabling `STRICT_TRANS_TABLES` throws strict mode warning
				Query: "set @@sql_mode = '" +
					"NO_ZERO_IN_DATE," +
					"NO_ZERO_DATE," +
					"ERROR_FOR_DIVISION_BY_ZERO'",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       3135,
				ExpectedWarningMessageSubstring: "'NO_ZERO_DATE', 'NO_ZERO_IN_DATE' and 'ERROR_FOR_DIVISION_BY_ZERO' " +
					"sql modes should be used with strict mode. " +
					"They will be merged with strict mode in a future release",
			},
			{
				Query: "select @@sql_mode",
				Expected: []sql.Row{
					{"NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO"},
				},
			},
			{
				// Disabling `NO_ZERO_DATE` throws strict mode warning
				Query: "set @@sql_mode = '" +
					"STRICT_TRANS_TABLES," +
					"NO_ZERO_IN_DATE," +
					"ERROR_FOR_DIVISION_BY_ZERO'",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       3135,
				ExpectedWarningMessageSubstring: "'NO_ZERO_DATE', 'NO_ZERO_IN_DATE' and 'ERROR_FOR_DIVISION_BY_ZERO' " +
					"sql modes should be used with strict mode. " +
					"They will be merged with strict mode in a future release",
			},
			{
				Query: "select @@sql_mode",
				Expected: []sql.Row{
					{"STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO"},
				},
			},
			{
				// Disabling `ERROR_FOR_DIVISION_BY_ZERO` throws strict mode warning
				Query: "set @@sql_mode = '" +
					"STRICT_TRANS_TABLES," +
					"NO_ZERO_IN_DATE," +
					"NO_ZERO_DATE'",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
				ExpectedWarningsCount: 1,
				ExpectedWarning:       3135,
				ExpectedWarningMessageSubstring: "'NO_ZERO_DATE', 'NO_ZERO_IN_DATE' and 'ERROR_FOR_DIVISION_BY_ZERO' " +
					"sql modes should be used with strict mode. " +
					"They will be merged with strict mode in a future release",
			},
			{
				Query: "select @@sql_mode",
				Expected: []sql.Row{
					{"STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE"},
				},
			},
		},
	},

	// Double Tests
	{
		Name:        "double with auto_increment",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "create table double_tbl (d double primary key auto_increment);",
				ExpectedErrStr: "Incorrect column specifier for column 'd'",
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11453
		Name:    "DEFAULT(col) expression",
		Dialect: "mysql", // DEFAULT(col) function is not valid Postgres syntax
		SetUpScript: []string{
			"create table t(pk int primary key, i int default 7, j int, k int generated always as (i + 10), l int not null, m int default null);",
			"insert into t(pk, i, l) values (1, 1, 1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "SELECT DEFAULT(pk) FROM t;",
				ExpectedErr: sql.ErrFieldNoDefaultValue,
			},
			{
				Query:    "SELECT DEFAULT(i) FROM t;",
				Expected: []sql.Row{{7}},
			},
			{
				Query:    "SELECT DEFAULT(i) AS d FROM t;",
				Expected: []sql.Row{{7}},
			},
			{
				Query:    "SELECT DEFAULT(j) FROM t;",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:       "SELECT DEFAULT(k) FROM t;",
				ExpectedErr: sql.ErrFieldNoDefaultValue,
			},
			{
				Query:       "SELECT DEFAULT(l) FROM t;",
				ExpectedErr: sql.ErrFieldNoDefaultValue,
			},
			{
				Query:    "SELECT DEFAULT(m) FROM t;",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:       "SELECT DEFAULT(asdfadf) FROM t;",
				ExpectedErr: sql.ErrColumnNotFound,
			},
		},
	},
	{
		Name: "inserting and updating using default values",
		SetUpScript: []string{
			"create table t1 (i int primary key, j int generated always as (i + 10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t1 (i, j) values (1, default);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Dialect:  "mysql", // This query should work in Doltgres but currently returns the wrong results. https://github.com/dolthub/doltgresql/issues/3203
				Query:    "select * from t1",
				Expected: []sql.Row{{1, 11}},
			},
			{
				Dialect:  "mysql", // This query should work in Doltgres but currently errors out. https://github.com/dolthub/doltgresql/issues/3204
				Query:    "update t1 set j = default where i = 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 1, Updated: 0}}}},
			},
			{
				Query:       "update t1 set i = default where i = 1;",
				ExpectedErr: sql.ErrFieldNoDefaultValue,
			},
		},
	},
}
