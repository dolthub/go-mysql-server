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
	"math"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// SessionScriptTests contains self-contained session script tests.
var SessionScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/9865
		Name:    "Stored procedure containing a transaction does not return EOF",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT)",
			`CREATE PROCEDURE my_proc()
BEGIN
    START TRANSACTION;
    INSERT INTO test_table VALUES (1, 'test');
    COMMIT;
END`,
			`CREATE PROCEDURE empty_procedure()
BEGIN
END`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CALL my_proc()",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, InsertID: 0, Info: nil}}},
			},
			{
				Query:    "SELECT * FROM test_table",
				Expected: []sql.Row{{1, "test"}},
			},
			{
				Query:    "CALL empty_procedure()",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, InsertID: 0, Info: nil}}},
			},
		},
	},
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
		Name:    "Test cases on select into statement",
		Dialect: "mysql",
		SetUpScript: []string{
			"SELECT * FROM (VALUES ROW(22,44,88)) AS t INTO @x,@y,@z",
			"CREATE TABLE tab1 (id int primary key, v1 int)",
			"INSERT INTO tab1 VALUES (1, 1), (2, 3), (3, 6)",
			"SELECT id FROM tab1 ORDER BY id DESC LIMIT 1 INTO @myVar",
			"CREATE TABLE tab2 (i2 int primary key, s text)",
			"INSERT INTO tab2 VALUES (1, 'b'), (2, 'm'), (3, 'g')",
			"SELECT m.id, t.s FROM tab1 m JOIN tab2 t on m.id = t.i2 ORDER BY t.s DESC LIMIT 1 INTO @myId, @myText",
			// TODO: union statement does not handle order by and limit clauses
			// "SELECT id FROM tab1 UNION select s FROM tab2 LIMIT 1 INTO @myUnion",
			"SELECT id FROM tab1 WHERE id > 3 UNION select s FROM tab2 WHERE s < 'f' INTO @mustSingleVar",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:                         `SELECT 1 INTO @abc`,
				SkipResultCheckOnServerEngine: true,
				Expected:                      []sql.Row{{types.NewOkResult(1)}},
				ExpectedColumns:               nil,
			},
			{
				Query:    `SELECT @abc`,
				Expected: []sql.Row{{int8(1)}},
			},
			{
				Query:    `SELECT @z, @x, @y`,
				Expected: []sql.Row{{88, 22, 44}},
			},
			{
				Query:    `SELECT @myVar, @mustSingleVar`,
				Expected: []sql.Row{{3, "b"}},
			},
			{
				Query:    `SELECT @myId, @myText, @myUnion`,
				Expected: []sql.Row{{2, "m", nil}},
			},
			{
				Query:       `SELECT id FROM tab1 ORDER BY id DESC INTO @myvar`,
				ExpectedErr: sql.ErrMoreThanOneRow,
			},
			{
				Query:       `SELECT id INTO DUMPFILE 'baddump.out' FROM tab1 ORDER BY id DESC LIMIT 15`,
				ExpectedErr: sql.ErrMoreThanOneRow,
			},
			{
				Query:       `select 1, 2, 3 into @my1, @my2`,
				ExpectedErr: sql.ErrColumnNumberDoesNotMatch,
			},
			{
				Query:          `SELECT id, v1 INTO @myFirstVar FROM tab1 ORDER BY id DESC LIMIT 1 INTO @mySecondVar`,
				ExpectedErrStr: "Multiple INTO clauses in one query block at position 84 near '@mySecondVar'",
			},
			{
				Query:          `SELECT id FROM tab1 WHERE id > 3 UNION select s INTO @mustSingleVar FROM tab2 WHERE s < 'f' ORDER BY s DESC`,
				ExpectedErrStr: "INTO clause is not allowed at position 98 near 'ORDER'",
			},
		},
	},
	{
		// All DECLARE statements are only allowed under BEGIN/END blocks
		Name: "Top-level DECLARE statements",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "DECLARE no_such_table CONDITION FOR SQLSTATE '42S02'",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:       "DECLARE no_such_table CONDITION FOR 1051",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:       "DECLARE a CHAR(16)",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:       "DECLARE cur2 CURSOR FOR SELECT i FROM test.t2",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:       "DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE",
				ExpectedErr: sql.ErrSyntaxError,
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
		Name:    "row_count() behavior",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table b (x int primary key)",
			"insert into b values (1), (2), (3), (4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "replace into b values (1)",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{-1}},
			},
			{
				Query:    "select count(*) from b",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{-1}},
			},
			{
				Query: "update b set x = x + 10 where x <> 2",
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 3,
					Info: plan.UpdateInfo{
						Matched: 3,
						Updated: 3,
					},
				}}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{-1}},
			},
			{
				Query:    "delete from b where x <> 2",
				Expected: []sql.Row{{types.NewOkResult(3)}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{-1}},
			},
			{
				Query:    "alter table b add column y int null",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{0}},
			},
			{
				Query:    "select row_count()",
				Expected: []sql.Row{{-1}},
			},
		},
	},
	{
		Name:    "found_rows() behavior",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table b (x int primary key)",
			"insert into b values (1), (2), (3), (4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from b where x < 2",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select * from b",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select * from b order by x  limit 3",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select * from b order by x limit 100",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select sql_calc_found_rows * from b order by x limit 3",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select sql_calc_found_rows * from b where x <= 2 order by x limit 1",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "select sql_calc_found_rows * from b where x <= 2 order by x limit 1",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "insert into b values (10), (11), (12), (13)",
				Expected: []sql.Row{{types.NewOkResult(4)}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "update b set x = x where x < 40",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, InsertID: 0, Info: plan.UpdateInfo{Matched: 8}}}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{8}},
			},
			{
				Query:    "update b set x = x where x > 10",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 0, InsertID: 0, Info: plan.UpdateInfo{Matched: 3}}}},
			},
			{
				Query:    "select found_rows()",
				Expected: []sql.Row{{3}},
			},
		},
	},
	{
		Name: "validate_password_strength and validate_password.length",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.length",
			"set @@global.validate_password.length = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('')",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select validate_password_strength('123')",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select validate_password_strength('1234')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.length = 1000",
			},
			{
				Query: "select validate_password_strength('ABCabc123!@!#')",
				Expected: []sql.Row{
					{25},
				},
			},
			{
				Query:          "set @@session.validate_password.length = 123",
				ExpectedErrStr: "Variable 'validate_password.length' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.length = @orig",
			},
		},
	},
	{
		Name: "validate_password_strength and validate_password.number_count",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.number_count",
			"set @@global.validate_password.number_count = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('ABCabc!@#')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.number_count = 1000",
			},
			{
				Query: "select validate_password_strength('ABCabc!!!!123456789012345678901234567890')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				Query:          "set @@session.validate_password.number_count = 123",
				ExpectedErrStr: "Variable 'validate_password.number_count' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.number_count = @orig",
			},
		},
	},
	{
		Name: "validate_password_strength and validate_password.mixed_case_count",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.mixed_case_count",
			"set @@global.validate_password.mixed_case_count = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('abcabc!@#123')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				Query: "select validate_password_strength('ABCABC!@#123')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.mixed_case_count = 1000",
			},
			{
				Query: "select validate_password_strength('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456!?!?!?')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				Query:          "set @@session.validate_password.mixed_case_count = 123",
				ExpectedErrStr: "Variable 'validate_password.mixed_case_count' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.mixed_case_count = @orig",
			},
		},
	},
	{
		Name: "validate_password_strength and validate_password.special_char_count",
		SetUpScript: []string{
			"set @orig = @@global.validate_password.special_char_count",
			"set @@global.validate_password.special_char_count = 0",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select validate_password_strength('abcABC123')",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.special_char_count = 1000",
			},
			{
				Query: "select validate_password_strength('abcABC123!@#$%^&*()                            ')",
				Expected: []sql.Row{
					{50},
				},
			},
			{
				Query:          "set @@session.validate_password.special_char_count = 123",
				ExpectedErrStr: "Variable 'validate_password.special_char_count' is a GLOBAL variable and should be set with SET GLOBAL",
			},
			{
				SkipResultsCheck: true,
				Query:            "set @@global.validate_password.special_char_count = @orig",
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
}
