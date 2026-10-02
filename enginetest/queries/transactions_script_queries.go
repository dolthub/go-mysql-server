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
)

// TransactionsScriptTests contains self-contained transactions script tests.
var TransactionsScriptTests = []ScriptTest{
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
}
