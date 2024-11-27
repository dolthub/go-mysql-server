// Copyright 2023 Dolthub, Inc.
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

var FulltextTests = []ScriptTest{
	{
		Name: "Basic matching 1 PK",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:              "SELECT pk, v1 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi"}},
			},
			{
				Query:              "SELECT v1, v2 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{"ghi", "jkl"}},
			},
			{
				Query:              "SELECT pk, v1, v2 FROM test WHERE MATCH(v2, v1) AGAINST ('jkl');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:              "SELECT pk, v2 FROM test WHERE MATCH(v2, v1) AGAINST ('jkl');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "jkl"}},
			},
			{
				Query:              "SELECT v1 FROM test WHERE MATCH(v2, v1) AGAINST ('jkl');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{"ghi"}},
			},
			{
				Query:              "SELECT v2 FROM test WHERE MATCH(v2, v1) AGAINST ('jkl');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{"jkl"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl') = 0;",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{uint64(1), "abc", "def pqr"}, {uint64(3), "mno", "mno"}, {uint64(4), "stu vwx", "xyz zyx yzx"}, {uint64(5), "ghs", "mno shg"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl') > 0;",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}, {uint64(3), "mno", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno') AND pk = 3;",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(3), "mno", "mno"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno') OR pk = 1;",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{uint64(1), "abc", "def pqr"}, {uint64(2), "ghi", "jkl"}, {uint64(3), "mno", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching 1 UK",
		SetUpScript: []string{
			"CREATE TABLE test (uk BIGINT UNSIGNED NOT NULL UNIQUE, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:              "SELECT uk, v1 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi"}},
			},
			{
				Query:              "SELECT uk, v2, v1 FROM test WHERE MATCH(v2, v1) AGAINST ('jkl');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "jkl", "ghi"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}, {uint64(3), "mno", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching No Keys",
		SetUpScript: []string{
			"CREATE TABLE test (v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES ('abc', 'def pqr'), ('ghi', 'jkl'), ('mno', 'mno'), ('stu vwx', 'xyz zyx yzx'), ('ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"ghi", "jkl"}},
			},
			{
				Query:              "SELECT v1 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"ghi"}},
			},
			{
				Query:              "SELECT v2 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"jkl"}},
			},
			{
				Query:              "SELECT v2, v1 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"jkl", "ghi"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"ghi", "jkl"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"ghi", "jkl"}, {"mno", "mno"}, {"ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching 2 PKs",
		SetUpScript: []string{
			"CREATE TABLE test (pk1 BIGINT UNSIGNED, pk2 BIGINT UNSIGNED, v1 VARCHAR(200), v2 VARCHAR(200), PRIMARY KEY (pk1, pk2), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 1, 'abc', 'def pqr'), (2, 1, 'ghi', 'jkl'), (3, 1, 'mno', 'mno'), (4, 1, 'stu vwx', 'xyz zyx yzx'), (5, 1, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), uint64(1), "ghi", "jkl"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), uint64(1), "ghi", "jkl"}, {uint64(3), uint64(1), "mno", "mno"}, {uint64(5), uint64(1), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching 2 PKs Reversed",
		SetUpScript: []string{
			"CREATE TABLE test (pk1 BIGINT UNSIGNED, pk2 BIGINT UNSIGNED, v1 VARCHAR(200), v2 VARCHAR(200), PRIMARY KEY (pk2, pk1), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 1, 'abc', 'def pqr'), (2, 1, 'ghi', 'jkl'), (3, 1, 'mno', 'mno'), (4, 1, 'stu vwx', 'xyz zyx yzx'), (5, 1, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), uint64(1), "ghi", "jkl"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), uint64(1), "ghi", "jkl"}, {uint64(3), uint64(1), "mno", "mno"}, {uint64(5), uint64(1), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching 2 PKs Non-Sequential",
		SetUpScript: []string{
			"CREATE TABLE test (pk1 BIGINT UNSIGNED, v1 VARCHAR(200), pk2 BIGINT UNSIGNED, v2 VARCHAR(200), PRIMARY KEY (pk2, pk1), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 1, 'def pqr'), (2, 'ghi', 1, 'jkl'), (3, 'mno', 1, 'mno'), (4, 'stu vwx', 1, 'xyz zyx yzx'), (5, 'ghs', 1, 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", uint64(1), "jkl"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", uint64(1), "jkl"}, {uint64(3), "mno", uint64(1), "mno"}, {uint64(5), "ghs", uint64(1), "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching 2 UKs",
		SetUpScript: []string{
			"CREATE TABLE test (uk1 BIGINT UNSIGNED NOT NULL, uk2 BIGINT UNSIGNED NOT NULL, v1 VARCHAR(200), v2 VARCHAR(200), UNIQUE KEY (uk1, uk2), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 1, 'abc', 'def pqr'), (2, 1, 'ghi', 'jkl'), (3, 1, 'mno', 'mno'), (4, 1, 'stu vwx', 'xyz zyx yzx'), (5, 1, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), uint64(1), "ghi", "jkl"}},
			},
			{
				Query:              "SELECT v2, uk2 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{"jkl", uint64(1)}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), uint64(1), "ghi", "jkl"}, {uint64(3), uint64(1), "mno", "mno"}, {uint64(5), uint64(1), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching 2 UKs Reversed",
		SetUpScript: []string{
			"CREATE TABLE test (uk1 BIGINT UNSIGNED NOT NULL, uk2 BIGINT UNSIGNED NOT NULL, v1 VARCHAR(200), v2 VARCHAR(200), UNIQUE KEY (uk2, uk1), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 1, 'abc', 'def pqr'), (2, 1, 'ghi', 'jkl'), (3, 1, 'mno', 'mno'), (4, 1, 'stu vwx', 'xyz zyx yzx'), (5, 1, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), uint64(1), "ghi", "jkl"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), uint64(1), "ghi", "jkl"}, {uint64(3), uint64(1), "mno", "mno"}, {uint64(5), uint64(1), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "Basic matching 2 UKs Non-Sequential",
		SetUpScript: []string{
			"CREATE TABLE test (uk1 BIGINT UNSIGNED NOT NULL, v1 VARCHAR(200), uk2 BIGINT UNSIGNED NOT NULL, v2 VARCHAR(200), UNIQUE KEY (uk1, uk2), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 1, 'def pqr'), (2, 'ghi', 1, 'jkl'), (3, 'mno', 1, 'mno'), (4, 'stu vwx', 1, 'xyz zyx yzx'), (5, 'ghs', 1, 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", uint64(1), "jkl"}},
			},
			{
				Query:              "SELECT v2, uk2 FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{"jkl", uint64(1)}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", uint64(1), "jkl"}, {uint64(3), "mno", uint64(1), "mno"}, {uint64(5), "ghs", uint64(1), "mno shg"}},
			},
		},
	},
	{
		Name: "Basic UPDATE and DELETE checks",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:    "UPDATE test SET v1 = 'rgb' WHERE pk = 2;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('rgb');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "rgb", "jkl"}},
			},
			{
				Query:    "UPDATE test SET v2 = 'mno' WHERE pk = 2;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('mno');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "rgb", "mno"}, {uint64(3), "mno", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
			{
				Query:    "DELETE FROM test WHERE pk = 3;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('mno');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "rgb", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "NULL handling",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', NULL), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, NULL, NULL), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{ // Full-Text handles NULL values by ignoring them, meaning non-null values are still added to the document
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('abc');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(1), "abc", nil}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:    "UPDATE test SET v1 = NULL WHERE pk = 2;",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('jkl');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), nil, "jkl"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST (NULL);",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{},
			},
			{
				Query:              "SELECT pk, v1, v2, MATCH(v1, v2) AGAINST (NULL) FROM test;",
				CheckIndexedAccess: false,
				Expected: []sql.UntypedSqlRow{
					{uint64(1), "abc", nil, float32(0)},
					{uint64(2), nil, "jkl", float32(0)},
					{uint64(3), "mno", "mno", float32(0)},
					{uint64(4), nil, nil, float32(0)},
					{uint64(5), "ghs", "mno shg", float32(0)},
				},
			},
			{
				Query:    "DROP INDEX idx ON test;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE test ADD FULLTEXT INDEX idx (v1, v2);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Collation handling",
		SetUpScript: []string{
			"CREATE TABLE test1 (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200) COLLATE utf8mb4_0900_bin, v2 VARCHAR(200) COLLATE utf8mb4_0900_bin, FULLTEXT idx (v1, v2));",
			"CREATE TABLE test2 (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200) COLLATE utf8mb4_0900_ai_ci, v2 VARCHAR(200) COLLATE utf8mb4_0900_ai_ci, FULLTEXT idx (v1, v2));",
			"INSERT INTO test1 VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
			"INSERT INTO test2 VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test1 WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:              "SELECT * FROM test1 WHERE MATCH(v2, v1) AGAINST ('jkl') = 0;",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{uint64(1), "abc", "def pqr"}, {uint64(3), "mno", "mno"}, {uint64(4), "stu vwx", "xyz zyx yzx"}, {uint64(5), "ghs", "mno shg"}},
			},
			{
				Query:              "SELECT * FROM test1 WHERE MATCH(v2, v1) AGAINST ('jkl mno') AND pk = 3;",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{uint64(3), "mno", "mno"}},
			},
			{
				Query:              "SELECT * FROM test1 WHERE MATCH(v1, v2) AGAINST ('GHI');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{},
			},
			{
				Query:              "SELECT * FROM test1 WHERE MATCH(v2, v1) AGAINST ('JKL') = 0;",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{uint64(1), "abc", "def pqr"}, {uint64(2), "ghi", "jkl"}, {uint64(3), "mno", "mno"}, {uint64(4), "stu vwx", "xyz zyx yzx"}, {uint64(5), "ghs", "mno shg"}},
			},
			{
				Query:              "SELECT * FROM test1 WHERE MATCH(v2, v1) AGAINST ('JKL MNO') AND pk = 3;",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{},
			},
			{
				Query:              "SELECT * FROM test2 WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:              "SELECT * FROM test2 WHERE MATCH(v2, v1) AGAINST ('jkl') = 0;",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{uint64(1), "abc", "def pqr"}, {uint64(3), "mno", "mno"}, {uint64(4), "stu vwx", "xyz zyx yzx"}, {uint64(5), "ghs", "mno shg"}},
			},
			{
				Query:              "SELECT * FROM test2 WHERE MATCH(v2, v1) AGAINST ('jkl mno') AND pk = 3;",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(3), "mno", "mno"}},
			},
			{
				Query:              "SELECT * FROM test2 WHERE MATCH(v1, v2) AGAINST ('GHI');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:              "SELECT * FROM test2 WHERE MATCH(v2, v1) AGAINST ('JKL') = 0;",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{uint64(1), "abc", "def pqr"}, {uint64(3), "mno", "mno"}, {uint64(4), "stu vwx", "xyz zyx yzx"}, {uint64(5), "ghs", "mno shg"}},
			},
			{
				Query:              "SELECT * FROM test2 WHERE MATCH(v2, v1) AGAINST ('JKL MNO') AND pk = 3;",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(3), "mno", "mno"}},
			},
		},
	},
	{ // We should not have many relevancy tests since the values are subject to change if/when the algorithm gets updated
		Name: "Relevancy Ordering",
		SetUpScript: []string{
			"CREATE TABLE test (pk INT PRIMARY KEY, doc TEXT, FULLTEXT idx (doc)) COLLATE=utf8mb4_general_ci;",
			"INSERT INTO test VALUES (2, 'g hhhh aaaab ooooo aaaa'), (1, 'bbbb ff cccc ddd eee'), (4, 'AAAA aaaa aaaac aaaa Aaaa aaaa'), (3, 'aaaA ff j kkkk llllllll');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT MATCH(doc) AGAINST('aaaa') AS relevance FROM test ORDER BY relevance DESC;",
				Expected: []sql.UntypedSqlRow{
					{float32(5.9636202)},
					{float32(4.0278959)},
					{float32(3.3721533)},
					{float32(0)},
				},
			},
			{
				Query: "SELECT MATCH(doc) AGAINST('aaaa') AS relevance, pk FROM test ORDER BY relevance DESC;",
				Expected: []sql.UntypedSqlRow{
					{float32(5.9636202), int32(4)},
					{float32(4.0278959), int32(2)},
					{float32(3.3721533), int32(3)},
					{float32(0), int32(1)},
				},
			},
			{
				Query: "SELECT pk, MATCH(doc) AGAINST('aaaa') AS relevance FROM test ORDER BY relevance ASC;",
				Expected: []sql.UntypedSqlRow{
					{int32(1), float32(0)},
					{int32(3), float32(3.3721533)},
					{int32(2), float32(4.0278959)},
					{int32(4), float32(5.9636202)},
				},
			},
			{
				Query: "SELECT pk, doc, MATCH(doc) AGAINST('aaaa') AS relevance FROM test ORDER BY relevance DESC;",
				Expected: []sql.UntypedSqlRow{
					{int32(4), "AAAA aaaa aaaac aaaa Aaaa aaaa", float32(5.9636202)},
					{int32(2), "g hhhh aaaab ooooo aaaa", float32(4.0278959)},
					{int32(3), "aaaA ff j kkkk llllllll", float32(3.3721533)},
					{int32(1), "bbbb ff cccc ddd eee", float32(0)},
				},
			},
			{
				Query: "SELECT pk, doc, MATCH(doc) AGAINST('aaaa') AS relevance FROM test ORDER BY relevance ASC;",
				Expected: []sql.UntypedSqlRow{
					{int32(1), "bbbb ff cccc ddd eee", float32(0)},
					{int32(3), "aaaA ff j kkkk llllllll", float32(3.3721533)},
					{int32(2), "g hhhh aaaab ooooo aaaa", float32(4.0278959)},
					{int32(4), "AAAA aaaa aaaac aaaa Aaaa aaaa", float32(5.9636202)},
				},
			},
			{
				Query: "SELECT pk FROM test ORDER BY MATCH(doc) AGAINST('aaaa') DESC;",
				Expected: []sql.UntypedSqlRow{
					{int32(4)},
					{int32(2)},
					{int32(3)},
					{int32(1)},
				},
			},
			{
				Query: "SELECT pk, doc FROM test ORDER BY MATCH(doc) AGAINST('aaaa') ASC;",
				Expected: []sql.UntypedSqlRow{
					{int32(1), "bbbb ff cccc ddd eee"},
					{int32(3), "aaaA ff j kkkk llllllll"},
					{int32(2), "g hhhh aaaab ooooo aaaa"},
					{int32(4), "AAAA aaaa aaaac aaaa Aaaa aaaa"},
				},
			},
			{
				Query: "SELECT 1 FROM test ORDER BY MATCH(doc) AGAINST('aaaa') DESC;",
				Expected: []sql.UntypedSqlRow{
					{int32(1)},
					{int32(1)},
					{int32(1)},
					{int32(1)},
				},
			},
			{
				Query: "SELECT pk, MATCH(doc) AGAINST('aaaa') AS relevance FROM test HAVING relevance > 4 ORDER BY relevance DESC;",
				Expected: []sql.UntypedSqlRow{
					{int32(4), float32(5.9636202)},
					{int32(2), float32(4.0278959)},
				},
			},
			{ // Test with an added column to ensure that unnecessary columns do not affect the results
				Query:    "ALTER TABLE test ADD COLUMN extracol INT DEFAULT 7;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SELECT pk FROM test ORDER BY MATCH(doc) AGAINST('aaaa') DESC;",
				Expected: []sql.UntypedSqlRow{
					{int32(4)},
					{int32(2)},
					{int32(3)},
					{int32(1)},
				},
			},
			{ // Drop the primary key to ensure that results are still consistent without a primary key
				Query:    "ALTER TABLE test DROP PRIMARY KEY;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query: "SELECT pk FROM test ORDER BY MATCH(doc) AGAINST('aaaa') ASC;",
				Expected: []sql.UntypedSqlRow{
					{int32(1)},
					{int32(3)},
					{int32(2)},
					{int32(4)},
				},
			},
			{
				Query: "SELECT pk, MATCH(doc) AGAINST('aaaa') AS relevance FROM test ORDER BY relevance DESC;",
				Expected: []sql.UntypedSqlRow{
					{int32(4), float32(5.9636202)},
					{int32(2), float32(4.0278959)},
					{int32(3), float32(3.3721533)},
					{int32(1), float32(0)},
				},
			},
		},
	},
	{
		Name: "CREATE INDEX before insertions",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200));",
			"CREATE FULLTEXT INDEX idx ON test (v1, v2);",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}, {uint64(3), "mno", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "CREATE INDEX after insertions",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
			"CREATE FULLTEXT INDEX idx ON test (v1, v2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}, {uint64(3), "mno", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "ALTER TABLE CREATE INDEX before insertions",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200));",
			"ALTER TABLE test ADD FULLTEXT INDEX idx (v1, v2);",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}, {uint64(3), "mno", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "ALTER TABLE CREATE INDEX after insertions",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
			"ALTER TABLE test ADD FULLTEXT INDEX idx (v1, v2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}, {uint64(3), "mno", "mno"}, {uint64(5), "ghs", "mno shg"}},
			},
		},
	},
	{
		Name: "DROP INDEX",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:    "DROP INDEX idx ON test;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				ExpectedErr:        sql.ErrNoFullTextIndexFound,
			},
		},
	},
	{
		Name: "ALTER TABLE DROP INDEX",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
			"CREATE FULLTEXT INDEX idx ON test (v1, v2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:    "ALTER TABLE test DROP INDEX idx;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				ExpectedErr:        sql.ErrNoFullTextIndexFound,
			},
		},
	},
	{
		Name: "ALTER TABLE ADD COLUMN",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE test ADD COLUMN v3 FLOAT DEFAULT 7 FIRST;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{float32(7), uint64(2), "ghi", "jkl"}},
			},
		},
	},
	{
		Name: "ALTER TABLE MODIFY COLUMN not used by index",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), v3 BIGINT UNSIGNED, FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr', 7), (2, 'ghi', 'jkl', 7), (3, 'mno', 'mno', 7), (4, 'stu vwx', 'xyz zyx yzx', 7), (5, 'ghs', 'mno shg', 7);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE test MODIFY COLUMN v3 FLOAT AFTER pk;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), float32(7), "ghi", "jkl"}},
			},
		},
	},
	{
		Name: "ALTER TABLE MODIFY COLUMN used by index to valid type",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE test MODIFY COLUMN v2 TEXT;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
		},
	},
	{
		Name: "ALTER TABLE MODIFY COLUMN used by index to invalid type",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "ALTER TABLE test MODIFY COLUMN v2 VARBINARY(200);",
				ExpectedErr: sql.ErrFullTextInvalidColumnType,
			},
		},
	},
	{
		Name: "ALTER TABLE DROP COLUMN not used by index",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), v3 BIGINT UNSIGNED, FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr', 7), (2, 'ghi', 'jkl', 7), (3, 'mno', 'mno', 7), (4, 'stu vwx', 'xyz zyx yzx', 7), (5, 'ghs', 'mno shg', 7);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE test DROP COLUMN v3;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
		},
	},
	{
		Name: "ALTER TABLE DROP COLUMN used by index",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), v3 VARCHAR(200), FULLTEXT idx1 (v1, v2), FULLTEXT idx2 (v2), FULLTEXT idx3 (v2, v3));",
			"INSERT INTO test VALUES (1, 'abc', 'def', 'ghi');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('abc');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(1), "abc", "def", "ghi"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2) AGAINST ('def');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(1), "abc", "def", "ghi"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v3) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(1), "abc", "def", "ghi"}},
			},
			{
				Query:    "SHOW CREATE TABLE test;",
				Expected: []sql.UntypedSqlRow{{"test", "CREATE TABLE `test` (\n  `pk` bigint unsigned NOT NULL,\n  `v1` varchar(200),\n  `v2` varchar(200),\n  `v3` varchar(200),\n  PRIMARY KEY (`pk`),\n  FULLTEXT KEY `idx1` (`v1`,`v2`),\n  FULLTEXT KEY `idx2` (`v2`),\n  FULLTEXT KEY `idx3` (`v2`,`v3`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "ALTER TABLE test DROP COLUMN v2;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('abc');",
				CheckIndexedAccess: true,
				ExpectedErr:        sql.ErrColumnNotFound,
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2) AGAINST ('def');",
				CheckIndexedAccess: true,
				ExpectedErr:        sql.ErrColumnNotFound,
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v3) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				ExpectedErr:        sql.ErrColumnNotFound,
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1) AGAINST ('abc');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(1), "abc", "ghi"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v3) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(1), "abc", "ghi"}},
			},
			{
				Query:    "SHOW CREATE TABLE test;",
				Expected: []sql.UntypedSqlRow{{"test", "CREATE TABLE `test` (\n  `pk` bigint unsigned NOT NULL,\n  `v1` varchar(200),\n  `v3` varchar(200),\n  PRIMARY KEY (`pk`),\n  FULLTEXT KEY `idx1` (`v1`),\n  FULLTEXT KEY `idx3` (`v3`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "ALTER TABLE test DROP COLUMN v3;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1) AGAINST ('abc');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(1), "abc"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v3) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				ExpectedErr:        sql.ErrColumnNotFound,
			},
			{
				Query:    "SHOW CREATE TABLE test;",
				Expected: []sql.UntypedSqlRow{{"test", "CREATE TABLE `test` (\n  `pk` bigint unsigned NOT NULL,\n  `v1` varchar(200),\n  PRIMARY KEY (`pk`),\n  FULLTEXT KEY `idx1` (`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "ALTER TABLE ADD PRIMARY KEY",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE test ADD PRIMARY KEY (pk);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
		},
	},
	{
		Name: "ALTER TABLE DROP PRIMARY KEY",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE test DROP PRIMARY KEY;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
		},
	},
	{
		Name: "ALTER TABLE DROP TABLE",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{ // This is mainly to check for a panic
				Query:    "DROP TABLE test;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "TRUNCATE TABLE",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES (1, 'abc', 'def pqr'), (2, 'ghi', 'jkl'), (3, 'mno', 'mno'), (4, 'stu vwx', 'xyz zyx yzx'), (5, 'ghs', 'mno shg');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{{uint64(2), "ghi", "jkl"}},
			},
			{
				Query:    "TRUNCATE TABLE test;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(5)}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: true,
				Expected:           []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "No prefix needed for TEXT columns",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE TABLE `film_text` (`film_id` SMALLINT NOT NULL, `title` VARCHAR(255) NOT NULL, `description` TEXT, PRIMARY KEY (`film_id`), FULLTEXT KEY `idx_title_description` (`title`,`description`));",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "CREATE TABLE other_table (pk BIGINT PRIMARY KEY, v1 TEXT);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "ALTER TABLE other_table ADD FULLTEXT INDEX idx (v1);",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "Rename new table to match old table",
		SetUpScript: []string{
			"CREATE TABLE test1 (v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test1 VALUES ('abc', 'def');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "RENAME TABLE test1 TO test2;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:              "SELECT * FROM test2 WHERE MATCH(v1, v2) AGAINST ('abc');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"abc", "def"}},
			},
			{
				Query:    "CREATE TABLE test1 (v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:    "INSERT INTO test1 VALUES ('ghi', 'jkl');",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:              "SELECT * FROM test1 WHERE MATCH(v1, v2) AGAINST ('abc');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{},
			},
			{
				Query:              "SELECT * FROM test2 WHERE MATCH(v1, v2) AGAINST ('abc');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"abc", "def"}},
			},
			{
				Query:              "SELECT * FROM test1 WHERE MATCH(v1, v2) AGAINST ('jkl');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"ghi", "jkl"}},
			},
			{
				Query:              "SELECT * FROM test2 WHERE MATCH(v1, v2) AGAINST ('jkl');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "Rename index",
		SetUpScript: []string{
			"CREATE TABLE test (v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v2, v1));",
			"INSERT INTO test VALUES ('abc', 'def');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SHOW CREATE TABLE test;",
				Expected: []sql.UntypedSqlRow{{"test", "CREATE TABLE `test` (\n  `v1` varchar(200),\n  `v2` varchar(200),\n  FULLTEXT KEY `idx` (`v2`,`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "ALTER TABLE test RENAME INDEX idx TO new_idx;",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(0)}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('abc');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"abc", "def"}},
			},
			{
				Query:    "SHOW CREATE TABLE test;",
				Expected: []sql.UntypedSqlRow{{"test", "CREATE TABLE `test` (\n  `v1` varchar(200),\n  `v2` varchar(200),\n  FULLTEXT KEY `new_idx` (`v2`,`v1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
		},
	},
	{
		Name: "Multiple overlapping indexes",
		SetUpScript: []string{
			"CREATE TABLE test (v1 TEXT, v2 VARCHAR(200), v3 MEDIUMTEXT, FULLTEXT idx1 (v1, v2), FULLTEXT idx2 (v1, v3), FULLTEXT idx3 (v2, v3));",
			"INSERT INTO test VALUES ('abc', 'def', 'ghi');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('abc');",
				// TODO keyColumns are null type, blocks index access
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"abc", "def", "ghi"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('def');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"abc", "def", "ghi"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v3) AGAINST ('abc');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"abc", "def", "ghi"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v3) AGAINST ('def');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v1, v3) AGAINST ('ghi');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"abc", "def", "ghi"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v3) AGAINST ('abc');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v3) AGAINST ('def');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"abc", "def", "ghi"}},
			},
			{
				Query:              "SELECT * FROM test WHERE MATCH(v2, v3) AGAINST ('ghi');",
				CheckIndexedAccess: false,
				Expected:           []sql.UntypedSqlRow{{"abc", "def", "ghi"}},
			},
		},
	},
	{
		Name: "Duplicate column names",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE test (v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v1));",
				ExpectedErr: sql.ErrDuplicateColumn,
			},
		},
	},
	{
		Name: "References missing column",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE test (v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v3));",
				ExpectedErr: sql.ErrKeyColumnDoesNotExist,
			},
		},
	},
	{
		Name: "Creating an index on an invalid type",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE test (v1 VARCHAR(200), v2 BIGINT, FULLTEXT idx (v1, v2));",
				ExpectedErr: sql.ErrFullTextInvalidColumnType,
			},
		},
	},
	{
		Name: "Foreign keys ignore Full-Text indexes",
		SetUpScript: []string{
			"CREATE TABLE parent (pk BIGINT, v1 VARCHAR(200), FULLTEXT idx (v1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "CREATE TABLE child1 (pk BIGINT, v1 VARCHAR(200), FULLTEXT idx (v1), CONSTRAINT fk FOREIGN KEY (v1) REFERENCES parent(v1));",
				ExpectedErr: sql.ErrForeignKeyMissingReferenceIndex,
			},
			{
				Query:       "CREATE TABLE child2 (pk BIGINT, v1 VARCHAR(200), INDEX idx (v1), CONSTRAINT fk FOREIGN KEY (v1) REFERENCES parent(v1));",
				ExpectedErr: sql.ErrForeignKeyMissingReferenceIndex,
			},
		},
	},
	{
		Name: "Full-Text with autoincrement",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, v1 VARCHAR(200), PRIMARY KEY(pk), FULLTEXT idx (v1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO test (v1) VALUES ('abc'), ('def');",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 2, InsertID: 1}}},
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.UntypedSqlRow{{uint64(1), "abc"}, {uint64(2), "def"}},
			},
		},
	},
	{
		Name: "Full-Text with default columns",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED NOT NULL DEFAULT '1', v1 VARCHAR(200) DEFAULT 'def', PRIMARY KEY(pk), FULLTEXT idx (v1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO test (v1) VALUES ('abc');",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "INSERT INTO test (pk, v1) VALUES (2, 'def');",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.UntypedSqlRow{{uint64(1), "abc"}, {uint64(2), "def"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1) AGAINST ('def');",
				Expected: []sql.UntypedSqlRow{{uint64(2), "def"}},
			},
		},
	},
	{
		Name: "large tokens",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT UNSIGNED NOT NULL DEFAULT '1', v1 VARCHAR(200) DEFAULT 'def', PRIMARY KEY(pk), FULLTEXT idx (v1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO test (v1) VALUES ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1) AGAINST ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1) AGAINST ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "REPLACE INTO test (v1) VALUES ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');",
				Expected: []sql.UntypedSqlRow{{types.OkResult{RowsAffected: 2}}},
			},
			{
				Query:    "SELECT * FROM test;",
				Expected: []sql.UntypedSqlRow{{uint64(1), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1) AGAINST ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1) AGAINST ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
}
