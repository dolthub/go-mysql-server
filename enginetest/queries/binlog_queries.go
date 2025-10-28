// Copyright 2025 Dolthub, Inc.
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
	"os"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var (
	binlogInsertStmts         = readBinlogTestFile("./testdata/binlog_insert.txt")
	binlogUpdateStmts         = readBinlogTestFile("./testdata/binlog_update.txt")
	binlogDeleteStmts         = readBinlogTestFile("./testdata/binlog_delete.txt")
	binlogFormatDescStmts     = readBinlogTestFile("./testdata/binlog_format_desc.txt")
	binlogTransactionMultiOps = readBinlogTestFile("./testdata/binlog_transaction_multi_ops.txt")
	binlogNoFormatDescStmts   = readBinlogTestFile("./testdata/binlog_no_format_desc.txt")
)

// BinlogScripts contains test cases for the BINLOG statement. To add tests: add a @test to binlog_maker.bats, generate
// the .txt file with BINLOG statements, then add a test case here with the corresponding setup.
var BinlogScripts = []ScriptTest{
	{
		Name: "SET sql_mode with numeric bitmask from binlog",
		Assertions: []ScriptTestAssertion{
			{Query: "SET @@session.sql_mode=1411383296", Expected: []sql.Row{{types.OkResult{}}}},
			{Query: "SELECT @@session.sql_mode", Expected: []sql.Row{{"ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,STRICT_TRANS_TABLES"}}},
		},
	},
	{
		Name: "SET collation variables with numeric IDs from binlog",
		Assertions: []ScriptTestAssertion{
			{Query: "SET @@session.collation_connection=33", Expected: []sql.Row{{types.OkResult{}}}},
			{Query: "SELECT @@session.collation_connection", Expected: []sql.Row{{"utf8mb3_general_ci"}}},
			{Query: "SELECT @@session.character_set_connection", Expected: []sql.Row{{"utf8mb3"}}},
			{Query: "SET @@session.collation_server=8", Expected: []sql.Row{{types.OkResult{}}}},
			{Query: "SELECT @@session.collation_server", Expected: []sql.Row{{"latin1_swedish_ci"}}},
			{Query: "SELECT @@session.character_set_server", Expected: []sql.Row{{"latin1"}}},
			// collation_database always returns the current database's collation. See sql/core.go:729-735
			{Query: "SET @@session.collation_database=33", Expected: []sql.Row{{types.OkResult{}}}},
			{Query: "SELECT @@session.collation_database", Expected: []sql.Row{{"utf8mb4_0900_bin"}}},
			// TODO: lc_time_names no-op
			{Query: "SET @@session.lc_time_names=0", Expected: []sql.Row{{types.OkResult{}}}},
			{Query: "SELECT @@session.lc_time_names", Expected: []sql.Row{{"0"}}},
		},
	},
	{
		Name: "BINLOG requires FORMAT_DESCRIPTION_EVENT first",
		SetUpScript: []string{
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100))",
		},
		Assertions: []ScriptTestAssertion{
			{Query: binlogNoFormatDescStmts[0], ExpectedErr: sql.ErrNoFormatDescriptionEventBeforeBinlogStatement},
		},
	},
	{
		Name: "BINLOG with simple INSERT",
		SetUpScript: []string{
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100))",
		},
		Assertions: []ScriptTestAssertion{
			{Query: binlogInsertStmts[0], Expected: []sql.Row{}},
			{Query: binlogInsertStmts[1], Expected: []sql.Row{}},
			{Query: binlogInsertStmts[2], Expected: []sql.Row{}},
			{
				Query: "SELECT * FROM users ORDER BY id",
				Expected: []sql.Row{
					{1, "Alice", "alice@example.com"},
					{2, "Bob", "bob@example.com"},
				},
			},
		},
	},
	{
		Name: "BINLOG with UPDATE",
		SetUpScript: []string{
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
		},
		Assertions: []ScriptTestAssertion{
			{Query: binlogUpdateStmts[0], Expected: []sql.Row{}},
			{Query: binlogUpdateStmts[1], Expected: []sql.Row{}},
			{
				Query: "SELECT name FROM users WHERE id = 1",
				Expected: []sql.Row{
					{"Alice Smith"},
				},
			},
		},
	},
	{
		Name: "BINLOG with DELETE",
		SetUpScript: []string{
			"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), email VARCHAR(100))",
			"INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			"INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
		},
		Assertions: []ScriptTestAssertion{
			{Query: binlogDeleteStmts[0], Expected: []sql.Row{}},
			{Query: binlogDeleteStmts[1], Expected: []sql.Row{}},
			{
				Query: "SELECT COUNT(*) FROM users",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "SELECT id FROM users",
				Expected: []sql.Row{
					{1},
				},
			},
		},
	},
	{
		Name: "BINLOG with FORMAT_DESCRIPTION only",
		Assertions: []ScriptTestAssertion{
			{Query: binlogFormatDescStmts[0], Expected: []sql.Row{}},
		},
	},
	{
		Name: "BINLOG transaction with multiple INSERT UPDATE DELETE",
		SetUpScript: []string{
			"CREATE TABLE multi_op_test (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100), value DECIMAL(10,2), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
		},
		Assertions: []ScriptTestAssertion{
			{Query: binlogTransactionMultiOps[0], Expected: []sql.Row{}},
			{Query: binlogTransactionMultiOps[1], Expected: []sql.Row{}},
			{Query: binlogTransactionMultiOps[2], Expected: []sql.Row{}},
			{Query: binlogTransactionMultiOps[3], Expected: []sql.Row{}},
			{Query: binlogTransactionMultiOps[4], Expected: []sql.Row{}},
			{Query: binlogTransactionMultiOps[5], Expected: []sql.Row{}},
			{
				Query: "SELECT COUNT(*) FROM multi_op_test",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "SELECT value FROM multi_op_test WHERE id = 1",
				Expected: []sql.Row{
					{"109.99"},
				},
			},
		},
	},
	{
		Name: "BINLOG with invalid base64",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "BINLOG 'not-valid-base64!!!'",
				ExpectedErr: sql.ErrBase64DecodeError,
			},
		},
	},
}

// readBinlogTestFile reads BINLOG statements from a testdata file. The file is pre-filtered by binlog_maker.bats to
// contain only BINLOG statements.
func readBinlogTestFile(path string) []string {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}

	content := strings.TrimSpace(string(data))
	if content == "" {
		return nil
	}

	parts := strings.Split(content, "BINLOG '")
	var stmts []string
	for i, part := range parts {
		if i == 0 && part == "" {
			continue
		}
		stmts = append(stmts, "BINLOG '"+part)
	}
	return stmts
}
