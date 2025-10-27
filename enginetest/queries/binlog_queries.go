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
)

var (
	binlogInsertStmts         = readBinlogTestFile("./testdata/binlog_insert.txt")
	binlogUpdateStmts         = readBinlogTestFile("./testdata/binlog_update.txt")
	binlogDeleteStmts         = readBinlogTestFile("./testdata/binlog_delete.txt")
	binlogFormatDescStmts     = readBinlogTestFile("./testdata/binlog_format_desc.txt")
	binlogTransactionMultiOps = readBinlogTestFile("./testdata/binlog_transaction_multi_ops.txt")
	binlogNoFormatDescStmts   = readBinlogTestFile("./testdata/binlog_no_format_desc.txt")
)

// BinlogScripts contains test cases for the BINLOG statement. Test data is generated from real MariaDB binlog events by
// running `bats binlog_maker.bats` in enginetest/testdata.
// To add tests: add a @test to binlog_maker.bats, generate the .dat file, then add a test case here.
var BinlogScripts = []ScriptTest{
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

	return strings.Split(content, "BINLOG '")
}
