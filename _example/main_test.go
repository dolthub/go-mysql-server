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

package main

import (
	"database/sql"
	"fmt"
	"net"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var expectedResults = [][]string{
	{"Jane Deo", "janedeo@gmail.com", `["556-565-566", "777-777-777"]`, "2022-11-01 12:00:00.000001"},
	{"Jane Doe", "jane@doe.com", `[]`, "2022-11-01 12:00:00.000001"},
	{"John Doe", "john@doe.com", `["555-555-555"]`, "2022-11-01 12:00:00.000001"},
	{"John Doe", "johnalt@doe.com", `[]`, "2022-11-01 12:00:00.000001"},
}

func TestExampleUsersDisabled(t *testing.T) {
	enableUsers = false
	useUnusedPort(t)
	go func() {
		main()
	}()

	conn, err := dbr.Open("mysql", fmt.Sprintf("no_user:@tcp(%s:%d)/%s", address, port, dbName), nil)
	require.NoError(t, err)
	require.NoError(t, conn.Ping())

	rows, err := conn.Query(fmt.Sprintf("SELECT * FROM %s;", tableName))
	require.NoError(t, err)
	checkRows(t, expectedResults, rows)
	require.NoError(t, conn.Close())
}

func TestExampleRootUserEnabled(t *testing.T) {
	enableUsers = true
	pretendThatFileExists = false
	useUnusedPort(t)
	go func() {
		main()
	}()

	conn, err := dbr.Open("mysql", fmt.Sprintf("no_user:@tcp(%s:%d)/%s", address, port, dbName), nil)
	require.NoError(t, err)
	require.ErrorContains(t, conn.Ping(), "User not found")
	conn, err = dbr.Open("mysql", fmt.Sprintf("root:@tcp(%s:%d)/%s", address, port, dbName), nil)
	require.NoError(t, err)
	require.NoError(t, conn.Ping())

	rows, err := conn.Query(fmt.Sprintf("SELECT * FROM %s;", tableName))
	require.NoError(t, err)
	checkRows(t, expectedResults, rows)
	require.NoError(t, conn.Close())
}

func TestExampleLoadedUser(t *testing.T) {
	enableUsers = true
	pretendThatFileExists = true
	useUnusedPort(t)
	go func() {
		main()
	}()

	conn, err := dbr.Open("mysql", fmt.Sprintf("no_user:@tcp(%s:%d)/%s", address, port, dbName), nil)
	require.NoError(t, err)
	require.ErrorContains(t, conn.Ping(), "User not found")
	conn, err = dbr.Open("mysql", fmt.Sprintf("root:@tcp(%s:%d)/%s", address, port, dbName), nil)
	require.NoError(t, err)
	require.ErrorContains(t, conn.Ping(), "User not found")
	conn, err = dbr.Open("mysql",
		fmt.Sprintf("gms_user:123456@tcp(%s:%d)/%s?allowCleartextPasswords=true", address, port, dbName), nil)
	require.NoError(t, err)
	require.NoError(t, conn.Ping())

	rows, err := conn.Query(fmt.Sprintf("SELECT * FROM %s;", tableName))
	require.NoError(t, err)
	checkRows(t, expectedResults, rows)
	require.NoError(t, conn.Close())
}

func TestIssue1621(t *testing.T) {
	// This is an issue that is specific to using the example server, as this is not a logic issue but a setup issue
	enableUsers = true
	useUnusedPort(t)
	go func() {
		main()
	}()

	conn, err := dbr.Open("mysql",
		fmt.Sprintf("root:@tcp(localhost:%d)/mydb", port), nil)
	require.NoError(t, err)
	require.NoError(t, conn.Ping())

	rows, err := conn.Query("CREATE TABLE `users` (`id` int(10) unsigned NOT NULL, `name` varchar(100) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
	require.NoError(t, err)
	require.NoError(t, rows.Close())
	rows, err = conn.Query("CREATE TABLE `managers` (`id` int(10) unsigned NOT NULL, `user_id` int(10) unsigned NOT NULL, PRIMARY KEY (`id`), FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
	require.NoError(t, err)
	require.NoError(t, rows.Close())
	require.NoError(t, conn.Close())
}

func checkRows(t *testing.T, expectedRows [][]string, actualRows *sql.Rows) {
	rowIdx := -1
	for actualRows.Next() {
		rowIdx++

		if assert.Less(t, rowIdx, len(expectedRows)) {
			compareRow := make([]string, len(expectedRows[rowIdx]))
			connRow := make([]*string, len(compareRow))
			interfaceRow := make([]any, len(compareRow))
			for i := range connRow {
				interfaceRow[i] = &connRow[i]
			}
			assert.NoError(t, actualRows.Scan(interfaceRow...))
			for i := range connRow {
				if assert.NotNil(t, connRow[i]) {
					compareRow[i] = *connRow[i]
				}
			}
			assert.Equal(t, expectedRows[rowIdx], compareRow)
		}
	}
	assert.NoError(t, actualRows.Close())
}

func useUnusedPort(t *testing.T) {
	// Tests should grab an open port, otherwise they'll fail if some hardcoded port is already in use
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	port = listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
}
