// Copyright 2020-2021 Dolthub, Inc.
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

package testmysql

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"
)

const connectionString = "root:@tcp(127.0.0.1:3306)/mydb"

func TestMySQL(t *testing.T) {
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		t.Fatalf("can't connect to mysql: %s", err)
	}

	rs, err := db.Query("SELECT name, email FROM mytable ORDER BY name, email")
	if err != nil {
		t.Fatalf("unable to get rows: %s", err)
	}

	var rows [][2]string
	for rs.Next() {
		var row [2]string
		if err := rs.Scan(&row[0], &row[1]); err != nil {
			t.Errorf("got error scanning row: %s", err)
		}

		rows = append(rows, row)
	}

	if err := rs.Err(); err != nil {
		t.Errorf("got unexpected error: %s", err)
	}

	expected := [][2]string{
		{"Evil Bob", "evilbob@gmail.com"},
		{"Jane Doe", "jane@doe.com"},
		{"John Doe", "john@doe.com"},
		{"John Doe", "johnalt@doe.com"},
	}

	if len(expected) != len(rows) {
		t.Errorf("got %d rows, expecting %d", len(rows), len(expected))
	}

	for i := range rows {
		if rows[i][0] != expected[i][0] || rows[i][1] != expected[i][1] {
			t.Errorf(
				"incorrect row %d, got: {%s, %s}, expected: {%s, %s}",
				i,
				rows[i][0], rows[i][1],
				expected[i][0], expected[i][1],
			)
		}
	}
}

func TestGrafana(t *testing.T) {
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		t.Fatalf("can't connect to mysql: %s", err)
	}

	tests := []struct {
		query    string
		expected [][]string
	}{
		{
			`SELECT 1`,
			[][]string{{"1"}},
		},
		{
			`select @@version_comment limit 1`,
			[][]string{{""}},
		},
		{
			`describe mytable`,
			[][]string{
				{"name", "text", "NO", "", "", ""},
				{"email", "text", "NO", "", "", ""},
				{"phone_numbers", "json", "NO", "", "", ""},
				{"created_at", "timestamp", "NO", "", "", ""},
			},
		},
		{
			`select count(*) from mytable where created_at ` +
				`between '2000-01-01T00:00:00Z' and '2999-01-01T00:00:00Z'`,
			[][]string{{"4"}},
		},
	}

	for _, c := range tests {
		rs, err := db.Query(c.query)
		if err != nil {
			t.Fatalf("unable to execute query: %s", err)
		}

		result := getResult(t, rs)

		if !reflect.DeepEqual(result, c.expected) {
			t.Fatalf("rows do not match, expected: %v, got: %v", c.expected, result)
		}
	}
}

func TestMySQLStreaming(t *testing.T) {
	conn, err := client.Connect("127.0.0.1:3306", "root", "", "mydb")
	if err != nil {
		t.Fatalf("can't connect to mysql: %s", err)
	}
	defer func() {
		err = conn.Close()
		if err != nil {
			t.Fatalf("error closing mysql connection: %s", err)
		}
	}()

	var result mysql.Result
	var rows [][2]string
	err = conn.ExecuteSelectStreaming("SELECT name, email FROM mytable ORDER BY name, email", &result, func(row []mysql.FieldValue) error {
		if len(row) != 2 {
			t.Fatalf("expected 2 columns, got %d", len(row))
		}
		rows = append(rows, [2]string{row[0].String(), row[1].String()})
		return nil
	}, nil)

	expected := [][2]string{
		{"Evil Bob", "evilbob@gmail.com"},
		{"Jane Doe", "jane@doe.com"},
		{"John Doe", "john@doe.com"},
		{"John Doe", "johnalt@doe.com"},
	}

	if len(expected) != len(rows) {
		t.Errorf("got %d rows, expecting %d", len(rows), len(expected))
	}

	for i := range rows {
		if rows[i][0] != expected[i][0] || rows[i][1] != expected[i][1] {
			t.Errorf(
				"incorrect row %d, got: {%s, %s}, expected: {%s, %s}",
				i,
				rows[i][0], rows[i][1],
				expected[i][0], expected[i][1],
			)
		}
	}
}

func TestMySQLStreamingPrepared(t *testing.T) {
	conn, err := client.Connect("127.0.0.1:3306", "root", "", "mydb")
	if err != nil {
		t.Fatalf("can't connect to mysql: %s", err)
	}
	defer func() {
		err = conn.Close()
		if err != nil {
			t.Fatalf("error closing mysql connection: %s", err)
		}
	}()

	stmt, err := conn.Prepare("SELECT name, email, ? FROM mytable ORDER BY name, email")
	if err != nil {
		t.Fatalf("error preparing statement: %s", err)
	}

	var result mysql.Result
	var rows [][3]string
	err = stmt.ExecuteSelectStreaming(&result, func(row []mysql.FieldValue) error {
		if len(row) != 3 {
			t.Fatalf("expected 3 columns, got %d", len(row))
		}
		rows = append(rows, [3]string{row[0].String(), row[1].String(), row[2].String()})
		return nil
	}, nil, "abc")

	expected := [][3]string{
		{"Evil Bob", "evilbob@gmail.com", "abc"},
		{"Jane Doe", "jane@doe.com", "abc"},
		{"John Doe", "john@doe.com", "abc"},
		{"John Doe", "johnalt@doe.com", "abc"},
	}

	if len(expected) != len(rows) {
		t.Errorf("got %d rows, expecting %d", len(rows), len(expected))
	}

	for i := range rows {
		if rows[i][0] != expected[i][0] || rows[i][1] != expected[i][1] || rows[i][2] != expected[i][2] {
			t.Errorf(
				"incorrect row %d, got: {%s, %s, %s}, expected: {%s, %s, %s}",
				i,
				rows[i][0], rows[i][1], rows[i][2],
				expected[i][0], expected[i][1], expected[i][2],
			)
		}
	}
}

func getResult(t *testing.T, rs *sql.Rows) [][]string {
	t.Helper()

	columns, err := rs.Columns()
	if err != nil {
		t.Fatalf("unable to get columns: %s", err)
	}

	var result [][]string
	p := make([]interface{}, len(columns))

	for rs.Next() {
		row := make([]interface{}, len(columns))
		for i := range row {
			p[i] = &row[i]
		}

		err = rs.Scan(p...)
		if err != nil {
			t.Fatalf("could not retrieve row: %s", err)
		}

		result = append(result, getStringSlice(row))
	}

	return result
}

func getStringSlice(row []interface{}) []string {
	rowStrings := make([]string, len(row))
	for i, r := range row {
		if r == nil {
			rowStrings[i] = "NULL"
		} else {
			rowStrings[i] = string(r.([]uint8))
		}
	}

	return rowStrings
}
