package testmysql

import (
	"database/sql"
	"reflect"
	"testing"

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
			`describe table mytable`,
			[][]string{
				{"name", "TEXT"},
				{"email", "TEXT"},
				{"phone_numbers", "JSON"},
				{"created_at", "TIMESTAMP"},
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
