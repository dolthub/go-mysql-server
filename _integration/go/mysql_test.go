package testmysql

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestMySQL(t *testing.T) {
	db, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:3306)/test")
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
