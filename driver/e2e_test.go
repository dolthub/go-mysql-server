package driver_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestQuery(t *testing.T) {
	mtb, records := personMemTable("db", "person")
	db := sqlOpen(t, mtb, t.Name()+"?jsonAs=object")
	now := time.Now()

	var id uint64
	var name, email string
	var numbers []any
	var created time.Time
	var count int
	var blob []byte

	cases := []struct {
		Name, Query string
		Args        []any
		Pointers    Pointers
		Expect      Records
	}{
		{"Select All", "SELECT * FROM db.person", nil, []any{&id, &name, &email, &numbers, &created}, records},
		{"Select First", "SELECT * FROM db.person LIMIT 1", nil, []any{&id, &name, &email, &numbers, &created}, records.Rows(0)},
		{"Select Name", "SELECT name FROM db.person", nil, []any{&name}, records.Columns(1)},
		{"Select Count", "SELECT COUNT(1) FROM db.person", nil, []any{&count}, Records{{len(records)}}},

		{"Insert", `INSERT INTO db.person (name, email, phone_numbers, created_at) VALUES ('foo', 'bar', '["baz"]', NOW())`, nil, []any{}, Records{}},
		{"Select Inserted", "SELECT name, email, phone_numbers FROM db.person WHERE name = 'foo'", nil, []any{&name, &email, &numbers}, Records{{"foo", "bar", []any{"baz"}}}},

		{"Update", "UPDATE db.person SET name = 'asdf' WHERE name = 'foo'", nil, []any{}, Records{}},
		{"Delete", "DELETE FROM db.person WHERE name = 'asdf'", nil, []any{}, Records{}},

		{"Select Binary Args", `SELECT ?`, []any{[]byte{1, 2, 3}}, []any{&blob}, Records{{[]byte{1, 2, 3}}}},
		{"Insert With time.Time", `INSERT INTO db.person (name, email, phone_numbers, created_at) VALUES ('foo', 'bar', '["baz"]', ?)`, []any{now}, []any{}, Records{}},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			rows, err := db.Query(c.Query, c.Args...)
			require.NoError(t, err, "Query")

			var i int
			for ; rows.Next(); i++ {
				require.NoError(t, rows.Scan(c.Pointers...), "Scan")
				values := c.Pointers.Values()

				if i >= len(c.Expect) {
					t.Errorf("Got row %d, expected %d total: %v", i+1, len(c.Expect), values)
					continue
				}

				expect := c.Expect[i]
				if !assert.Equal(t, len(expect), len(values)) {
					continue
				}
				for i := range expect {
					expect := expect[i]
					actual := values[i]
					if jv, ok := expect.(types.JSONDocument); ok {
						expect = jv.Val
					}
					assert.Equal(t, expect, actual, "Values")
				}
			}

			require.NoError(t, rows.Err(), "Rows.Err")

			if i < len(c.Expect) {
				t.Errorf("Expected %d row(s), got %d", len(c.Expect), i)
			}
		})
	}
}

func TestExec(t *testing.T) {
	mtb, records := personMemTable("db", "person")
	db := sqlOpen(t, mtb, t.Name())

	cases := []struct {
		Name, Statement string
		RowsAffected    int
	}{
		{"Insert", `INSERT INTO db.person (name, email, phone_numbers, created_at) VALUES ('asdf', 'qwer', '["zxcv"]', NOW())`, 1},
		{"Update", "UPDATE db.person SET name = 'foo' WHERE name = 'asdf'", 1},
		{"Delete", "DELETE FROM db.person WHERE name = 'foo'", 1},
		{"Delete All", "DELETE FROM db.person WHERE LENGTH(name) < 100", len(records)},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			res, err := db.Exec(c.Statement)
			require.NoError(t, err, "Exec")

			count, err := res.RowsAffected()
			require.NoError(t, err, "RowsAffected")
			assert.EqualValues(t, c.RowsAffected, count, "RowsAffected")
		})
	}

	errCases := []struct {
		Name, Statement string
		Error           string
	}{
		{"Select", "SELECT * FROM db.person", "no result"},
	}

	for _, c := range errCases {
		t.Run(c.Name, func(t *testing.T) {
			res, err := db.Exec(c.Statement)
			require.NoError(t, err, "Exec")

			_, err = res.RowsAffected()
			require.Error(t, err, "RowsAffected")
			assert.Equal(t, c.Error, err.Error())
		})
	}
}
