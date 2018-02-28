package sqle_test

import (
	"io"
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"github.com/stretchr/testify/require"
)

const (
	driverName = "engine_tests"
)

func TestQueries(t *testing.T) {
	e := newEngine(t)

	testQuery(t, e,
		"SELECT i FROM mytable;",
		[][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE i = 2;",
		[][]interface{}{{int64(2)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable ORDER BY i DESC;",
		[][]interface{}{{int64(3)}, {int64(2)}, {int64(1)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC;",
		[][]interface{}{{int64(1)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC LIMIT 1;",
		[][]interface{}{{int64(1)}},
	)

	testQuery(t, e,
		"SELECT COUNT(*) FROM mytable;",
		[][]interface{}{{int32(3)}},
	)

	testQuery(t, e,
		"SELECT COUNT(*) FROM mytable LIMIT 1;",
		[][]interface{}{{int32(3)}},
	)

	testQuery(t, e,
		"SELECT COUNT(*) AS c FROM mytable;",
		[][]interface{}{{int32(3)}},
	)

	testQuery(t, e,
		"SELECT substring(s, 2, 3) FROM mytable",
		[][]interface{}{{"irs"}, {"eco"}, {"hir"}},
	)

	testQuery(t, e,
		"SELECT YEAR('2007-12-11') FROM mytable",
		[][]interface{}{{int32(2007)}, {int32(2007)}, {int32(2007)}},
	)
}

func TestInsertInto(t *testing.T) {
	e := newEngine(t)
	testQuery(t, e,
		"INSERT INTO mytable (s, i) VALUES ('x', 999);",
		[][]interface{}{{int64(1)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE s = 'x';",
		[][]interface{}{{int64(999)}},
	)
}

func testQuery(t *testing.T, e *sqle.Engine, q string, r [][]interface{}) {
	t.Run(q, func(t *testing.T) {
		require := require.New(t)

		_, rows, err := e.Query(q)
		require.NoError(err)

		i := 0
		for {
			row, err := rows.Next()
			if err == io.EOF {
				break
			}
			require.NoError(err)
			for j, c := range row {
				cc := r[i][j]
				require.Equal(cc, c)
			}

			i++
		}

		require.Equal(len(r), i)
	})
}

func newEngine(t *testing.T) *sqle.Engine {
	require := require.New(t)

	table := mem.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int64},
		{Name: "s", Type: sql.Text},
	})
	require.Nil(table.Insert(sql.NewRow(int64(1), "first row")))
	require.Nil(table.Insert(sql.NewRow(int64(2), "second row")))
	require.Nil(table.Insert(sql.NewRow(int64(3), "third row")))

	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	e := sqle.New()
	e.AddDatabase(db)

	return e
}
