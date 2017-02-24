package sqle_test

import (
	gosql "database/sql"
	"testing"

	"gopkg.in/sqle/sqle.v0"
	"gopkg.in/sqle/sqle.v0/mem"
	"gopkg.in/sqle/sqle.v0/sql"

	"github.com/stretchr/testify/require"
)

const (
	driverName = "engine_tests"
)

func TestQueries(t *testing.T) {
	testQuery(t,
		"SELECT i FROM mytable;",
		[][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
	)

	testQuery(t,
		"SELECT i FROM mytable WHERE i = 2;",
		[][]interface{}{{int64(2)}},
	)

	testQuery(t,
		"SELECT i FROM mytable ORDER BY i DESC;",
		[][]interface{}{{int64(3)}, {int64(2)}, {int64(1)}},
	)

	testQuery(t,
		"SELECT i FROM mytable WHERE s = 'a' ORDER BY i DESC;",
		[][]interface{}{{int64(1)}},
	)

	testQuery(t,
		"SELECT i FROM mytable WHERE s = 'a' ORDER BY i DESC LIMIT 1;",
		[][]interface{}{{int64(1)}},
	)

	testQuery(t,
		"SELECT COUNT(*) FROM mytable;",
		[][]interface{}{{int64(3)}},
	)

	testQuery(t,
		"SELECT COUNT(*) AS c FROM mytable;",
		[][]interface{}{{int64(3)}},
	)
}

func testQuery(t *testing.T, q string, r [][]interface{}) {
	t.Run(q, func(t *testing.T) {
		assert := require.New(t)

		e := newEngine(t)
		sqle.DefaultEngine = e

		db, err := gosql.Open(sqle.DriverName, "")
		assert.NoError(err)
		defer func() { assert.NoError(db.Close()) }()

		res, err := db.Query(q)
		assert.NoError(err)
		defer func() { assert.NoError(res.Close()) }()

		cols, err := res.Columns()
		assert.NoError(err)
		assert.Equal(len(r[0]), len(cols))

		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := 0; i < len(cols); i++ {
			valPtrs[i] = &vals[i]
		}

		i := 0
		for {
			if !res.Next() {
				break
			}

			err := res.Scan(valPtrs...)
			assert.NoError(err)

			assert.Equal(r[i], vals)
			i++
		}

		assert.NoError(res.Err())
		assert.Equal(len(r), i)
	})
}

func newEngine(t *testing.T) *sqle.Engine {
	assert := require.New(t)

	table := mem.NewTable("mytable", sql.Schema{
		{"i", sql.BigInteger},
		{"s", sql.String},
	})
	assert.Nil(table.Insert(sql.NewRow(int64(1), "a")))
	assert.Nil(table.Insert(sql.NewRow(int64(2), "b")))
	assert.Nil(table.Insert(sql.NewRow(int64(3), "c")))

	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	e := sqle.New()
	e.AddDatabase(db)

	return e
}
