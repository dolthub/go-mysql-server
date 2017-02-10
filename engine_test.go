package gitql_test

import (
	gosql "database/sql"
	"testing"

	"github.com/gitql/gitql"
	"github.com/gitql/gitql/mem"
	"github.com/gitql/gitql/sql"

	"github.com/stretchr/testify/require"
)

const (
	driverName = "engine_tests"
)

func TestEngine_Query(t *testing.T) {
	e := newEngine(t)
	gosql.Register(driverName, e)
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
		"SELECT i FROM mytable WHERE s = 'a' ORDER BY i DESC;",
		[][]interface{}{{int64(1)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE s = 'a' ORDER BY i DESC LIMIT 1;",
		[][]interface{}{{int64(1)}},
	)

	testQuery(t, e,
		"SELECT COUNT(*) FROM mytable;",
		[][]interface{}{{int64(3)}},
	)
}

func testQuery(t *testing.T, e *gitql.Engine, q string, r [][]interface{}) {
	assert := require.New(t)

	db, err := gosql.Open(driverName, "")
	assert.NoError(err)
	defer func() { assert.NoError(db.Close()) }()

	res, err := db.Query(q)
	assert.NoError(err)
	defer func() { assert.NoError(res.Close()) }()

	cols, err := res.Columns()
	assert.NoError(err)
	assert.Equal(len(r[0]), len(cols))

	i := 0
	for {
		if !res.Next() {
			break
		}

		expectedRow := r[i]
		i++

		row := make([]interface{}, len(expectedRow))
		for i := range row {
			i64 := int64(0)
			row[i] = &i64
		}

		assert.NoError(res.Scan(row...))
		for i := range row {
			row[i] = *(row[i].(*int64))
		}

		assert.Equal(expectedRow, row)
	}

	assert.Equal(len(r), i)
}

func newEngine(t *testing.T) *gitql.Engine {
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

	e := gitql.New()
	e.AddDatabase(db)

	return e
}
