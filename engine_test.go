package gitql_test

import (
	"io"
	"testing"

	"github.com/gitql/gitql"
	"github.com/gitql/gitql/mem"
	"github.com/gitql/gitql/sql"

	"github.com/stretchr/testify/require"
)

func TestEngine_Query(t *testing.T) {
	e := newEngine(t)
	testQuery(t, e,
		"SELECT i FROM mytable;",
		[]sql.Row{
			sql.NewMemoryRow(int64(1)),
			sql.NewMemoryRow(int64(2)),
			sql.NewMemoryRow(int64(3)),
		},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE i = 2;",
		[]sql.Row{
			sql.NewMemoryRow(int64(2)),
		},
	)

	testQuery(t, e,
		"SELECT i FROM mytable ORDER BY i DESC;",
		[]sql.Row{
			sql.NewMemoryRow(int64(3)),
			sql.NewMemoryRow(int64(2)),
			sql.NewMemoryRow(int64(1)),
		},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE s = 'a' ORDER BY i DESC;",
		[]sql.Row{
			sql.NewMemoryRow(int64(1)),
		},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE s = 'a' ORDER BY i DESC LIMIT 1;",
		[]sql.Row{
			sql.NewMemoryRow(int64(1)),
		},
	)
}

func testQuery(t *testing.T, e *gitql.Engine, q string, r []sql.Row) {
	assert := require.New(t)

	schema, iter, err := e.Query(q)
	assert.Nil(err)
	assert.NotNil(iter)
	assert.NotNil(schema)

	results := []sql.Row{}
	for {
		el, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			assert.Fail("returned err distinct of io.EOF: %q", err)
		}
		results = append(results, el)
	}

	assert.Len(results, len(r))
	assert.Equal(results, r)
}

func newEngine(t *testing.T) *gitql.Engine {
	assert := require.New(t)

	table := mem.NewTable("mytable", sql.Schema{
		{"i", sql.BigInteger},
		{"s", sql.String},
	})
	assert.Nil(table.Insert(int64(1), "a"))
	assert.Nil(table.Insert(int64(2), "b"))
	assert.Nil(table.Insert(int64(3), "c"))

	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	e := gitql.New()
	e.AddDatabase(db)

	return e
}
