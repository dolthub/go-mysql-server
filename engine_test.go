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
	assert := require.New(t)

	table := mem.NewTable("mytable", sql.Schema{{"i", sql.Integer}})
	assert.Nil(table.Insert(int32(1)))
	assert.Nil(table.Insert(int32(2)))
	assert.Nil(table.Insert(int32(3)))

	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	e := gitql.New()
	e.AddDatabase(db)

	q := "SELECT i FROM mytable;"
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
	assert.Len(results, 3)
	assert.Equal(sql.NewMemoryRow(int32(1)), results[0])
	assert.Equal(sql.NewMemoryRow(int32(2)), results[1])
	assert.Equal(sql.NewMemoryRow(int32(3)), results[2])
}
