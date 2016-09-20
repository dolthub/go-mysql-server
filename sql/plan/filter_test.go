package plan

import (
	"testing"

	"github.com/mvader/gitql/mem"
	"github.com/mvader/gitql/sql"
	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	assert := assert.New(t)
	childSchema := sql.Schema{
		sql.Field{"col1", sql.String},
		sql.Field{"col2", sql.String},
		sql.Field{"col3", sql.Integer},
		sql.Field{"col4", sql.BigInteger},
	}
	child := mem.NewTable("test", childSchema)
	child.Insert("col1_1", "col2_1", 1111, int64(2222))
	child.Insert("col1_2", "col2_2", 3333, int64(4444))

	f := NewFilter("col1", child, "col1_1")

	assert.Equal(1, len(f.Children()))

	iter, err := f.RowIter()
	assert.Nil(err)
	assert.NotNil(iter)

	row, err := iter.Next()
	assert.Nil(err)
	assert.NotNil(row)

	assert.Equal("col1_1", row.Fields()[0])
	assert.Equal("col2_1", row.Fields()[1])

	row, err = iter.Next()
	assert.NotNil(err)
	assert.Nil(row)

	f = NewFilter("col3", child, 1111)

	iter, err = f.RowIter()
	assert.Nil(err)
	assert.NotNil(iter)

	row, err = iter.Next()
	assert.Nil(err)
	assert.NotNil(row)

	assert.Equal(1111, row.Fields()[2])
	assert.Equal(int64(2222), row.Fields()[3])

	f = NewFilter("col4", child, int64(4444))

	iter, err = f.RowIter()
	assert.Nil(err)
	assert.NotNil(iter)

	row, err = iter.Next()
	assert.Nil(err)
	assert.NotNil(row)

	assert.Equal(3333, row.Fields()[2])
	assert.Equal(int64(4444), row.Fields()[3])
}
