package mem

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/mvader/gitql/sql"
)

func TestTable_Name(t *testing.T) {
	assert := assert.New(t)
	s := sql.Schema{
		sql.Field{"col1", sql.String},
	}
	table := NewTable("test", s)
	assert.Equal("test", table.Name())
}

func TestTable_Insert_RowIter(t *testing.T) {
	assert := assert.New(t)
	s := sql.Schema{
		sql.Field{"col1", sql.String},
	}
	table := NewTable("test", s)
	iter, err := table.RowIter()
	assert.Nil(err)
	assert.NotNil(iter)
	_, err = iter.Next()
	assert.Equal(io.EOF, err)
	err = table.Insert("foo")
	assert.Nil(err)
	err = table.Insert("bar")
	assert.Nil(err)
	iter, err = table.RowIter()
	row, err := iter.Next()
	assert.NotNil(row)
	assert.Nil(err)
	assert.Equal("foo", row.Fields()[0])
	row, err = iter.Next()
	assert.Equal("bar", row.Fields()[0])
	row, err = iter.Next()
	assert.Nil(row)
	assert.Equal(io.EOF, err)
}
