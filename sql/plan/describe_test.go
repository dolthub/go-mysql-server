package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/sqle/sqle.v0/mem"
	"gopkg.in/sqle/sqle.v0/sql"
)

func TestDescribe(t *testing.T) {
	assert := assert.New(t)

	table := mem.NewTable("test", sql.Schema{
		{Name: "c1", Type: sql.String},
		{Name: "c2", Type: sql.Integer},
	})

	d := NewDescribe(table)
	iter, err := d.RowIter()
	assert.Nil(err)
	assert.NotNil(iter)

	n, err := iter.Next()
	assert.Nil(err)
	assert.Equal(sql.NewRow("c1", "string"), n)

	n, err = iter.Next()
	assert.Nil(err)
	assert.Equal(sql.NewRow("c2", "integer"), n)

	n, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(n)
}

func TestDescribe_Empty(t *testing.T) {
	assert := assert.New(t)

	d := NewDescribe(NewUnresolvedTable("test_table"))

	iter, err := d.RowIter()
	assert.Nil(err)
	assert.NotNil(iter)

	n, err := iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(n)
}
