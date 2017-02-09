package plan

import (
	"io"
	"testing"

	"github.com/gitql/gitql/mem"
	"github.com/gitql/gitql/sql"
	"github.com/stretchr/testify/assert"
)

func TestDescribe(t *testing.T) {
	assert := assert.New(t)

	table := mem.NewTable("test", sql.Schema{
		sql.Column{Name: "c1", Type: sql.String},
		sql.Column{Name: "c2", Type: sql.Integer},
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
