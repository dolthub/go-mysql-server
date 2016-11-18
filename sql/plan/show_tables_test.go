package plan

import (
	"io"
	"testing"

	"github.com/gitql/gitql/mem"
	"github.com/gitql/gitql/sql"
	"github.com/stretchr/testify/assert"
)

func TestShowTables(t *testing.T) {
	assert := assert.New(t)

	unresolvedShowTables := NewShowTables(&sql.UnresolvedDatabase{})

	assert.False(unresolvedShowTables.Resolved())
	assert.Nil(unresolvedShowTables.Children())

	db := mem.NewDatabase("test")
	db.AddTable("test1", mem.NewTable("test1", nil))
	db.AddTable("test2", mem.NewTable("test2", nil))
	db.AddTable("test3", mem.NewTable("test3", nil))

	resolvedShowTables := NewShowTables(db)
	assert.True(resolvedShowTables.Resolved())
	assert.Nil(resolvedShowTables.Children())

	iter, err := resolvedShowTables.RowIter()
	assert.Nil(err)

	res, err := iter.Next()
	assert.Nil(err)
	assert.Equal("test1", res.Fields()[0])

	res, err = iter.Next()
	assert.Nil(err)
	assert.Equal("test2", res.Fields()[0])

	res, err = iter.Next()
	assert.Nil(err)
	assert.Equal("test3", res.Fields()[0])

	_, err = iter.Next()
	assert.Equal(io.EOF, err)
}
