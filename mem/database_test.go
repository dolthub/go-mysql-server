package mem

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/mvader/gitql/sql"
)

func TestDatabase_Name(t *testing.T) {
	assert := assert.New(t)
	db := NewDatabase("test")
	assert.Equal("test", db.Name())
}

func TestDatabase_AddTable(t *testing.T) {
	assert := assert.New(t)
	db := NewDatabase("test")
	relations := db.Relations()
	assert.Equal(0, len(relations))
	table := &Table{"test_table", sql.Schema{}, nil}
	db.AddTable("test_table", table)
	relations = db.Relations()
	assert.Equal(1, len(relations))
	rel, ok := relations["test_table"]
	assert.True(ok)
	assert.NotNil(rel)
}
