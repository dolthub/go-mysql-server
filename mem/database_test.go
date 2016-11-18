package mem

import (
	"testing"

	"github.com/gitql/gitql/sql"

	"github.com/stretchr/testify/assert"
)

func TestDatabase_Name(t *testing.T) {
	assert := assert.New(t)
	db := NewDatabase("test")
	assert.Equal("test", db.Name())
}

func TestDatabase_AddTable(t *testing.T) {
	assert := assert.New(t)
	db := NewDatabase("test")
	tables := db.Tables()
	assert.Equal(0, len(tables))
	table := &Table{"test_table", sql.Schema{}, nil}
	db.AddTable("test_table", table)
	tables = db.Tables()
	assert.Equal(1, len(tables))
	tt, ok := tables["test_table"]
	assert.True(ok)
	assert.NotNil(tt)
}
