package mem

import (
	"testing"

	"github.com/gitql/gitql/sql"

	"github.com/stretchr/testify/assert"
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

	rows, err := sql.NodeToRows(table)
	assert.Nil(err)
	assert.Len(rows, 0)

	err = table.Insert("foo")
	rows, err = sql.NodeToRows(table)
	assert.Nil(err)
	assert.Len(rows, 1)
	assert.Nil(s.CheckRow(rows[0]))

	err = table.Insert("bar")
	rows, err = sql.NodeToRows(table)
	assert.Nil(err)
	assert.Len(rows, 2)
	assert.Nil(s.CheckRow(rows[0]))
	assert.Nil(s.CheckRow(rows[1]))
}
