package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/mvader/gitql/mem"
	"github.com/mvader/gitql/sql"
)

func TestProject(t *testing.T) {
	assert := assert.New(t)
	childSchema := sql.Schema{
		sql.Field{"col1", sql.String},
		sql.Field{"col2", sql.String},
	}
	child := mem.NewTable("test", childSchema)
	child.Insert("col1_1", "col2_1")
	child.Insert("col1_2", "col2_2")
	p := NewProject([]string{"col2"}, child)
	assert.Equal(1, len(p.Children()))
	schema := sql.Schema{
		sql.Field{"col2", sql.String},
	}
	assert.Equal(schema, p.Schema())
	iter, err := p.RowIter()
	assert.Nil(err)
	assert.NotNil(iter)
	row, err := iter.Next()
	assert.Nil(err)
	assert.NotNil(row)
	fields := row.Fields()
	assert.Equal(1, len(fields))
	assert.Equal("col2_1", fields[0])
	row, err = iter.Next()
	assert.Nil(err)
	assert.NotNil(row)
	fields = row.Fields()
	assert.Equal(1, len(fields))
	assert.Equal("col2_2", fields[0])
	row, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(row)
}
