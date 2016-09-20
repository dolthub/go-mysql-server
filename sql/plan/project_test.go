package plan

import (
	"io"
	"testing"

	"github.com/mvader/gitql/mem"
	"github.com/mvader/gitql/sql"
	"github.com/stretchr/testify/require"
)

func TestProject(t *testing.T) {
	require := require.New(t)
	childSchema := sql.Schema{
		sql.Field{"col1", sql.String},
		sql.Field{"col2", sql.String},
	}
	child := mem.NewTable("test", childSchema)
	child.Insert("col1_1", "col2_1")
	child.Insert("col1_2", "col2_2")
	p := NewProject([]string{"col2"}, child)
	require.Equal(1, len(p.Children()))
	schema := sql.Schema{
		sql.Field{"col2", sql.String},
	}
	require.Equal(schema, p.Schema())
	iter, err := p.RowIter()
	require.Nil(err)
	require.NotNil(iter)
	row, err := iter.Next()
	require.Nil(err)
	require.NotNil(row)
	fields := row.Fields()
	require.Equal(1, len(fields))
	require.Equal("col2_1", fields[0])
	row, err = iter.Next()
	require.Nil(err)
	require.NotNil(row)
	fields = row.Fields()
	require.Equal(1, len(fields))
	require.Equal("col2_2", fields[0])
	row, err = iter.Next()
	require.Equal(io.EOF, err)
	require.Nil(row)

	p = NewProject(nil, child)
	require.Equal(2, len(p.schema))
}
