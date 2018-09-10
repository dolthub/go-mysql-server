package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestProject(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()
	childSchema := sql.Schema{
		{Name: "col1", Type: sql.Text, Nullable: true},
		{Name: "col2", Type: sql.Text, Nullable: true},
	}
	child := mem.NewTable("test", childSchema)
	child.Insert(sql.NewEmptyContext(), sql.NewRow("col1_1", "col2_1"))
	child.Insert(sql.NewEmptyContext(), sql.NewRow("col1_2", "col2_2"))
	p := NewProject(
		[]sql.Expression{expression.NewGetField(1, sql.Text, "col2", true)},
		NewResolvedTable(child),
	)
	require.Equal(1, len(p.Children()))
	schema := sql.Schema{
		{Name: "col2", Type: sql.Text, Nullable: true},
	}
	require.Equal(schema, p.Schema())
	iter, err := p.RowIter(ctx)
	require.NoError(err)
	require.NotNil(iter)
	row, err := iter.Next()
	require.NoError(err)
	require.NotNil(row)
	require.Equal(1, len(row))
	require.Equal("col2_1", row[0])
	row, err = iter.Next()
	require.NoError(err)
	require.NotNil(row)
	require.Equal(1, len(row))
	require.Equal("col2_2", row[0])
	row, err = iter.Next()
	require.Equal(io.EOF, err)
	require.Nil(row)

	p = NewProject(nil, NewResolvedTable(child))
	require.Equal(0, len(p.Schema()))

	p = NewProject([]sql.Expression{
		expression.NewAlias(
			expression.NewGetField(1, sql.Text, "col2", true),
			"foo",
		),
	}, NewResolvedTable(child))
	schema = sql.Schema{
		{Name: "foo", Type: sql.Text, Nullable: true},
	}
	require.Equal(schema, p.Schema())
}

func BenchmarkProject(b *testing.B) {
	require := require.New(b)
	ctx := sql.NewEmptyContext()

	for i := 0; i < b.N; i++ {
		d := NewProject([]sql.Expression{
			expression.NewGetField(0, sql.Text, "strfield", true),
			expression.NewGetField(1, sql.Float64, "floatfield", true),
			expression.NewGetField(2, sql.Boolean, "boolfield", false),
			expression.NewGetField(3, sql.Int32, "intfield", false),
			expression.NewGetField(4, sql.Int64, "bigintfield", false),
			expression.NewGetField(5, sql.Blob, "blobfield", false),
		}, NewResolvedTable(benchtable))

		iter, err := d.RowIter(ctx)
		require.NoError(err)
		require.NotNil(iter)

		for {
			_, err := iter.Next()
			if err == io.EOF {
				break
			}

			require.NoError(err)
		}
	}
}
