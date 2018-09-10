package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestDistinct(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	childSchema := sql.Schema{
		{Name: "name", Type: sql.Text, Nullable: true},
		{Name: "email", Type: sql.Text, Nullable: true},
	}
	child := mem.NewTable("test", childSchema)

	rows := []sql.Row{
		sql.NewRow("john", "john@doe.com"),
		sql.NewRow("jane", "jane@doe.com"),
		sql.NewRow("john", "johnx@doe.com"),
		sql.NewRow("martha", "marthax@doe.com"),
		sql.NewRow("martha", "martha@doe.com"),
	}

	for _, r := range rows {
		require.NoError(child.Insert(sql.NewEmptyContext(), r))
	}

	p := NewProject([]sql.Expression{
		expression.NewGetField(0, sql.Text, "name", true),
	}, NewResolvedTable(child))
	d := NewDistinct(p)

	iter, err := d.RowIter(ctx)
	require.NoError(err)
	require.NotNil(iter)

	var results []string
	for {
		row, err := iter.Next()
		if err == io.EOF {
			break
		}

		require.NoError(err)
		result, ok := row[0].(string)
		require.True(ok, "first row column should be string, but is %T", row[0])
		results = append(results, result)
	}

	require.Equal([]string{"john", "jane", "martha"}, results)
}

func TestOrderedDistinct(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	childSchema := sql.Schema{
		{Name: "name", Type: sql.Text, Nullable: true},
		{Name: "email", Type: sql.Text, Nullable: true},
	}
	child := mem.NewTable("test", childSchema)

	rows := []sql.Row{
		sql.NewRow("jane", "jane@doe.com"),
		sql.NewRow("john", "john@doe.com"),
		sql.NewRow("john", "johnx@doe.com"),
		sql.NewRow("martha", "martha@doe.com"),
		sql.NewRow("martha", "marthax@doe.com"),
	}

	for _, r := range rows {
		require.NoError(child.Insert(sql.NewEmptyContext(), r))
	}

	p := NewProject([]sql.Expression{
		expression.NewGetField(0, sql.Text, "name", true),
	}, NewResolvedTable(child))
	d := NewOrderedDistinct(p)

	iter, err := d.RowIter(ctx)
	require.NoError(err)
	require.NotNil(iter)

	var results []string
	for {
		row, err := iter.Next()
		if err == io.EOF {
			break
		}

		require.NoError(err)
		result, ok := row[0].(string)
		require.True(ok, "first row column should be string, but is %T", row[0])
		results = append(results, result)
	}

	require.Equal([]string{"jane", "john", "martha"}, results)
}

func BenchmarkDistinct(b *testing.B) {
	require := require.New(b)
	ctx := sql.NewEmptyContext()

	for i := 0; i < b.N; i++ {
		p := NewProject([]sql.Expression{
			expression.NewGetField(0, sql.Text, "strfield", true),
			expression.NewGetField(1, sql.Float64, "floatfield", true),
			expression.NewGetField(2, sql.Boolean, "boolfield", false),
			expression.NewGetField(3, sql.Int32, "intfield", false),
			expression.NewGetField(4, sql.Int64, "bigintfield", false),
			expression.NewGetField(5, sql.Blob, "blobfield", false),
		}, NewResolvedTable(benchtable))
		d := NewDistinct(p)

		iter, err := d.RowIter(ctx)
		require.NoError(err)
		require.NotNil(iter)

		var rows int
		for {
			_, err := iter.Next()
			if err == io.EOF {
				break
			}

			require.NoError(err)
			rows++
		}
		require.Equal(100, rows)
	}
}

func BenchmarkOrderedDistinct(b *testing.B) {
	require := require.New(b)
	ctx := sql.NewEmptyContext()

	for i := 0; i < b.N; i++ {
		p := NewProject([]sql.Expression{
			expression.NewGetField(0, sql.Text, "strfield", true),
			expression.NewGetField(1, sql.Float64, "floatfield", true),
			expression.NewGetField(2, sql.Boolean, "boolfield", false),
			expression.NewGetField(3, sql.Int32, "intfield", false),
			expression.NewGetField(4, sql.Int64, "bigintfield", false),
			expression.NewGetField(5, sql.Blob, "blobfield", false),
		}, NewResolvedTable(benchtable))
		d := NewOrderedDistinct(p)

		iter, err := d.RowIter(ctx)
		require.NoError(err)
		require.NotNil(iter)

		var rows int
		for {
			_, err := iter.Next()
			if err == io.EOF {
				break
			}

			require.NoError(err)
			rows++
		}
		require.Equal(100, rows)
	}
}
