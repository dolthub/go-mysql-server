package aggregation

import (
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestCountEval1(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCount(expression.NewLiteral(1, sql.Int32))
	b := c.NewBuffer()
	require.Equal(int64(0), eval(t, c, b))

	require.NoError(c.Update(ctx, b, nil))
	require.NoError(c.Update(ctx, b, sql.NewRow("foo")))
	require.NoError(c.Update(ctx, b, sql.NewRow(1)))
	require.NoError(c.Update(ctx, b, sql.NewRow(nil)))
	require.NoError(c.Update(ctx, b, sql.NewRow(1, 2, 3)))
	require.Equal(int64(5), eval(t, c, b))

	b2 := c.NewBuffer()
	require.NoError(c.Update(ctx, b2, nil))
	require.NoError(c.Update(ctx, b2, sql.NewRow("foo")))
	require.NoError(c.Merge(ctx, b, b2))
	require.Equal(int64(7), eval(t, c, b))
}

func TestCountEvalStar(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCount(expression.NewStar())
	b := c.NewBuffer()
	require.Equal(int64(0), eval(t, c, b))

	require.NoError(c.Update(ctx, b, nil))
	require.NoError(c.Update(ctx, b, sql.NewRow("foo")))
	require.NoError(c.Update(ctx, b, sql.NewRow(1)))
	require.NoError(c.Update(ctx, b, sql.NewRow(nil)))
	require.NoError(c.Update(ctx, b, sql.NewRow(1, 2, 3)))
	require.Equal(int64(5), eval(t, c, b))

	b2 := c.NewBuffer()
	require.NoError(c.Update(ctx, b2, sql.NewRow()))
	require.NoError(c.Update(ctx, b2, sql.NewRow("foo")))
	require.NoError(c.Merge(ctx, b, b2))
	require.Equal(int64(7), eval(t, c, b))
}

func TestCountEvalString(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCount(expression.NewGetField(0, sql.Text, "", true))
	b := c.NewBuffer()
	require.Equal(int64(0), eval(t, c, b))

	require.NoError(c.Update(ctx, b, sql.NewRow("foo")))
	require.Equal(int64(1), eval(t, c, b))

	require.NoError(c.Update(ctx, b, sql.NewRow(nil)))
	require.Equal(int64(1), eval(t, c, b))
}

func TestCountDistinctEval1(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCountDistinct(expression.NewLiteral(1, sql.Int32))
	b := c.NewBuffer()
	require.Equal(int64(0), eval(t, c, b))

	require.NoError(c.Update(ctx, b, nil))
	require.NoError(c.Update(ctx, b, sql.NewRow("foo")))
	require.NoError(c.Update(ctx, b, sql.NewRow(1)))
	require.NoError(c.Update(ctx, b, sql.NewRow(nil)))
	require.NoError(c.Update(ctx, b, sql.NewRow(1, 2, 3)))
	require.Equal(int64(1), eval(t, c, b))
}

func TestCountDistinctEvalStar(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCountDistinct(expression.NewStar())
	b := c.NewBuffer()
	require.Equal(int64(0), eval(t, c, b))

	require.NoError(c.Update(ctx, b, nil))
	require.NoError(c.Update(ctx, b, sql.NewRow("foo")))
	require.NoError(c.Update(ctx, b, sql.NewRow(1)))
	require.NoError(c.Update(ctx, b, sql.NewRow(nil)))
	require.NoError(c.Update(ctx, b, sql.NewRow(1, 2, 3)))
	require.Equal(int64(5), eval(t, c, b))

	b2 := c.NewBuffer()
	require.NoError(c.Update(ctx, b2, sql.NewRow(1)))
	require.NoError(c.Update(ctx, b2, sql.NewRow("foo")))
	require.NoError(c.Update(ctx, b2, sql.NewRow(5)))
	require.NoError(c.Merge(ctx, b, b2))

	require.Equal(int64(6), eval(t, c, b))
}

func TestCountDistinctEvalString(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCountDistinct(expression.NewGetField(0, sql.Text, "", true))
	b := c.NewBuffer()
	require.Equal(int64(0), eval(t, c, b))

	require.NoError(c.Update(ctx, b, sql.NewRow("foo")))
	require.Equal(int64(1), eval(t, c, b))

	require.NoError(c.Update(ctx, b, sql.NewRow(nil)))
	require.NoError(c.Update(ctx, b, sql.NewRow("foo")))
	require.NoError(c.Update(ctx, b, sql.NewRow("bar")))
	require.Equal(int64(2), eval(t, c, b))
}
