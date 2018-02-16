package expression

import (
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"github.com/stretchr/testify/require"
)

func TestCount_Name(t *testing.T) {
	require := require.New(t)

	c := NewCount(NewLiteral("foo", sql.Text))
	require.Equal("count(literal_TEXT)", c.Name())
}

func TestCount_Eval_1(t *testing.T) {
	require := require.New(t)

	c := NewCount(NewLiteral(1, sql.Int32))
	b := c.NewBuffer()
	require.Equal(int32(0), c.Eval(b))

	c.Update(b, nil)
	c.Update(b, sql.NewRow("foo"))
	c.Update(b, sql.NewRow(1))
	c.Update(b, sql.NewRow(nil))
	c.Update(b, sql.NewRow(1, 2, 3))
	require.Equal(int32(5), c.Eval(b))

	b2 := c.NewBuffer()
	c.Update(b2, nil)
	c.Update(b2, sql.NewRow("foo"))
	c.Merge(b, b2)
	require.Equal(int32(7), c.Eval(b))
}

func TestCount_Eval_Star(t *testing.T) {
	require := require.New(t)

	c := NewCount(NewStar())
	b := c.NewBuffer()
	require.Equal(int32(0), c.Eval(b))

	c.Update(b, nil)
	c.Update(b, sql.NewRow("foo"))
	c.Update(b, sql.NewRow(1))
	c.Update(b, sql.NewRow(nil))
	c.Update(b, sql.NewRow(1, 2, 3))
	require.Equal(int32(5), c.Eval(b))

	b2 := c.NewBuffer()
	c.Update(b2, sql.NewRow())
	c.Update(b2, sql.NewRow("foo"))
	c.Merge(b, b2)
	require.Equal(int32(7), c.Eval(b))
}

func TestCount_Eval_String(t *testing.T) {
	require := require.New(t)

	c := NewCount(NewGetField(0, sql.Text, "", true))
	b := c.NewBuffer()
	require.Equal(int32(0), c.Eval(b))

	c.Update(b, sql.NewRow("foo"))
	require.Equal(int32(1), c.Eval(b))

	c.Update(b, sql.NewRow(nil))
	require.Equal(int32(1), c.Eval(b))
}

func TestFirst_Name(t *testing.T) {
	require := require.New(t)

	c := NewFirst(NewGetField(0, sql.Int32, "field", true))
	require.Equal("first(field)", c.Name())
}

func TestFirst_Eval(t *testing.T) {
	require := require.New(t)

	c := NewFirst(NewGetField(0, sql.Int32, "field", true))
	b := c.NewBuffer()
	require.Nil(c.Eval(b))

	c.Update(b, sql.NewRow(int32(1)))
	require.Equal(int32(1), c.Eval(b))

	c.Update(b, sql.NewRow(int32(2)))
	require.Equal(int32(1), c.Eval(b))

	b2 := c.NewBuffer()
	c.Update(b2, sql.NewRow(int32(2)))
	c.Merge(b, b2)
	require.Equal(int32(1), c.Eval(b))
}
