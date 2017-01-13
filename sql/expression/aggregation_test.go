package expression

import (
	"testing"

	"github.com/gitql/gitql/sql"

	"github.com/stretchr/testify/require"
)

func TestCount_Name(t *testing.T) {
	assert := require.New(t)

	c := NewCount(NewLiteral("foo", sql.String))
	assert.Equal("count(literal_string)", c.Name())
}

func TestCount_Eval_1(t *testing.T) {
	assert := require.New(t)

	c := NewCount(NewLiteral(1, sql.Integer))
	b := c.NewBuffer()
	assert.Equal(int32(0), c.Eval(b))

	c.Update(b, nil)
	c.Update(b, sql.NewRow("foo"))
	c.Update(b, sql.NewRow(1))
	c.Update(b, sql.NewRow(1, 2, 3))
	assert.Equal(int32(4), c.Eval(b))

	b2 := c.NewBuffer()
	c.Update(b2, nil)
	c.Update(b2, sql.NewRow("foo"))
	c.Merge(b, b2)
	assert.Equal(int32(6), c.Eval(b))
}

func TestCount_Eval_Star(t *testing.T) {
	assert := require.New(t)

	c := NewCount(NewStar())
	b := c.NewBuffer()
	assert.Equal(int32(0), c.Eval(b))

	c.Update(b, nil)
	c.Update(b, sql.NewRow("foo"))
	c.Update(b, sql.NewRow(1))
	c.Update(b, sql.NewRow(1, 2, 3))
	assert.Equal(int32(4), c.Eval(b))

	b2 := c.NewBuffer()
	c.Update(b2, sql.NewRow())
	c.Update(b2, sql.NewRow("foo"))
	c.Merge(b, b2)
	assert.Equal(int32(6), c.Eval(b))
}

func TestCount_Eval_String(t *testing.T) {
	assert := require.New(t)

	c := NewCount(NewGetField(0, sql.String, ""))
	b := c.NewBuffer()
	assert.Equal(int32(0), c.Eval(b))

	c.Update(b, sql.NewRow("foo"))
	assert.Equal(int32(1), c.Eval(b))

	c.Update(b, sql.NewRow(nil))
	assert.Equal(int32(1), c.Eval(b))
}

func TestFirst_Name(t *testing.T) {
	assert := require.New(t)

	c := NewFirst(NewGetField(0, sql.Integer, "field"))
	assert.Equal("first(field)", c.Name())
}

func TestFirst_Eval(t *testing.T) {
	assert := require.New(t)

	c := NewFirst(NewGetField(0, sql.Integer, "field"))
	b := c.NewBuffer()
	assert.Nil(c.Eval(b))

	c.Update(b, sql.NewRow(int32(1)))
	assert.Equal(int32(1), c.Eval(b))

	c.Update(b, sql.NewRow(int32(2)))
	assert.Equal(int32(1), c.Eval(b))

	b2 := c.NewBuffer()
	c.Update(b2, sql.NewRow(int32(2)))
	c.Merge(b, b2)
	assert.Equal(int32(1), c.Eval(b))
}
