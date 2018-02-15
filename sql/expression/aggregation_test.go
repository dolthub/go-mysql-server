package expression

import (
	"testing"
	"time"

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
	require.Equal(int32(0), eval(t, c, b))

	require.NoError(c.Update(b, nil))
	require.NoError(c.Update(b, sql.NewRow("foo")))
	require.NoError(c.Update(b, sql.NewRow(1)))
	require.NoError(c.Update(b, sql.NewRow(nil)))
	require.NoError(c.Update(b, sql.NewRow(1, 2, 3)))
	require.Equal(int32(5), eval(t, c, b))

	b2 := c.NewBuffer()
	require.NoError(c.Update(b2, nil))
	require.NoError(c.Update(b2, sql.NewRow("foo")))
	require.NoError(c.Merge(b, b2))
	require.Equal(int32(7), eval(t, c, b))
}

func TestCount_Eval_Star(t *testing.T) {
	require := require.New(t)

	c := NewCount(NewStar())
	b := c.NewBuffer()
	require.Equal(int32(0), eval(t, c, b))

	c.Update(b, nil)
	c.Update(b, sql.NewRow("foo"))
	c.Update(b, sql.NewRow(1))
	c.Update(b, sql.NewRow(nil))
	c.Update(b, sql.NewRow(1, 2, 3))
	require.Equal(int32(5), eval(t, c, b))

	b2 := c.NewBuffer()
	c.Update(b2, sql.NewRow())
	c.Update(b2, sql.NewRow("foo"))
	c.Merge(b, b2)
	require.Equal(int32(7), eval(t, c, b))
}

func TestCount_Eval_String(t *testing.T) {
	require := require.New(t)

	c := NewCount(NewGetField(0, sql.Text, "", true))
	b := c.NewBuffer()
	require.Equal(int32(0), eval(t, c, b))

	c.Update(b, sql.NewRow("foo"))
	require.Equal(int32(1), eval(t, c, b))

	c.Update(b, sql.NewRow(nil))
	require.Equal(int32(1), eval(t, c, b))
}

func TestMin_Name(t *testing.T) {
	assert := require.New(t)

	m := NewMin(NewGetField(0, sql.Int32, "field", true))
	assert.Equal("min(field)", m.Name())
}

func TestMin_Eval_Int32(t *testing.T) {
	assert := require.New(t)

	m := NewMin(NewGetField(0, sql.Int32, "field", true))
	b := m.NewBuffer()

	m.Update(b, sql.NewRow(int32(7)))
	m.Update(b, sql.NewRow(int32(2)))
	m.Update(b, sql.NewRow(nil))

	v, err := m.Eval(b)
	assert.NoError(err)
	assert.Equal(int32(2), v)
}

func TestMin_Eval_Text(t *testing.T) {
	assert := require.New(t)

	m := NewMin(NewGetField(0, sql.Text, "field", true))
	b := m.NewBuffer()

	m.Update(b, sql.NewRow("a"))
	m.Update(b, sql.NewRow("A"))
	m.Update(b, sql.NewRow("b"))

	v, err := m.Eval(b)
	assert.NoError(err)
	assert.Equal("A", v)
}

func TestMin_Eval_Timestamp(t *testing.T) {
	assert := require.New(t)

	m := NewMin(NewGetField(0, sql.Timestamp, "field", true))
	b := m.NewBuffer()

	expected, _ := time.Parse(sql.TimestampLayout, "2006-01-02 15:04:05")
	someTime, _ := time.Parse(sql.TimestampLayout, "2007-01-02 15:04:05")
	otherTime, _ := time.Parse(sql.TimestampLayout, "2008-01-02 15:04:05")

	m.Update(b, sql.NewRow(someTime))
	m.Update(b, sql.NewRow(expected))
	m.Update(b, sql.NewRow(otherTime))

	v, err := m.Eval(b)
	assert.NoError(err)
	assert.Equal(expected, v)
}

func TestMin_Eval_NULL(t *testing.T) {
	assert := require.New(t)

	m := NewMin(NewGetField(0, sql.Int32, "field", true))
	b := m.NewBuffer()

	m.Update(b, sql.NewRow(nil))
	m.Update(b, sql.NewRow(nil))
	m.Update(b, sql.NewRow(nil))

	v, err := m.Eval(b)
	assert.NoError(err)
	assert.Equal(nil, v)
}

func TestMin_Eval_Empty(t *testing.T) {
	assert := require.New(t)

	m := NewMin(NewGetField(0, sql.Int32, "field", true))
	b := m.NewBuffer()

	v, err := m.Eval(b)
	assert.NoError(err)
	assert.Equal(nil, v)
}
func TestMax_Name(t *testing.T) {
	assert := require.New(t)

	m := NewMax(NewGetField(0, sql.Int32, "field", true))
	assert.Equal("max(field)", m.Name())
}

func TestMax_Eval_Int32(t *testing.T) {
	assert := require.New(t)

	m := NewMax(NewGetField(0, sql.Int32, "field", true))
	b := m.NewBuffer()

	m.Update(b, sql.NewRow(int32(7)))
	m.Update(b, sql.NewRow(int32(2)))
	m.Update(b, sql.NewRow(int32(6)))

	assert.Equal(int32(7), m.Eval(b))
}

func TestMax_Eval_Text(t *testing.T) {
	assert := require.New(t)

	m := NewMax(NewGetField(0, sql.Text, "field", true))
	b := m.NewBuffer()

	m.Update(b, sql.NewRow("a"))
	m.Update(b, sql.NewRow("A"))
	m.Update(b, sql.NewRow("b"))

	assert.Equal("b", m.Eval(b))
}

func TestMax_Eval_Timestamp(t *testing.T) {
	assert := require.New(t)

	m := NewMax(NewGetField(0, sql.Timestamp, "field", true))
	b := m.NewBuffer()

	expected, _ := time.Parse(sql.TimestampLayout, "2008-01-02 15:04:05")
	someTime, _ := time.Parse(sql.TimestampLayout, "2007-01-02 15:04:05")
	otherTime, _ := time.Parse(sql.TimestampLayout, "2006-01-02 15:04:05")

	m.Update(b, sql.NewRow(someTime))
	m.Update(b, sql.NewRow(expected))
	m.Update(b, sql.NewRow(otherTime))

	assert.Equal(expected, m.Eval(b))
}
