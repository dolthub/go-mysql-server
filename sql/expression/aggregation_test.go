package expression

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
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
	m.Update(b, sql.NewRow(nil))
	m.Update(b, sql.NewRow(int32(6)))

	v, err := m.Eval(b)
	assert.NoError(err)
	assert.Equal(int32(7), v)
}

func TestMax_Eval_Text(t *testing.T) {
	assert := require.New(t)

	m := NewMax(NewGetField(0, sql.Text, "field", true))
	b := m.NewBuffer()

	m.Update(b, sql.NewRow("a"))
	m.Update(b, sql.NewRow("A"))
	m.Update(b, sql.NewRow("b"))

	v, err := m.Eval(b)
	assert.NoError(err)
	assert.Equal("b", v)
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

	v, err := m.Eval(b)
	assert.NoError(err)
	assert.Equal(expected, v)
}
func TestMax_Eval_NULL(t *testing.T) {
	assert := require.New(t)

	m := NewMax(NewGetField(0, sql.Int32, "field", true))
	b := m.NewBuffer()

	m.Update(b, sql.NewRow(nil))
	m.Update(b, sql.NewRow(nil))
	m.Update(b, sql.NewRow(nil))

	v, err := m.Eval(b)
	assert.NoError(err)
	assert.Equal(nil, v)
}

func TestMax_Eval_Empty(t *testing.T) {
	assert := require.New(t)

	m := NewMax(NewGetField(0, sql.Int32, "field", true))
	b := m.NewBuffer()

	v, err := m.Eval(b)
	assert.NoError(err)
	assert.Equal(nil, v)
}

func TestAvg_Name(t *testing.T) {
	require := require.New(t)

	avgNode := NewAvg(NewGetField(0, sql.Int32, "col1", true))
	require.Equal("avg(col1)", avgNode.Name())
}

func TestAvg_Eval_INT32(t *testing.T) {
	require := require.New(t)

	avgNode := NewAvg(NewGetField(0, sql.Int32, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(buffer))

	avgNode.Update(buffer, sql.NewRow(int32(1)))
	require.Equal(float64(1), eval(t, avgNode, buffer))

	avgNode.Update(buffer, sql.NewRow(int32(2)))
	require.Equal(float64(1.5), eval(t, avgNode, buffer))
}

func TestAvg_Eval_UINT64(t *testing.T) {
	require := require.New(t)

	avgNode := NewAvg(NewGetField(0, sql.Uint64, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(buffer))

	err := avgNode.Update(buffer, sql.NewRow(uint64(1)))
	require.NoError(err)
	require.Equal(float64(1), eval(t, avgNode, buffer))

	err = avgNode.Update(buffer, sql.NewRow(uint64(2)))
	require.NoError(err)
	require.Equal(float64(1.5), eval(t, avgNode, buffer))
}

func TestAvg_Eval_Text(t *testing.T) {
	require := require.New(t)

	avgNode := NewAvg(NewGetField(0, sql.Text, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(buffer))

	err := avgNode.Update(buffer, sql.NewRow("foo"))
	require.Error(err)
}

func TestAvg_Merge(t *testing.T) {
	require := require.New(t)

	avgNode := NewAvg(NewGetField(0, sql.Float32, "col1", true))
	require.NotNil(avgNode)

	buffer1 := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(buffer1))
	err := avgNode.Update(buffer1, sql.NewRow(float32(1)))
	require.NoError(err)
	err = avgNode.Update(buffer1, sql.NewRow(float32(4)))
	require.NoError(err)
	require.Equal(float64(2.5), eval(t, avgNode, buffer1))

	buffer2 := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(buffer2))
	err = avgNode.Update(buffer2, sql.NewRow(float32(2)))
	require.NoError(err)
	err = avgNode.Update(buffer2, sql.NewRow(float32(7)))
	require.NoError(err)
	err = avgNode.Update(buffer2, sql.NewRow(float32(12)))
	require.NoError(err)
	require.Equal(float64(7), eval(t, avgNode, buffer2))

	err = avgNode.Merge(buffer1, buffer2)
	require.NoError(err)
	require.Equal(float64(5.2), eval(t, avgNode, buffer1))
}

func TestAvg_NULL(t *testing.T) {
	require := require.New(t)

	avgNode := NewAvg(NewGetField(0, sql.Uint64, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(buffer))

	err := avgNode.Update(buffer, sql.NewRow(nil))
	require.NoError(err)
	require.Equal(nil, eval(t, avgNode, buffer))
}
