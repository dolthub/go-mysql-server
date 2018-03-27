package aggregation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestMax_String(t *testing.T) {
	assert := require.New(t)
	m := NewMax(expression.NewGetField(0, sql.Int32, "field", true))
	assert.Equal("MAX(field)", m.String())
}

func TestMax_Eval_Int32(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewMax(expression.NewGetField(0, sql.Int32, "field", true))
	b := m.NewBuffer()

	m.Update(ctx, b, sql.NewRow(int32(7)))
	m.Update(ctx, b, sql.NewRow(nil))
	m.Update(ctx, b, sql.NewRow(int32(6)))

	v, err := m.Eval(ctx, b)
	assert.NoError(err)
	assert.Equal(int32(7), v)
}

func TestMax_Eval_Text(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewMax(expression.NewGetField(0, sql.Text, "field", true))
	b := m.NewBuffer()

	m.Update(ctx, b, sql.NewRow("a"))
	m.Update(ctx, b, sql.NewRow("A"))
	m.Update(ctx, b, sql.NewRow("b"))

	v, err := m.Eval(ctx, b)
	assert.NoError(err)
	assert.Equal("b", v)
}

func TestMax_Eval_Timestamp(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewMax(expression.NewGetField(0, sql.Timestamp, "field", true))
	b := m.NewBuffer()

	expected, _ := time.Parse(sql.TimestampLayout, "2008-01-02 15:04:05")
	someTime, _ := time.Parse(sql.TimestampLayout, "2007-01-02 15:04:05")
	otherTime, _ := time.Parse(sql.TimestampLayout, "2006-01-02 15:04:05")

	m.Update(ctx, b, sql.NewRow(someTime))
	m.Update(ctx, b, sql.NewRow(expected))
	m.Update(ctx, b, sql.NewRow(otherTime))

	v, err := m.Eval(ctx, b)
	assert.NoError(err)
	assert.Equal(expected, v)
}
func TestMax_Eval_NULL(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewMax(expression.NewGetField(0, sql.Int32, "field", true))
	b := m.NewBuffer()

	m.Update(ctx, b, sql.NewRow(nil))
	m.Update(ctx, b, sql.NewRow(nil))
	m.Update(ctx, b, sql.NewRow(nil))

	v, err := m.Eval(ctx, b)
	assert.NoError(err)
	assert.Equal(nil, v)
}

func TestMax_Eval_Empty(t *testing.T) {
	assert := require.New(t)
	ctx := sql.NewEmptyContext()

	m := NewMax(expression.NewGetField(0, sql.Int32, "field", true))
	b := m.NewBuffer()

	v, err := m.Eval(ctx, b)
	assert.NoError(err)
	assert.Equal(nil, v)
}
