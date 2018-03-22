package aggregation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestAvg_String(t *testing.T) {
	require := require.New(t)

	avg := NewAvg(expression.NewGetField(0, sql.Int32, "col1", true))
	require.Equal("AVG(col1)", avg.String())
}

func TestAvg_Eval_INT32(t *testing.T) {
	require := require.New(t)
	session := sql.NewBaseSession(context.TODO())

	avgNode := NewAvg(expression.NewGetField(0, sql.Int32, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(session, buffer))

	avgNode.Update(session, buffer, sql.NewRow(int32(1)))
	require.Equal(float64(1), eval(t, avgNode, buffer))

	avgNode.Update(session, buffer, sql.NewRow(int32(2)))
	require.Equal(float64(1.5), eval(t, avgNode, buffer))
}

func TestAvg_Eval_UINT64(t *testing.T) {
	require := require.New(t)
	session := sql.NewBaseSession(context.TODO())

	avgNode := NewAvg(expression.NewGetField(0, sql.Uint64, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(session, buffer))

	err := avgNode.Update(session, buffer, sql.NewRow(uint64(1)))
	require.NoError(err)
	require.Equal(float64(1), eval(t, avgNode, buffer))

	err = avgNode.Update(session, buffer, sql.NewRow(uint64(2)))
	require.NoError(err)
	require.Equal(float64(1.5), eval(t, avgNode, buffer))
}

func TestAvg_Eval_NoNum(t *testing.T) {
	require := require.New(t)
	session := sql.NewBaseSession(context.TODO())

	avgNode := NewAvg(expression.NewGetField(0, sql.Text, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(session, buffer))

	err := avgNode.Update(session, buffer, sql.NewRow("foo"))
	require.NoError(err)
	require.Equal(float64(0), eval(t, avgNode, buffer))
}

func TestAvg_Merge(t *testing.T) {
	require := require.New(t)
	session := sql.NewBaseSession(context.TODO())

	avgNode := NewAvg(expression.NewGetField(0, sql.Float32, "col1", true))
	require.NotNil(avgNode)

	buffer1 := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(session, buffer1))
	err := avgNode.Update(session, buffer1, sql.NewRow(float32(1)))
	require.NoError(err)
	err = avgNode.Update(session, buffer1, sql.NewRow(float32(4)))
	require.NoError(err)
	require.Equal(float64(2.5), eval(t, avgNode, buffer1))

	buffer2 := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(session, buffer2))
	err = avgNode.Update(session, buffer2, sql.NewRow(float32(2)))
	require.NoError(err)
	err = avgNode.Update(session, buffer2, sql.NewRow(float32(7)))
	require.NoError(err)
	err = avgNode.Update(session, buffer2, sql.NewRow(float32(12)))
	require.NoError(err)
	require.Equal(float64(7), eval(t, avgNode, buffer2))

	err = avgNode.Merge(session, buffer1, buffer2)
	require.NoError(err)
	require.Equal(float64(5.2), eval(t, avgNode, buffer1))
}

func TestAvg_NULL(t *testing.T) {
	require := require.New(t)
	session := sql.NewBaseSession(context.TODO())

	avgNode := NewAvg(expression.NewGetField(0, sql.Uint64, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(session, buffer))

	err := avgNode.Update(session, buffer, sql.NewRow(nil))
	require.NoError(err)
	require.Equal(nil, eval(t, avgNode, buffer))
}
