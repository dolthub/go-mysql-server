package aggregation

import (
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

func TestAvg_Float64(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avg := NewAvg(expression.NewGetField(0, sql.Float64, "col1", true))
	buffer := avg.NewBuffer()
	avg.Update(ctx, buffer, sql.NewRow(float64(23.2220000)))

	require.Equal(float64(23.222), eval(t, avg, buffer))
}

func TestAvg_Eval_INT32(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(expression.NewGetField(0, sql.Int32, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Equal(float64(0), eval(t, avgNode, buffer))

	avgNode.Update(ctx, buffer, sql.NewRow(int32(1)))
	require.Equal(float64(1), eval(t, avgNode, buffer))

	avgNode.Update(ctx, buffer, sql.NewRow(int32(2)))
	require.Equal(float64(1.5), eval(t, avgNode, buffer))
}

func TestAvg_Eval_UINT64(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(expression.NewGetField(0, sql.Uint64, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Equal(float64(0), eval(t, avgNode, buffer))

	err := avgNode.Update(ctx, buffer, sql.NewRow(uint64(1)))
	require.NoError(err)
	require.Equal(float64(1), eval(t, avgNode, buffer))

	err = avgNode.Update(ctx, buffer, sql.NewRow(uint64(2)))
	require.NoError(err)
	require.Equal(float64(1.5), eval(t, avgNode, buffer))
}

func TestAvg_Eval_String(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(expression.NewGetField(0, sql.Text, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Equal(float64(0), eval(t, avgNode, buffer))

	err := avgNode.Update(ctx, buffer, sql.NewRow("foo"))
	require.NoError(err)
	require.Equal(float64(0), eval(t, avgNode, buffer))

	err = avgNode.Update(ctx, buffer, sql.NewRow("2"))
	require.NoError(err)
	require.Equal(float64(1), eval(t, avgNode, buffer))
}

func TestAvg_Merge(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(expression.NewGetField(0, sql.Float32, "col1", true))
	require.NotNil(avgNode)

	buffer1 := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(ctx, buffer1))
	err := avgNode.Update(ctx, buffer1, sql.NewRow(float32(1)))
	require.NoError(err)
	err = avgNode.Update(ctx, buffer1, sql.NewRow(float32(4)))
	require.NoError(err)
	require.Equal(float64(2.5), eval(t, avgNode, buffer1))

	buffer2 := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(ctx, buffer2))
	err = avgNode.Update(ctx, buffer2, sql.NewRow(float32(2)))
	require.NoError(err)
	err = avgNode.Update(ctx, buffer2, sql.NewRow(float32(7)))
	require.NoError(err)
	err = avgNode.Update(ctx, buffer2, sql.NewRow(float32(12)))
	require.NoError(err)
	require.Equal(float64(7), eval(t, avgNode, buffer2))

	err = avgNode.Merge(ctx, buffer1, buffer2)
	require.NoError(err)
	require.Equal(float64(5.2), eval(t, avgNode, buffer1))
}

func TestAvg_NULL(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	avgNode := NewAvg(expression.NewGetField(0, sql.Uint64, "col1", true))
	buffer := avgNode.NewBuffer()
	require.Zero(avgNode.Eval(ctx, buffer))

	err := avgNode.Update(ctx, buffer, sql.NewRow(nil))
	require.NoError(err)
	require.Equal(nil, eval(t, avgNode, buffer))
}
