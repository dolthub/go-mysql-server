package sql_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

var conditions = []struct {
	evaluated bool
	value     interface{}
	t         sql.Type
}{
	{true, int16(1), sql.Int16},
	{false, int16(0), sql.Int16},
	{true, int32(1), sql.Int32},
	{false, int32(0), sql.Int32},
	{true, int(1), sql.Int64},
	{false, int(0), sql.Int64},
	{true, float32(1), sql.Float32},
	{true, float64(1), sql.Float64},
	{false, float32(0), sql.Float32},
	{false, float64(0), sql.Float64},
	{true, float32(0.5), sql.Float32},
	{true, float64(0.5), sql.Float64},
	{true, "1", sql.LongText},
	{false, "0", sql.LongText},
	{false, "foo", sql.LongText},
	{false, "0.5", sql.LongText},
	{false, time.Duration(0), sql.Timestamp},
	{true, time.Duration(1), sql.Timestamp},
	{false, false, sql.Boolean},
	{true, true, sql.Boolean},
}

func TestEvaluateCondition(t *testing.T) {
	for _, v := range conditions {
		t.Run(fmt.Sprint(v.value, " evaluated to ", v.evaluated, " type ", v.t), func(t *testing.T) {
			require := require.New(t)
			b, err := sql.EvaluateCondition(sql.NewEmptyContext(), expression.NewLiteral(v.value, v.t), sql.NewRow())
			require.NoError(err)
			require.Equal(v.evaluated, b)
		})
	}
}
