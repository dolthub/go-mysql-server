package function

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestSleep(t *testing.T) {
	f := NewSleep(
		expression.NewGetField(0, sql.Int64, "n", false),
	)
	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		waitTime float64
		err      bool
	}{
		{"null input", sql.NewRow(nil), nil, 0, false},
		{"string input", sql.NewRow("foo"), nil, 0, true},
		{"float input", sql.NewRow(3.14), int(0), 3.0, false},
		{"number is zero", sql.NewRow(0), int(0), 0, false},
		{"negative number", sql.NewRow(-4), int(0), 0, false},
		{"positive number", sql.NewRow(4), int(0), 4.0, false},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			t1 := time.Now()
			v, err := f.Eval(ctx, tt.row)
			t2 := time.Now()
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)

				waited := t2.Sub(t1).Seconds()
				require.InDelta(waited, tt.waitTime, 0.1)
			}
		})
	}
}
