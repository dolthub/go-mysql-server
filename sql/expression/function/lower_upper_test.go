package function

import (
	"testing"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestLower(t *testing.T) {
	testCases := []struct {
		name     string
		rowType  sql.Type
		row      sql.Row
		expected interface{}
	}{
		{"text nil", sql.LongText, sql.NewRow(nil), nil},
		{"text ok", sql.LongText, sql.NewRow("LoWeR"), "lower"},
		{"binary ok", sql.Blob, sql.NewRow([]byte("LoWeR")), "lower"},
		{"other type", sql.Int32, sql.NewRow(int32(1)), "1"},
	}

	for _, tt := range testCases {
		f := NewLower(expression.NewGetField(0, tt.rowType, "", true))

		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, f, tt.row))
		})

		req := require.New(t)
		req.True(f.IsNullable())
		req.Equal(tt.rowType, f.Type())
	}
}

func TestUpper(t *testing.T) {
	testCases := []struct {
		name     string
		rowType  sql.Type
		row      sql.Row
		expected interface{}
	}{
		{"text nil", sql.LongText, sql.NewRow(nil), nil},
		{"text ok", sql.LongText, sql.NewRow("UpPeR"), "UPPER"},
		{"binary ok", sql.Blob, sql.NewRow([]byte("UpPeR")), "UPPER"},
		{"other type", sql.Int32, sql.NewRow(int32(1)), "1"},
	}

	for _, tt := range testCases {
		f := NewUpper(expression.NewGetField(0, tt.rowType, "", true))

		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, f, tt.row))
		})

		req := require.New(t)
		req.True(f.IsNullable())
		req.Equal(tt.rowType, f.Type())
	}
}
