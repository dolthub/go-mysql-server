package function

import (
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAbsValue(t *testing.T) {
	signedTypes := []sql.Type{sql.Int64, sql.Int32, sql.Int24, sql.Int16, sql.Int8}
	unsignedTypes := []sql.Type{sql.Uint64, sql.Uint32, sql.Uint24, sql.Uint16, sql.Uint8}
	floatTypes := []sql.Type{sql.Float64, sql.Float32, sql.MustCreateDecimalType(16,16)}

	testCases := []struct {
		name     string
		types    []sql.Type
		row      sql.Row
		expected interface{}
		err      error
	}{
		{
			"signed types positive int",
			signedTypes,
			sql.NewRow(5),
			5,
			nil,
		},{
			"signed types negative int",
			signedTypes,
			sql.NewRow(-5),
			5,
			nil,
		},
		{
			"unsigned types positive int",
			unsignedTypes,
			sql.NewRow(5),
			5,
			nil,
		},{
			"unsigned types negative int",
			unsignedTypes,
			sql.NewRow(-5),
			5,
			nil,
		},
		{
			"float positive int",
			floatTypes,
			sql.NewRow(5.0),
			5.0,
			nil,
		},{
			"float negative int",
			floatTypes,
			sql.NewRow(-5.0),
			5.0,
			nil,
		},
		{
			"string should return nil",
			[]sql.Type{sql.Text},
			sql.NewRow("test"),
			nil,
			nil,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			for _, sqlType := range test.types {
				f := NewAbsVal(expression.NewGetField(0, sqlType, "blob", true))

				res, err := f.Eval(sql.NewEmptyContext(), test.row)

				if test.err == nil {
					require.NoError(t, err)
					require.Equal(t, test.expected, res)
				} else {
					require.Error(t, err)
				}
			}
		})
	}
}
