package expression

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestConvert(t *testing.T) {
	tests := []struct {
		name        string
		row         sql.Row
		expression  sql.Expression
		castTo      string
		expected    interface{}
		expectedErr bool
	}{
		{
			name:        "convert int32 to signed",
			row:         nil,
			expression:  NewLiteral(int32(1), sql.Int32),
			castTo:      ConvertToSigned,
			expected:    int64(1),
			expectedErr: false,
		},
		{
			name:        "convert int32 to unsigned",
			row:         nil,
			expression:  NewLiteral(int32(-5), sql.Int32),
			castTo:      ConvertToUnsigned,
			expected:    uint64(18446744073709551611),
			expectedErr: false,
		},
		{
			name:        "convert string to signed",
			row:         nil,
			expression:  NewLiteral("-3", sql.Text),
			castTo:      ConvertToSigned,
			expected:    int64(-3),
			expectedErr: false,
		},
		{
			name:        "convert string to unsigned",
			row:         nil,
			expression:  NewLiteral("-3", sql.Int32),
			castTo:      ConvertToUnsigned,
			expected:    uint64(18446744073709551613),
			expectedErr: false,
		},
		{
			name:        "convert int to string",
			row:         nil,
			expression:  NewLiteral(-3, sql.Int32),
			castTo:      ConvertToChar,
			expected:    "-3",
			expectedErr: false,
		},
		{
			name:        "impossible conversion string to unsigned",
			row:         nil,
			expression:  NewLiteral("hello", sql.Text),
			castTo:      ConvertToUnsigned,
			expected:    uint64(0),
			expectedErr: false,
		},
		{
			name:        "imposible conversion string to signed",
			row:         nil,
			castTo:      ConvertToSigned,
			expression:  NewLiteral("A", sql.Text),
			expected:    int64(0),
			expectedErr: false,
		},
		{
			name:        "string to datetime",
			row:         nil,
			expression:  NewLiteral("2017-12-12", sql.Text),
			castTo:      ConvertToDatetime,
			expected:    time.Date(2017, time.December, 12, 0, 0, 0, 0, time.UTC),
			expectedErr: false,
		},
		{
			name:        "impossible conversion string to datetime",
			row:         nil,
			castTo:      ConvertToDatetime,
			expression:  NewLiteral(1, sql.Int32),
			expected:    nil,
			expectedErr: false,
		},
		{
			name:        "string to date",
			row:         nil,
			castTo:      ConvertToDate,
			expression:  NewLiteral("2017-12-12 11:12:13", sql.Int32),
			expected:    time.Date(2017, time.December, 12, 11, 12, 13, 0, time.UTC),
			expectedErr: false,
		},
		{
			name:        "impossible conversion string to date",
			row:         nil,
			castTo:      ConvertToDate,
			expression:  NewLiteral(1, sql.Int32),
			expected:    nil,
			expectedErr: false,
		},
		{
			name:        "float to binary",
			row:         nil,
			castTo:      ConvertToBinary,
			expression:  NewLiteral(float64(-2.3), sql.Float64),
			expected:    []byte("-2.3"),
			expectedErr: false,
		},
		{
			name:        "string to json",
			row:         nil,
			castTo:      ConvertToJSON,
			expression:  NewLiteral(`{"a":2}`, sql.Text),
			expected:    []byte(`{"a":2}`),
			expectedErr: false,
		},
		{
			name:        "int to json",
			row:         nil,
			castTo:      ConvertToJSON,
			expression:  NewLiteral(2, sql.Int32),
			expected:    []byte("2"),
			expectedErr: false,
		},
		{
			name:        "imposible conversion string to json",
			row:         nil,
			castTo:      ConvertToJSON,
			expression:  NewLiteral("3>2", sql.Text),
			expected:    nil,
			expectedErr: true,
		},
		{
			name:        "bool to signed",
			row:         nil,
			castTo:      ConvertToSigned,
			expression:  NewLiteral(true, sql.Boolean),
			expected:    int64(1),
			expectedErr: false,
		},
		{
			name:        "bool to datetime",
			row:         nil,
			castTo:      ConvertToDatetime,
			expression:  NewLiteral(true, sql.Boolean),
			expected:    nil,
			expectedErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			convert := NewConvert(test.expression, test.castTo)
			val, err := convert.Eval(sql.NewEmptyContext(), test.row)
			if test.expectedErr {
				require.Error(err)
			} else {
				require.NoError(err)
			}

			require.Equal(test.expected, val)
		})
	}
}
