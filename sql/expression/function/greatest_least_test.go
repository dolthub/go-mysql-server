package function

import (
	"testing"
	"unsafe"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestGreatest(t *testing.T) {
	testCases := []struct {
		name     string
		args     []sql.Expression
		expected interface{}
	}{
		{
			"null",
			[]sql.Expression{
				expression.NewLiteral(nil, sql.Null),
				expression.NewLiteral(5, sql.Int64),
				expression.NewLiteral(1, sql.Int64),
			},
			nil,
		},
		{
			"negative and all ints",
			[]sql.Expression{
				expression.NewLiteral(int64(-1), sql.Int64),
				expression.NewLiteral(int64(5), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			},
			int64(5),
		},
		{
			"string mixed",
			[]sql.Expression{
				expression.NewLiteral(string("9"), sql.LongText),
				expression.NewLiteral(int64(5), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			},
			float64(9),
		},
		{
			"unconvertible string mixed ignored",
			[]sql.Expression{
				expression.NewLiteral(string("10.5"), sql.LongText),
				expression.NewLiteral(string("foobar"), sql.Int64),
				expression.NewLiteral(int64(5), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			},
			float64(10.5),
		},
		{
			"float mixed",
			[]sql.Expression{
				expression.NewLiteral(float64(10.0), sql.Float64),
				expression.NewLiteral(int(5), sql.Int64),
				expression.NewLiteral(int(1), sql.Int64),
			},
			float64(10.0),
		},
		{
			"all strings",
			[]sql.Expression{
				expression.NewLiteral("aaa", sql.LongText),
				expression.NewLiteral("bbb", sql.LongText),
				expression.NewLiteral("9999", sql.LongText),
				expression.NewLiteral("", sql.LongText),
			},
			"bbb",
		},
		{
			"all strings and empty",
			[]sql.Expression{
				expression.NewLiteral("aaa", sql.LongText),
				expression.NewLiteral("bbb", sql.LongText),
				expression.NewLiteral("9999", sql.LongText),
				expression.NewLiteral("", sql.LongText),
			},
			"bbb",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			f, err := NewGreatest(tt.args...)
			require.NoError(err)

			output, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(err)
			require.Equal(tt.expected, output)
		})
	}
}

func TestGreatestUnsignedOverflow(t *testing.T) {
	require := require.New(t)

	var x int
	var gr sql.Expression
	var err error

	switch unsafe.Sizeof(x) {
	case 4:
		gr, err = NewGreatest(
			expression.NewLiteral(int32(1), sql.Int32),
			expression.NewLiteral(uint32(4294967295), sql.Uint32),
		)
		require.NoError(err)
	case 8:
		gr, err = NewGreatest(
			expression.NewLiteral(int64(1), sql.Int64),
			expression.NewLiteral(uint64(18446744073709551615), sql.Uint64),
		)
		require.NoError(err)
	default:
		// non 32/64 bits??
		return
	}

	_, err = gr.Eval(sql.NewEmptyContext(), nil)
	require.EqualError(err, "Unsigned integer too big to fit on signed integer")
}

func TestLeast(t *testing.T) {
	testCases := []struct {
		name     string
		args     []sql.Expression
		expected interface{}
	}{
		{
			"null",
			[]sql.Expression{
				expression.NewLiteral(nil, sql.Null),
				expression.NewLiteral(5, sql.Int64),
				expression.NewLiteral(1, sql.Int64),
			},
			nil,
		},
		{
			"negative and all ints",
			[]sql.Expression{
				expression.NewLiteral(int64(-1), sql.Int64),
				expression.NewLiteral(int64(5), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			},
			int64(-1),
		},
		{
			"string mixed",
			[]sql.Expression{
				expression.NewLiteral(string("10"), sql.LongText),
				expression.NewLiteral(int64(5), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			},
			float64(1),
		},
		{
			"unconvertible string mixed ignored",
			[]sql.Expression{
				expression.NewLiteral(string("10.5"), sql.LongText),
				expression.NewLiteral(string("foobar"), sql.Int64),
				expression.NewLiteral(int64(5), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			},
			float64(1),
		},
		{
			"float mixed",
			[]sql.Expression{
				expression.NewLiteral(float64(10.0), sql.Float64),
				expression.NewLiteral(int(5), sql.Int64),
				expression.NewLiteral(int(1), sql.Int64),
			},
			float64(1.0),
		},
		{
			"all strings",
			[]sql.Expression{
				expression.NewLiteral("aaa", sql.LongText),
				expression.NewLiteral("bbb", sql.LongText),
				expression.NewLiteral("9999", sql.LongText),
			},
			"9999",
		},
		{
			"all strings and empty",
			[]sql.Expression{
				expression.NewLiteral("aaa", sql.LongText),
				expression.NewLiteral("bbb", sql.LongText),
				expression.NewLiteral("9999", sql.LongText),
				expression.NewLiteral("", sql.LongText),
			},
			"",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			f, err := NewLeast(tt.args...)
			require.NoError(err)

			output, err := f.Eval(sql.NewEmptyContext(), nil)
			require.NoError(err)
			require.Equal(tt.expected, output)
		})
	}
}
