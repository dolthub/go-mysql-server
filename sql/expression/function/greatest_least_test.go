package function

import (
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"testing"
)

func TestGreatest(t *testing.T) {

	// XXX
	//table := mem.NewTable("foo", sql.Schema{
	//	{Name: "str_no_num", Type: sql.Text, Source: "foo"},
	//	{Name: "str_num1", Type: sql.Text, Source: "foo"},
	//	{Name: "str_num9", Type: sql.Text, Source: "foo"},
	//	{Name: "int1", Type: sql.Int64, Source: "foo"},
	//	{Name: "int5", Type: sql.Int64, Source: "foo"},
	//	{Name: "float2", Type: sql.Float64, Source: "foo"},
	//	{Name: "float9", Type: sql.Float64, Source: "foo"},
	//})

	testCases := []struct {
		name string
		args []sql.Expression
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
				expression.NewLiteral(string("9"), sql.Text),
				expression.NewLiteral(int64(5), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			},
			float64(9),
		},
		{
            "unconvertible string mixed ignored",
            []sql.Expression{
				expression.NewLiteral(string("10.5"), sql.Text),
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
				expression.NewLiteral("aaa", sql.Text),
				expression.NewLiteral("bbb", sql.Text),
				expression.NewLiteral("9999", sql.Text),
				expression.NewLiteral("", sql.Text),
			},
			"bbb",
		},
		{
			"all strings and empty",
			[]sql.Expression{
				expression.NewLiteral("aaa", sql.Text),
				expression.NewLiteral("bbb", sql.Text),
				expression.NewLiteral("9999", sql.Text),
				expression.NewLiteral("", sql.Text),
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

func TestLeast(t *testing.T) {
	testCases := []struct {
		name string
		args []sql.Expression
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
				expression.NewLiteral(string("10"), sql.Text),
				expression.NewLiteral(int64(5), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64),
			},
			float64(1),
		},
		{
			"unconvertible string mixed ignored",
			[]sql.Expression{
				expression.NewLiteral(string("10.5"), sql.Text),
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
				expression.NewLiteral("aaa", sql.Text),
				expression.NewLiteral("bbb", sql.Text),
				expression.NewLiteral("9999", sql.Text),
			},
			"9999",
		},
		{
			"all strings and empty",
			[]sql.Expression{
				expression.NewLiteral("aaa", sql.Text),
				expression.NewLiteral("bbb", sql.Text),
				expression.NewLiteral("9999", sql.Text),
				expression.NewLiteral("", sql.Text),
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
