package function

import (
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"

	errors "gopkg.in/src-d/go-errors.v1"
)

func TestRegexpMatches(t *testing.T) {
	testCases := []struct {
		pattern  interface{}
		text     interface{}
		flags    interface{}
		expected interface{}
		err      *errors.Kind
	}{
		{
			`^foobar(.*)bye$`,
			"foobarhellobye",
			"",
			[]interface{}{"foobarhellobye", "hello"},
			nil,
		},
		{
			"bop",
			"bopbeepbop",
			"",
			[]interface{}{"bop", "bop"},
			nil,
		},
		{
			"bop",
			"bopbeepBop",
			"i",
			[]interface{}{"bop", "Bop"},
			nil,
		},
		{
			"bop",
			"helloworld",
			"",
			nil,
			nil,
		},
		{
			"foo",
			"",
			"",
			nil,
			nil,
		},
		{
			"",
			"",
			"",
			[]interface{}{""},
			nil,
		},
		{
			"bop",
			nil,
			"",
			nil,
			nil,
		},
		{
			"bop",
			"beep",
			nil,
			nil,
			nil,
		},
		{
			nil,
			"bop",
			"",
			nil,
			nil,
		},
		{
			"bop",
			"bopbeepBop",
			"ix",
			nil,
			errInvalidRegexpFlag,
		},
	}

	t.Run("cacheable", func(t *testing.T) {
		for _, tt := range testCases {
			var flags sql.Expression
			if tt.flags != "" {
				flags = expression.NewLiteral(tt.flags, sql.LongText)
			}
			f, err := NewRegexpMatches(
				expression.NewLiteral(tt.text, sql.LongText),
				expression.NewLiteral(tt.pattern, sql.LongText),
				flags,
			)
			require.NoError(t, err)

			t.Run(f.String(), func(t *testing.T) {
				require := require.New(t)
				result, err := f.Eval(sql.NewEmptyContext(), nil)
				if tt.err == nil {
					require.NoError(err)
					require.Equal(tt.expected, result)
				} else {
					require.Error(err)
					require.True(tt.err.Is(err))
				}
			})
		}
	})

	t.Run("not cacheable", func(t *testing.T) {
		for _, tt := range testCases {
			var flags sql.Expression
			if tt.flags != "" {
				flags = expression.NewGetField(2, sql.LongText, "x", false)
			}
			f, err := NewRegexpMatches(
				expression.NewGetField(0, sql.LongText, "x", false),
				expression.NewGetField(1, sql.LongText, "x", false),
				flags,
			)
			require.NoError(t, err)

			t.Run(f.String(), func(t *testing.T) {
				require := require.New(t)
				result, err := f.Eval(sql.NewEmptyContext(), sql.Row{tt.text, tt.pattern, tt.flags})
				if tt.err == nil {
					require.NoError(err)
					require.Equal(tt.expected, result)
				} else {
					require.Error(err)
					require.True(tt.err.Is(err))
				}
			})
		}
	})
}
