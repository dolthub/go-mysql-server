package function

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestJSONExtract(t *testing.T) {
	f2, err := NewJSONExtract(
		expression.NewGetField(0, sql.LongText, "arg1", false),
		expression.NewGetField(1, sql.LongText, "arg2", false),
	)
	require.NoError(t, err)

	f3, err := NewJSONExtract(
		expression.NewGetField(0, sql.LongText, "arg1", false),
		expression.NewGetField(1, sql.LongText, "arg2", false),
		expression.NewGetField(2, sql.LongText, "arg3", false),
	)
	require.NoError(t, err)

	f4, err := NewJSONExtract(
		expression.NewGetField(0, sql.LongText, "arg1", false),
		expression.NewGetField(1, sql.LongText, "arg2", false),
		expression.NewGetField(2, sql.LongText, "arg3", false),
		expression.NewGetField(3, sql.LongText, "arg4", false),
	)
	require.NoError(t, err)

	json := map[string]interface{}{
		"a": []interface{}{1, 2, 3, 4},
		"b": map[string]interface{}{
			"c": "foo",
			"d": true,
		},
		"e": []interface{}{
			[]interface{}{1, 2},
			[]interface{}{3, 4},
		},
	}

	testCases := []struct {
		f        sql.Expression
		row      sql.Row
		expected interface{}
		err      error
	}{
		{f2, sql.Row{json, "FOO"}, nil, errors.New("should start with '$'")},
		{f2, sql.Row{nil, "$.b.c"}, nil, nil},
		{f2, sql.Row{json, "$.foo"}, nil, nil},
		{f2, sql.Row{json, "$.b.c"}, "foo", nil},
		{f3, sql.Row{json, "$.b.c", "$.b.d"}, []interface{}{"foo", true}, nil},
		{f4, sql.Row{json, "$.b.c", "$.b.d", "$.e[0][*]"}, []interface{}{
			"foo",
			true,
			[]interface{}{1., 2.},
		}, nil},
	}

	for _, tt := range testCases {
		t.Run(tt.f.String(), func(t *testing.T) {
			require := require.New(t)
			result, err := tt.f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err == nil {
				require.NoError(err)
			} else {
				require.Equal(err.Error(), tt.err.Error())
			}

			require.Equal(tt.expected, result)
		})
	}
}
