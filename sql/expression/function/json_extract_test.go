package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestJSONExtract(t *testing.T) {
	f2, err := NewJSONExtract(
		expression.NewGetField(0, sql.Text, "arg1", false),
		expression.NewGetField(1, sql.Text, "arg2", false),
	)
	require.NoError(t, err)

	f3, err := NewJSONExtract(
		expression.NewGetField(0, sql.Text, "arg1", false),
		expression.NewGetField(1, sql.Text, "arg2", false),
		expression.NewGetField(2, sql.Text, "arg3", false),
	)
	require.NoError(t, err)

	f4, err := NewJSONExtract(
		expression.NewGetField(0, sql.Text, "arg1", false),
		expression.NewGetField(1, sql.Text, "arg2", false),
		expression.NewGetField(2, sql.Text, "arg3", false),
		expression.NewGetField(3, sql.Text, "arg4", false),
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
	}{
		{f2, sql.Row{json, "$.b.c"}, "foo"},
		{f3, sql.Row{json, "$.b.c", "$.b.d"}, []interface{}{"foo", true}},
		{f4, sql.Row{json, "$.b.c", "$.b.d", "$.e[0][*]"}, []interface{}{
			"foo",
			true,
			[]interface{}{1., 2.},
		}},
	}

	for _, tt := range testCases {
		t.Run(tt.f.String(), func(t *testing.T) {
			require := require.New(t)

			result, err := tt.f.Eval(sql.NewEmptyContext(), tt.row)
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}
