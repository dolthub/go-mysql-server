package function

import (
	"testing"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestJSONUnquote(t *testing.T) {
	require := require.New(t)
	js := NewJSONUnquote(expression.NewGetField(0, sql.LongText, "json", false))

	testCases := []struct {
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{sql.Row{nil}, nil, false},
		{sql.Row{"\"abc\""}, `abc`, false},
		{sql.Row{"[1, 2, 3]"}, `[1, 2, 3]`, false},
		{sql.Row{"\"\t\u0032\""}, "\t2", false},
		{sql.Row{"\\"}, nil, true},
	}

	for _, tt := range testCases {
		result, err := js.Eval(sql.NewEmptyContext(), tt.row)

		if !tt.err {
			require.NoError(err)
			require.Equal(tt.expected, result)
		} else {
			require.NotNil(err)
		}
	}
}
