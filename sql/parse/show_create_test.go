package parse

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestParseShowCreateTableQuery(t *testing.T) {
	testCases := []struct {
		query  string
		result sql.Node
		err    *errors.Kind
	}{
		{
			"SHOW CREATE",
			nil,
			errUnsupportedShowCreateQuery,
		},
		{
			"SHOW CREATE ANYTHING",
			nil,
			errUnsupportedShowCreateQuery,
		},
		{
			"SHOW CREATE ASDF foo",
			nil,
			errUnsupportedShowCreateQuery,
		},
		{
			"SHOW CREATE TABLE mytable",
			plan.NewShowCreateTable(sql.UnresolvedDatabase("").Name(),
				nil,
				"mytable"),
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.query, func(t *testing.T) {
			require := require.New(t)

			result, err := parseShowCreate(tt.query)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.result, result)
			}
		})
	}
}
