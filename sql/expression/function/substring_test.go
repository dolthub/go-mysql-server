package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestSubstring(t *testing.T) {
	f, err := NewSubstring(
		expression.NewGetField(0, sql.Text, "str", true),
		expression.NewGetField(1, sql.Int32, "start", false),
		expression.NewGetField(2, sql.Int64, "len", false),
	)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"null string", sql.NewRow(nil, 1, 1), nil, false},
		{"null start", sql.NewRow("foo", nil, 1), nil, false},
		{"null len", sql.NewRow("foo", 1, nil), nil, false},
		{"negative start", sql.NewRow("foo", -1, 10), "o", false},
		{"negative length", sql.NewRow("foo", 1, -1), "", false},
		{"length 0", sql.NewRow("foo", 1, 0), "", false},
		{"start bigger than string", sql.NewRow("foo", 50, 10), "", false},
		{"negative start bigger than string", sql.NewRow("foo", -4, 10), "", false},
		{"length overflows", sql.NewRow("foo", 2, 10), "oo", false},
		{"length overflows by one", sql.NewRow("foo", 2, 2), "oo", false},
		{"substring contained", sql.NewRow("foo", 1, 2), "fo", false},
		{"negative start until str beginning", sql.NewRow("foo", -3, 2), "fo", false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()

			v, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, v)
			}
		})
	}
}
