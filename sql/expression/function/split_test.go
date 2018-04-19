package function

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestSplit(t *testing.T) {
	testCases := []struct {
		name      string
		input     interface{}
		delimiter interface{}
		expected  interface{}
	}{
		{"has delimiter", "a-b-c", "-", []interface{}{"a", "b", "c"}},
		{"regexp delimiter", "a--b----c-d", "-+", []interface{}{"a", "b", "c", "d"}},
		{"does not have delimiter", "a.b.c", "-", []interface{}{"a.b.c"}},
		{"input is nil", nil, "-", nil},
		{"delimiter is nil", "a-b-c", nil, nil},
	}

	f := NewSplit(
		expression.NewGetField(0, sql.Text, "input", true),
		expression.NewGetField(1, sql.Text, "delimiter", true),
	)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			v, err := f.Eval(sql.NewEmptyContext(), sql.NewRow(tt.input, tt.delimiter))
			require.NoError(t, err)
			require.Equal(t, tt.expected, v)
		})
	}
}
