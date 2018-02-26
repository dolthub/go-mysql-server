package expression

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestIsBinary(t *testing.T) {
	f := NewIsBinary(NewGetField(0, sql.Blob, "blob", true))

	testCases := []struct {
		name     string
		row      sql.Row
		expected bool
	}{
		{"binary", sql.NewRow([]byte{0, 1, 2}), true},
		{"not binary", sql.NewRow([]byte{1, 2, 3}), false},
		{"null", sql.NewRow(nil), false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, f, tt.row))
		})
	}
}
