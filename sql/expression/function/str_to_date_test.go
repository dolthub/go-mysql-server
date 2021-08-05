package function

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestStrToDate(t *testing.T) {
	testCases := [...]struct {
		name     string
		dateStr  string
		fmtStr   string
		expected interface{}
		// TODO: add expected error case
	}{
		{"standard", "invaliddate", "%s:%s", nil},
	}

	for _, tt := range testCases {
		f, err := NewStrToDate(sql.NewEmptyContext(),
			expression.NewGetField(0, sql.Text, "", true),
			expression.NewGetField(1, sql.Text, "", true),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, f, sql.NewRow(tt.dateStr, tt.fmtStr)))
		})
		req := require.New(t)
		req.True(f.IsNullable())
	}
}
