package function

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestStrToDate(t *testing.T) {
	setupTimezone(t)

	testCases := [...]struct {
		name     string
		dateStr  string
		fmtStr   string
		expected string
	}{
		{"standard", "Dec 26, 2000 2:13:15", "%b %e, %Y %T", "2000-12-26 02:13:15"},
	}

	for _, tt := range testCases {
		f, err := NewStrToDate(
			expression.NewGetField(0, types.Text, "", true),
			expression.NewGetField(1, types.Text, "", true),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(tt.name, func(t *testing.T) {
			dtime := eval(t, f, sql.NewRow(tt.dateStr, tt.fmtStr))
			require.Equal(t, tt.expected, dtime)
		})
		req := require.New(t)
		req.True(f.IsNullable())
	}
}

func TestStrToDateFailure(t *testing.T) {
	setupTimezone(t)

	testCases := [...]struct {
		name    string
		dateStr string
		fmtStr  string
	}{
		{"standard", "BadMonth 26, 2000 2:13:15", "%b %e, %Y %T"},
	}

	for _, tt := range testCases {
		f, err := NewStrToDate(
			expression.NewGetField(0, types.Text, "", true),
			expression.NewGetField(1, types.Text, "", true),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(tt.name, func(t *testing.T) {
			dtime := eval(t, f, sql.NewRow(tt.dateStr, tt.fmtStr))
			require.Equal(t, nil, dtime)
		})
		req := require.New(t)
		req.True(f.IsNullable())
	}
}

func setupTimezone(t *testing.T) {
	loc, err := time.LoadLocation("America/Chicago")
	if err != nil {
		t.Fatal(err)
	}
	old := time.Local
	time.Local = loc
	t.Cleanup(func() { time.Local = old })
}
