package function

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

func TestStrToDate(t *testing.T) {
	setupTimezone(t)

	testCases := [...]struct {
		name     string
		dateStr  string
		fmtStr   string
		expected interface{}
	}{
		{"standard", "Dec 26, 2000 2:13:15", "%b %e, %Y %T", time.Date(2000, time.December, 26, 2, 13, 15, 0, time.UTC)},
		{"ymd", "20240101", "%Y%m%d", time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)},
		{"ymd", "2024121", "%Y%m%d", time.Date(2024, time.December, 1, 0, 0, 0, 0, time.UTC)},
		{"ymd", "20241301", "%Y%m%d", nil},
		{"ymd", "20240001", "%Y%m%d", nil},
		{"ymd-with-time", "2024010203:04:05", "%Y%m%d%T", time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)},
		{"ymd-with-time", "202408122:03:04", "%Y%m%d%T", time.Date(2024, time.August, 12, 2, 3, 4, 0, time.UTC)},
		// TODO: It shoud be nil, but returns "2024-02-31"
		// {"ymd", "20240231", "%Y%m%d", nil},
	}

	for _, tt := range testCases {
		f := NewStrToDate(
			expression.NewGetField(0, types.Text, "", true),
			expression.NewGetField(1, types.Text, "", true),
		)
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
		f := NewStrToDate(
			expression.NewGetField(0, types.Text, "", true),
			expression.NewGetField(1, types.Text, "", true),
		)
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
