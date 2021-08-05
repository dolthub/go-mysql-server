package parsedate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseDate(t *testing.T) {
	tests := [...]struct {
		name     string
		date     string
		format   string
		expected string
	}{
		{"simple", "Jan 3, 2000", "%b %e, %Y", "2000-01-03 00:00:00 -0600 CST"},
		{"simple_with_spaces", "Jan  3 ,   2000", "%b %e, %Y", "2000-01-03 00:00:00 -0600 CST"},
		{"reverse", "2023/Apr/ 1", "%Y/%b/%e", "2023-04-01 00:00:00 -0500 CDT"},
		{"reverse_with_spaces", " 2023 /Apr/ 01  ", "%Y/%b/%e", "2023-04-01 00:00:00 -0500 CDT"},
		{"paren_tok", "Jan 3%, 2000", "%b %e%%, %Y", "2000-01-03 00:00:00 -0600 CST"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := ParseDateWithFormat(tt.date, tt.format)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual.(time.Time).String())
		})
	}
}
