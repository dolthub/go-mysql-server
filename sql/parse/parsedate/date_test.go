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
		{"simple_with_spaces", "Jan  03 ,   2000", "%b %e, %Y", "2000-01-03 00:00:00 -0600 CST"},
		{"simple_with_spaces", "Jan  15 ,   2000", "%b %e, %Y", "2000-01-15 00:00:00 -0600 CST"},
		{"reverse", "2023/Apr/ 1", "%Y/%b/%e", "2023-04-01 00:00:00 -0500 CDT"},
		{"reverse_with_spaces", " 2023 /Apr/ 01  ", "%Y/%b/%e", "2023-04-01 00:00:00 -0500 CDT"},
		{"weekday", "Thu, Aug 5, 2021", "%a, %b %e, %Y", "2021-08-05 00:00:00 -0500 CDT"},

		// with time
		{"with_time", "Jan 3, 22:23:00 2000", "%b %e, %H:%i:%s %Y", "2000-01-03 22:23:00 -0600 CST"},
		{"with_pm", "Jan 3, 10:23:00 PM 2000", "%b %e, %H:%i:%s %p %Y", "2000-01-03 22:23:00 -0600 CST"},
		{"lowercase_pm", "Jan 3, 10:23:00 pm 2000", "%b %e, %H:%i:%s %p %Y", "2000-01-03 22:23:00 -0600 CST"},

		{"month_number", "1 3, 10:23:00 pm 2000", "%c %e, %H:%i:%s %p %Y", "2000-01-03 22:23:00 -0600 CST"},

		{"day_with_suffix", "Jan 3rd, 10:23:00 pm 2000", "%b %D, %H:%i:%s %p %Y", "2000-01-03 22:23:00 -0600 CST"},
		{"day_with_suffix_2", "Jan 21st, 10:23:00 pm 2000", "%b %D, %H:%i:%s %p %Y", "2000-01-21 22:23:00 -0600 CST"},
		{"with_timestamp", "01/02/2003, 12:13:14", "%c/%d/%Y, %T", "2003-01-02 12:13:14 -0600 CST"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := ParseDateWithFormat(tt.date, tt.format)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual.(time.Time).String())
		})
	}
}
