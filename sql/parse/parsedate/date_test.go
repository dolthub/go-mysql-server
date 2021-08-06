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

		{"month_number", "03: 3, 20", "%m: %e, %y", "2020-03-03 00:00:00 -0600 CST"},
		{"month_name", "march: 3, 20", "%M: %e, %y", "2020-03-03 00:00:00 -0600 CST"},
		{"month_name_2", "january: 3, 20", "%M: %e, %y", "2020-01-03 00:00:00 -0600 CST"},
		{"month_name_2", "january: 3, 70", "%M: %e, %y", "1970-01-03 00:00:00 -0600 CST"},
		{"month_name_2", "january: 3, 69", "%M: %e, %y", "2069-01-03 00:00:00 -0600 CST"},

		{"microseconds", "01/02/99 314", "%m/%e/%y %f", "1999-01-02 00:00:00.000314 -0600 CST"},
		{"hour_number", "01/02/99 5:14", "%m/%e/%y %h:%i", "1999-01-02 05:14:00 -0600 CST"},
		{"hour_number", "01/02/99 5:14", "%m/%e/%y %I:%i", "1999-01-02 05:14:00 -0600 CST"},

		{"timestamp", "01/02/99 05:14:12 PM", "%m/%e/%y %r", "1999-01-02 17:14:12 -0600 CST"},
		{"date_with_seconds", "01/02/99 57", "%m/%e/%y %S", "1999-01-02 00:00:57 -0600 CST"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := ParseDateWithFormat(tt.date, tt.format)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual.(time.Time).String())
		})
	}
}
