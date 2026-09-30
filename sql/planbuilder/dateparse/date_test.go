package dateparse

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseDate(t *testing.T) {
	setupTimezone(t)

	tests := [...]struct {
		name     string
		date     string
		format   string
		expected interface{}
	}{
		{
			name:     "simple",
			date:     "Jan 3, 2000",
			format:   "%b %e, %Y",
			expected: time.Date(2000, time.January, 3, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "simple_with_spaces",
			date:     "Nov  03 ,   2000",
			format:   "%b %e, %Y",
			expected: time.Date(2000, time.November, 3, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "simple_with_spaces_2",
			date:     "Dec  15 ,   2000",
			format:   "%b %e, %Y",
			expected: time.Date(2000, time.December, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "reverse",
			date:     "2023/Feb/ 1",
			format:   "%Y/%b/%e",
			expected: time.Date(2023, time.February, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "reverse_with_spaces",
			date:     " 2023 /Apr/ 01  ",
			format:   "%Y/%b/%e",
			expected: time.Date(2023, time.April, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "weekday",
			date:     "Thu, Aug 5, 2021",
			format:   "%a, %b %e, %Y",
			expected: time.Date(2021, time.August, 5, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "weekday",
			date:     "Fri, Aug 6, 2021",
			format:   "%a, %b %e, %Y",
			expected: time.Date(2021, time.August, 6, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "weekday",
			date:     "Sat, Aug 7, 2021",
			format:   "%a, %b %e, %Y",
			expected: time.Date(2021, time.August, 7, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "weekday",
			date:     "Sun, Aug 8, 2021",
			format:   "%a, %b %e, %Y",
			expected: time.Date(2021, time.August, 8, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "weekday",
			date:     "Mon, Aug 9, 2021",
			format:   "%a, %b %e, %Y",
			expected: time.Date(2021, time.August, 9, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "weekday",
			date:     "Tue, Aug 10, 2021",
			format:   "%a, %b %e, %Y",
			expected: time.Date(2021, time.August, 10, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "weekday",
			date:     "Wed, Aug 11, 2021",
			format:   "%a, %b %e, %Y",
			expected: time.Date(2021, time.August, 11, 0, 0, 0, 0, time.UTC),
		},

		{
			name:     "time_only",
			date:     "22:23:00",
			format:   "%H:%i:%s",
			expected: time.Date(-1, time.November, 30, 22, 23, 0, 0, time.UTC),
		},
		{
			name:     "with_time",
			date:     "Sep 3, 22:23:00 2000",
			format:   "%b %e, %H:%i:%s %Y",
			expected: time.Date(2000, time.September, 3, 22, 23, 0, 0, time.UTC),
		},
		{
			name:     "with_pm",
			date:     "May 3, 10:23:00 PM 2000",
			format:   "%b %e, %h:%i:%s %p %Y",
			expected: time.Date(2000, time.May, 3, 22, 23, 0, 0, time.UTC),
		},
		{
			name:     "lowercase_pm",
			date:     "Jul 3, 10:23:00 pm 2000",
			format:   "%b %e, %h:%i:%s %p %Y",
			expected: time.Date(2000, time.July, 3, 22, 23, 0, 0, time.UTC),
		},
		{
			name:     "with_am",
			date:     "Mar 3, 10:23:00 am 2000",
			format:   "%b %e, %h:%i:%s %p %Y",
			expected: time.Date(2000, time.March, 3, 10, 23, 0, 0, time.UTC),
		},
		{
			name:     "midnight",
			date:     "12:00 AM",
			format:   "%h:%i %p",
			expected: time.Date(-1, time.November, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "noon",
			date:     "12:00 PM",
			format:   "%h:%i %p",
			expected: time.Date(-1, time.November, 30, 12, 0, 0, 0, time.UTC),
		},

		{
			name:     "month_number",
			date:     "1 3, 10:23:00 pm 2000",
			format:   "%c %e, %h:%i:%s %p %Y",
			expected: time.Date(2000, time.January, 3, 22, 23, 0, 0, time.UTC),
		},

		{
			name:     "day_with_suffix",
			date:     "Jun 3rd, 10:23:00 pm 2000",
			format:   "%b %D, %h:%i:%s %p %Y",
			expected: time.Date(2000, time.June, 3, 22, 23, 0, 0, time.UTC),
		},
		{
			name:     "day_with_suffix_2",
			date:     "Oct 21st, 10:23:00 pm 2000",
			format:   "%b %D, %h:%i:%s %p %Y",
			expected: time.Date(2000, time.October, 21, 22, 23, 0, 0, time.UTC),
		},
		{
			name:     "with_timestamp",
			date:     "01/02/2003, 12:13:14",
			format:   "%c/%d/%Y, %T",
			expected: time.Date(2003, time.January, 2, 12, 13, 14, 0, time.UTC),
		},

		{
			name:     "month_number",
			date:     "03: 3, 20",
			format:   "%m: %e, %y",
			expected: time.Date(2020, time.March, 3, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "month_name",
			date:     "march: 3, 20",
			format:   "%M: %e, %y",
			expected: time.Date(2020, time.March, 3, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "two_digit_date",
			date:     "january: 3, 20",
			format:   "%M: %e, %y",
			expected: time.Date(2020, time.January, 3, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "two_digit_date_2000",
			date:     "september: 3, 70",
			format:   "%M: %e, %y",
			expected: time.Date(1970, time.September, 3, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "two_digit_date_1900",
			date:     "may: 3, 69",
			format:   "%M: %e, %y",
			expected: time.Date(2069, time.May, 3, 0, 0, 0, 0, time.UTC),
		},

		{
			name:     "microseconds",
			date:     "01/02/99 314",
			format:   "%m/%e/%y %f",
			expected: time.Date(1999, time.January, 2, 0, 0, 0, 314000, time.UTC),
		},
		{
			name:     "hour_number",
			date:     "01/02/99 5:14",
			format:   "%m/%e/%y %h:%i",
			expected: time.Date(1999, time.January, 2, 5, 14, 0, 0, time.UTC),
		},
		{
			name:     "hour_number_2",
			date:     "01/02/99 5:14",
			format:   "%m/%e/%y %I:%i",
			expected: time.Date(1999, time.January, 2, 5, 14, 0, 0, time.UTC),
		},

		{
			name:     "timestamp",
			date:     "01/02/99 05:14:12 PM",
			format:   "%m/%e/%y %r",
			expected: time.Date(1999, time.January, 2, 17, 14, 12, 0, time.UTC),
		},
		{
			name:     "date_with_seconds",
			date:     "01/02/99 57",
			format:   "%m/%e/%y %S",
			expected: time.Date(1999, time.January, 2, 0, 0, 57, 0, time.UTC),
		},

		{
			name:     "date_by_year_offset",
			date:     "100 20",
			format:   "%j %y",
			expected: time.Date(2020, time.April, 9, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "date_by_year_offset_singledigit_year",
			date:     "100 5",
			format:   "%j %y",
			expected: time.Date(2005, time.April, 10, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := ParseDateWithFormat(tt.date, tt.format)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestParseDateTwelveHourClock(t *testing.T) {
	setupTimezone(t)

	tests := [...]struct {
		name     string
		date     string
		format   string
		expected interface{}
	}{
		// %p moves the hour into the afternoon, and 12 PM stays at noon.
		{
			name:     "pm_afternoon",
			date:     "01:02 PM",
			format:   "%h:%i %p",
			expected: time.Date(-1, time.November, 30, 13, 2, 0, 0, time.UTC),
		},
		{
			name:     "pm_noon",
			date:     "12:02 PM",
			format:   "%h:%i %p",
			expected: time.Date(-1, time.November, 30, 12, 2, 0, 0, time.UTC),
		},
		{
			name:     "pm_late",
			date:     "11:02 PM",
			format:   "%h:%i %p",
			expected: time.Date(-1, time.November, 30, 23, 2, 0, 0, time.UTC),
		},
		{
			name:     "am_morning",
			date:     "01:02 AM",
			format:   "%h:%i %p",
			expected: time.Date(-1, time.November, 30, 1, 2, 0, 0, time.UTC),
		},
		{
			name:     "am_midnight",
			date:     "12:02 AM",
			format:   "%h:%i %p",
			expected: time.Date(-1, time.November, 30, 0, 2, 0, 0, time.UTC),
		},
		{
			name:     "pm_lowercase",
			date:     "01:02 pm",
			format:   "%h:%i %p",
			expected: time.Date(-1, time.November, 30, 13, 2, 0, 0, time.UTC),
		},
		{
			name:     "capital_i_specifier",
			date:     "01:02 PM",
			format:   "%I:%i %p",
			expected: time.Date(-1, time.November, 30, 13, 2, 0, 0, time.UTC),
		},
		{
			name:     "lowercase_l_specifier",
			date:     "1:02 PM",
			format:   "%l:%i %p",
			expected: time.Date(-1, time.November, 30, 13, 2, 0, 0, time.UTC),
		},

		// %r carries its own AM/PM marker.
		{
			name:     "r_pm",
			date:     "05:14:12 PM",
			format:   "%r",
			expected: time.Date(-1, time.November, 30, 17, 14, 12, 0, time.UTC),
		},
		{
			name:     "r_am",
			date:     "05:14:12 AM",
			format:   "%r",
			expected: time.Date(-1, time.November, 30, 5, 14, 12, 0, time.UTC),
		},
		{
			name:     "r_noon",
			date:     "12:14:12 PM",
			format:   "%r",
			expected: time.Date(-1, time.November, 30, 12, 14, 12, 0, time.UTC),
		},
		{
			name:     "r_midnight",
			date:     "12:14:12 AM",
			format:   "%r",
			expected: time.Date(-1, time.November, 30, 0, 14, 12, 0, time.UTC),
		},

		// Without %p a 12-hour specifier still wraps 12 to 0, as MySQL does.
		{
			name:     "twelve_without_marker",
			date:     "12:34",
			format:   "%h:%i",
			expected: time.Date(-1, time.November, 30, 0, 34, 0, 0, time.UTC),
		},
		{
			name:     "one_without_marker",
			date:     "01:34",
			format:   "%h:%i",
			expected: time.Date(-1, time.November, 30, 1, 34, 0, 0, time.UTC),
		},

		{
			name:     "pm_with_date",
			date:     "May 3, 10:23:00 PM 2000",
			format:   "%b %e, %h:%i:%s %p %Y",
			expected: time.Date(2000, time.May, 3, 22, 23, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := ParseDateWithFormat(tt.date, tt.format)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestParseDateTwelveHourClockOutOfRange(t *testing.T) {
	setupTimezone(t)

	// MySQL rejects an hour outside 1..12 for the 12-hour specifiers, which
	// makes STR_TO_DATE return NULL rather than a silently wrong time.
	tests := [...]struct {
		name   string
		date   string
		format string
	}{
		{name: "thirteen_pm", date: "13:02 PM", format: "%h:%i %p"},
		{name: "zero_am", date: "00:02 AM", format: "%h:%i %p"},
		{name: "thirteen_no_marker", date: "13:02", format: "%h:%i"},
		{name: "zero_no_marker", date: "00:02", format: "%I:%i"},
		{name: "r_thirteen", date: "13:14:12 PM", format: "%r"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseDateWithFormat(tt.date, tt.format)
			require.Error(t, err)
		})
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

func TestConversionFailure(t *testing.T) {
	tests := [...]struct {
		name          string
		date          string
		format        string
		result        interface{}
		expectedError string
	}{
		// with strict mode with NO_ZERO_IN_DATE,NO_ZERO_DATE enabled, these tests result NULL
		{"no_year", "Jan 3", "%b %e", time.Date(0, time.January, 3, 0, 0, 0, 0, time.UTC), ""},
		{"no_day", "Jan 2000", "%b %Y", time.Date(2000, time.January, 0, 0, 0, 0, 0, time.UTC), ""},
		{"day_of_month_and_day_of_year", "Jan 3, 100 2000", "%b %e, %j %Y", time.Date(2000, time.April, 9, 0, 0, 0, 0, time.UTC), ""},

		{"24hour_time_with_pm", "May 3, 10:23:00 PM 2000", "%b %e, %H:%i:%s %p %Y", nil, "cannot use 24 hour time (H) with AM/PM (p)"},
		{"specifier_end_of_line", "Jan 3", "%b %e %", nil, `"%" found at end of format string`},
		{"unknown_format_specifier", "Jan 3", "%b %e %L", nil, `unknown format specifier "L"`},
		{"invalid_number_hour", "0021:12:14", "%T", nil, `specifier %T failed to parse "0021:12:14": expected literal ":", got "2"`},
		{"invalid_number_hour_2", "0012:12:14", "%r", nil, `specifier %r failed to parse "0012:12:14": expected literal ":", got "1"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := ParseDateWithFormat(tt.date, tt.format)
			if tt.expectedError != "" {
				require.Error(t, err)
				require.Equal(t, tt.expectedError, err.Error())
			} else {
				require.Equal(t, tt.result, r)
			}
		})
	}
}

func TestParseErr(t *testing.T) {
	tests := [...]struct {
		name          string
		date          string
		format        string
		expectedError interface{}
	}{
		{"simple", "a", "b", ParseLiteralErr{
			Literal: 'b', Tokens: "a", err: fmt.Errorf(`expected literal "b", got "a"`)},
		},
		{"bad_numeral", "abc", "%e", ParseSpecifierErr{
			Specifier: 'e', Tokens: "abc", err: fmt.Errorf("strconv.ParseUint: parsing \"\": invalid syntax")},
		},
		{"bad_month", "1 Jen, 2000", "%e %b, %Y", ParseSpecifierErr{
			Specifier: 'b', Tokens: "Jen, 2000", err: fmt.Errorf(`invalid month abbreviation "Jen"`)},
		},
		{"bad_weekday", "Ten 1 Jan, 2000", "%a %e %b, %Y", ParseSpecifierErr{
			Specifier: 'a', Tokens: "Ten 1 Jan, 2000", err: fmt.Errorf(`invalid week abbreviation "Ten"`)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseDateWithFormat(tt.date, tt.format)
			require.Error(t, err)
			require.Equal(t, tt.expectedError.(error).Error(), err.Error())
			require.IsType(t, err, tt.expectedError)
		})
	}
}
