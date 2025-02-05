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
		{"simple", "Jan 3, 2000", "%b %e, %Y", time.Date(2000, time.January, 3, 0, 0, 0, 0, time.UTC)},
		{"simple_with_spaces", "Nov  03 ,   2000", "%b %e, %Y", time.Date(2000, time.November, 3, 0, 0, 0, 0, time.UTC)},
		{"simple_with_spaces_2", "Dec  15 ,   2000", "%b %e, %Y", time.Date(2000, time.December, 15, 0, 0, 0, 0, time.UTC)},
		{"reverse", "2023/Feb/ 1", "%Y/%b/%e", time.Date(2023, time.February, 1, 0, 0, 0, 0, time.UTC)},
		{"reverse_with_spaces", " 2023 /Apr/ 01  ", "%Y/%b/%e", time.Date(2023, time.April, 1, 0, 0, 0, 0, time.UTC)},
		{"weekday", "Thu, Aug 5, 2021", "%a, %b %e, %Y", time.Date(2021, time.August, 5, 0, 0, 0, 0, time.UTC)},
		{"weekday", "Fri, Aug 6, 2021", "%a, %b %e, %Y", time.Date(2021, time.August, 6, 0, 0, 0, 0, time.UTC)},
		{"weekday", "Sat, Aug 7, 2021", "%a, %b %e, %Y", time.Date(2021, time.August, 7, 0, 0, 0, 0, time.UTC)},
		{"weekday", "Sun, Aug 8, 2021", "%a, %b %e, %Y", time.Date(2021, time.August, 8, 0, 0, 0, 0, time.UTC)},
		{"weekday", "Mon, Aug 9, 2021", "%a, %b %e, %Y", time.Date(2021, time.August, 9, 0, 0, 0, 0, time.UTC)},
		{"weekday", "Tue, Aug 10, 2021", "%a, %b %e, %Y", time.Date(2021, time.August, 10, 0, 0, 0, 0, time.UTC)},
		{"weekday", "Wed, Aug 11, 2021", "%a, %b %e, %Y", time.Date(2021, time.August, 11, 0, 0, 0, 0, time.UTC)},

		{"time_only", "22:23:00", "%H:%i:%s", time.Date(-1, time.November, 30, 22, 23, 0, 0, time.UTC)},
		{"with_time", "Sep 3, 22:23:00 2000", "%b %e, %H:%i:%s %Y", time.Date(2000, time.September, 3, 22, 23, 0, 0, time.UTC)},
		{"with_pm", "May 3, 10:23:00 PM 2000", "%b %e, %h:%i:%s %p %Y", time.Date(2000, time.May, 3, 10, 23, 0, 0, time.UTC)},
		{"lowercase_pm", "Jul 3, 10:23:00 pm 2000", "%b %e, %h:%i:%s %p %Y", time.Date(2000, time.July, 3, 10, 23, 0, 0, time.UTC)},
		{"with_am", "Mar 3, 10:23:00 am 2000", "%b %e, %h:%i:%s %p %Y", time.Date(2000, time.March, 3, 10, 23, 0, 0, time.UTC)},

		{"month_number", "1 3, 10:23:00 pm 2000", "%c %e, %h:%i:%s %p %Y", time.Date(2000, time.January, 3, 10, 23, 0, 0, time.UTC)},

		{"day_with_suffix", "Jun 3rd, 10:23:00 pm 2000", "%b %D, %h:%i:%s %p %Y", time.Date(2000, time.June, 3, 10, 23, 0, 0, time.UTC)},
		{"day_with_suffix_2", "Oct 21st, 10:23:00 pm 2000", "%b %D, %h:%i:%s %p %Y", time.Date(2000, time.October, 21, 10, 23, 0, 0, time.UTC)},
		{"with_timestamp", "01/02/2003, 12:13:14", "%c/%d/%Y, %T", time.Date(2003, time.January, 2, 12, 13, 14, 0, time.UTC)},

		{"month_number", "03: 3, 20", "%m: %e, %y", time.Date(2020, time.March, 3, 0, 0, 0, 0, time.UTC)},
		{"month_name", "march: 3, 20", "%M: %e, %y", time.Date(2020, time.March, 3, 0, 0, 0, 0, time.UTC)},
		{"two_digit_date", "january: 3, 20", "%M: %e, %y", time.Date(2020, time.January, 3, 0, 0, 0, 0, time.UTC)},
		{"two_digit_date_2000", "september: 3, 70", "%M: %e, %y", time.Date(1970, time.September, 3, 0, 0, 0, 0, time.UTC)},
		{"two_digit_date_1900", "may: 3, 69", "%M: %e, %y", time.Date(2069, time.May, 3, 0, 0, 0, 0, time.UTC)},

		{"microseconds", "01/02/99 314", "%m/%e/%y %f", time.Date(1999, time.January, 2, 0, 0, 0, 314000, time.UTC)},
		{"hour_number", "01/02/99 5:14", "%m/%e/%y %h:%i", time.Date(1999, time.January, 2, 5, 14, 0, 0, time.UTC)},
		{"hour_number_2", "01/02/99 5:14", "%m/%e/%y %I:%i", time.Date(1999, time.January, 2, 5, 14, 0, 0, time.UTC)},

		{"timestamp", "01/02/99 05:14:12 PM", "%m/%e/%y %r", time.Date(1999, time.January, 2, 5, 14, 12, 0, time.UTC)},
		{"date_with_seconds", "01/02/99 57", "%m/%e/%y %S", time.Date(1999, time.January, 2, 0, 0, 57, 0, time.UTC)},

		{"date_by_year_offset", "100 20", "%j %y", time.Date(2020, time.April, 9, 0, 0, 0, 0, time.UTC)},
		{"date_by_year_offset_singledigit_year", "100 5", "%j %y", time.Date(2005, time.April, 10, 0, 0, 0, 0, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := ParseDateWithFormat(tt.date, tt.format)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
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
