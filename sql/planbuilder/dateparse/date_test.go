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
		expected string
	}{
		{"simple", "Jan 3, 2000", "%b %e, %Y", "2000-01-03"},
		{"simple_with_spaces", "Nov  03 ,   2000", "%b %e, %Y", "2000-11-03"},
		{"simple_with_spaces_2", "Dec  15 ,   2000", "%b %e, %Y", "2000-12-15"},
		{"reverse", "2023/Feb/ 1", "%Y/%b/%e", "2023-02-01"},
		{"reverse_with_spaces", " 2023 /Apr/ 01  ", "%Y/%b/%e", "2023-04-01"},
		{"weekday", "Thu, Aug 5, 2021", "%a, %b %e, %Y", "2021-08-05"},
		{"weekday", "Fri, Aug 6, 2021", "%a, %b %e, %Y", "2021-08-06"},
		{"weekday", "Sat, Aug 7, 2021", "%a, %b %e, %Y", "2021-08-07"},
		{"weekday", "Sun, Aug 8, 2021", "%a, %b %e, %Y", "2021-08-08"},
		{"weekday", "Mon, Aug 9, 2021", "%a, %b %e, %Y", "2021-08-09"},
		{"weekday", "Tue, Aug 10, 2021", "%a, %b %e, %Y", "2021-08-10"},
		{"weekday", "Wed, Aug 11, 2021", "%a, %b %e, %Y", "2021-08-11"},

		{"time_only", "22:23:00", "%H:%i:%s", "22:23:00"},
		{"with_time", "Sep 3, 22:23:00 2000", "%b %e, %H:%i:%s %Y", "2000-09-03 22:23:00"},
		{"with_pm", "May 3, 10:23:00 PM 2000", "%b %e, %h:%i:%s %p %Y", "2000-05-03 22:23:00"},
		{"lowercase_pm", "Jul 3, 10:23:00 pm 2000", "%b %e, %h:%i:%s %p %Y", "2000-07-03 22:23:00"},
		{"with_am", "Mar 3, 10:23:00 am 2000", "%b %e, %h:%i:%s %p %Y", "2000-03-03 10:23:00"},

		{"month_number", "1 3, 10:23:00 pm 2000", "%c %e, %h:%i:%s %p %Y", "2000-01-03 22:23:00"},

		{"day_with_suffix", "Jun 3rd, 10:23:00 pm 2000", "%b %D, %h:%i:%s %p %Y", "2000-06-03 22:23:00"},
		{"day_with_suffix_2", "Oct 21st, 10:23:00 pm 2000", "%b %D, %h:%i:%s %p %Y", "2000-10-21 22:23:00"},
		{"with_timestamp", "01/02/2003, 12:13:14", "%c/%d/%Y, %T", "2003-01-02 12:13:14"},

		{"month_number", "03: 3, 20", "%m: %e, %y", "2020-03-03"},
		{"month_name", "march: 3, 20", "%M: %e, %y", "2020-03-03"},
		{"two_digit_date", "january: 3, 20", "%M: %e, %y", "2020-01-03"},
		{"two_digit_date_2000", "september: 3, 70", "%M: %e, %y", "1970-09-03"},
		{"two_digit_date_1900", "may: 3, 69", "%M: %e, %y", "2069-05-03"},

		{"microseconds", "01/02/99 314", "%m/%e/%y %f", "1999-01-02 00:00:00.314000"},
		{"hour_number", "01/02/99 5:14", "%m/%e/%y %h:%i", "1999-01-02 05:14:00"},
		{"hour_number_2", "01/02/99 5:14", "%m/%e/%y %I:%i", "1999-01-02 05:14:00"},

		{"timestamp", "01/02/99 05:14:12 PM", "%m/%e/%y %r", "1999-01-02 17:14:12"},
		{"date_with_seconds", "01/02/99 57", "%m/%e/%y %S", "1999-01-02 00:00:57"},

		{"date_by_year_offset", "100 20", "%j %y", "2020-04-09"},
		{"date_by_year_offset_singledigit_year", "100 5", "%j %y", "2005-04-10"},
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
		{"no_year", "Jan 3", "%b %e", "0000-01-03", ""},
		{"no_day", "Jan 2000", "%b %y", "2020-01-00", ""},
		{"day_of_month_and_day_of_year", "Jan 3, 100 2000", "%b %e, %j %y", "2020-04-09", ""},

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
			Specifier: 'b', Tokens: "jen, 2000", err: fmt.Errorf(`invalid month abbreviation "jen"`)},
		},
		{"bad_weekday", "Ten 1 Jan, 2000", "%a %e %b, %Y", ParseSpecifierErr{
			Specifier: 'a', Tokens: "ten 1 jan, 2000", err: fmt.Errorf(`invalid week abbreviation "ten"`)},
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
