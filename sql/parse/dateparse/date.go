package dateparse

import (
	"fmt"
	"strings"
	"time"
)

// ParseDateWithFormat parses the date string according to the given
// format string, as defined in the MySQL specification.
//
// Reference the MySQL docs for valid format specifiers.
// This implementation attempts to match the spec to the extent possible.
//
//
// More info: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format
//
// Even more info: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_str-to-date
func ParseDateWithFormat(date, format string) (time.Time, error) {
	parsers, err := parsersFromFormatString(format)
	if err != nil {
		return time.Time{}, err
	}

	// trim all leading and trailing whitespace
	date = strings.TrimSpace(date)

	// convert to all lowercase
	date = strings.ToLower(date)

	var result datetime
	target := date
	for _, parser := range parsers {
		target = takeAllSpaces(target)
		rest, err := parser(&result, target)
		if err != nil {
			return time.Time{}, err
		}
		target = rest
	}

	return evaluate(result)
}

func parsersFromFormatString(format string) ([]parser, error) {
	i := 0
	parsers := make([]parser, 0)
	for {
		if len(format) <= i {
			break
		}
		if format[i] == '%' {
			if len(format) <= i+1 {
				return nil, fmt.Errorf("\"%%\" found at end of format string")
			}
			specifier := format[i+1]
			parser, ok := formatSpecifiers[format[i+1]]
			if !ok {
				return nil, fmt.Errorf("unknown format specifier \"%c\"", specifier)
			}
			if parser == nil {
				return nil, fmt.Errorf("format specifier \"%c\" not yet supported", specifier)
			}
			// wrap the parser in an error handler to provide rich error types
			parsers = append(parsers, func(result *datetime, chars string) (string, error) {
				rest, err := parser(result, chars)
				if err != nil {
					return "", ParseSpecifierErr{
						Specifier: specifier,
						Tokens:    chars,
						err:       err,
					}
				}
				return rest, nil
			})
			// both the '%' and the specifier are consumed
			i += 2
		} else {
			literal := format[i]
			// wrap the parser in an error handler to provide rich error types
			parsers = append(parsers, func(result *datetime, chars string) (string, error) {
				rest, err := literalParser(literal)(result, chars)
				if err != nil {
					return "", ParseLiteralErr{
						Literal: literal,
						Tokens:  chars,
						err:     err,
					}
				}
				return rest, nil
			})
			// just the literal was consumed,
			i++
		}
	}
	return parsers, nil
}

type datetime struct {
	// this is completely ignored, but we still parse it for correctness
	weekday *time.Weekday

	// day of the month
	day *uint

	month *time.Month
	year  *uint

	// true = AM, false = PM, nil = unspecified
	am *bool

	dayOfYear *uint

	week         *uint
	hours        *uint
	minutes      *uint
	seconds      *uint
	miliseconds  *uint
	microseconds *uint
	nanoseconds  *uint
}

// ParseSpecifierErr defines a error when attempting to parse
// the date string input according to a specified format directive.
type ParseSpecifierErr struct {
	Specifier byte
	Tokens    string
	err       error
}

func (p ParseSpecifierErr) Unwrap() error { return p.err }

func (p ParseSpecifierErr) Error() string {
	return fmt.Sprintf("specifier %%%c failed to parse \"%s\": %s", p.Specifier, p.Tokens, p.err.Error())
}

// ParseLiteralErr defines a error when attempting to parse
// the date string input according to a literal character specified
// in the format string.
type ParseLiteralErr struct {
	Literal byte
	Tokens  string
	err     error
}

func (p ParseLiteralErr) Unwrap() error { return p.err }

func (p ParseLiteralErr) Error() string {
	return fmt.Sprintf("literal %c not matched in \"%s\": %s", p.Literal, p.Tokens, p.err.Error())
}

// formatSpecifiers defines the formatting directives for parsing and formatting dates.
//
// Reference: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format
var formatSpecifiers = map[byte]parser{
	// %a	Abbreviated weekday name (Sun..Sat)
	'a': parseWeedayAbbreviation,
	// %b	Abbreviated month name (Jan..Dec)
	'b': parseMonthAbbreviation,
	// %c	Month, numeric (0..12)
	'c': parseMonthNumeric,
	// %D	Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
	'D': parseDayNumericWithEnglishSuffix,
	// %d	Day of the month, numeric (00..31)
	'd': parseDayOfMonthNumeric,
	// %e	Day of the month, numeric (0..31)
	'e': parseDayOfMonthNumeric,
	// %f	Microseconds (000000..999999)
	'f': parseMicrosecondsNumeric,
	// %H	Hour (00..23)
	'H': parse24HourNumeric,
	// %h	Hour (01..12)
	'h': parse12HourNumeric,
	// %I	Hour (01..12)
	'I': parse12HourNumeric,
	// %i	Minutes, numeric (00..59)
	'i': parseMinuteNumeric,
	// %j	Day of year (001..366)
	'j': parseDayOfYearNumeric,
	// %k	Hour (0..23)
	'k': parse24HourNumeric,
	// %l	Hour (1..12)
	'l': parse12HourNumeric,
	// %M	Month name (January..December)
	'M': parseMonthName,
	// %m	Month, numeric (00..12)
	'm': parseMonthNumeric,
	// %p	AM or PM
	'p': parseAmPm,
	// %r	Time, 12-hour (hh:mm:ss followed by AM or PM)
	'r': parse12HourTimestamp,
	// %S	Seconds (00..59)
	'S': parseSecondsNumeric,
	// %s	Seconds (00..59)
	's': parseSecondsNumeric,
	// %T	Time, 24-hour (hh:mm:ss)
	'T': parse24HourTimestamp,
	'U': nil,
	'u': nil,
	'V': nil,
	'v': nil,
	'W': nil,
	'w': nil,
	'X': nil,
	'x': nil,
	// %Y	Year, numeric, four digits
	'Y': parseYear4DigitNumeric,
	// %y	Year, numeric (two digits)
	'y': parseYear2DigitNumeric,
	'%': literalParser('%'),
}

func boolPtr(a bool) *bool { return &a }

func weekdayAbbrev(abbrev string) (time.Weekday, bool) {
	switch abbrev {
	case "sun":
		return time.Sunday, true
	case "mon":
		return time.Monday, true
	case "tue":
		return time.Tuesday, true
	case "wed":
		return time.Wednesday, true
	case "thu":
		return time.Thursday, true
	case "fri":
		return time.Friday, true
	case "sat":
		return time.Saturday, true
	}
	return 0, false
}

func monthAbbrev(abbrev string) (time.Month, bool) {
	switch abbrev {
	case "jan":
		return time.January, true
	case "feb":
		return time.February, true
	case "mar":
		return time.March, true
	case "apr":
		return time.April, true
	case "may":
		return time.May, true
	case "jun":
		return time.June, true
	case "jul":
		return time.July, true
	case "aug":
		return time.August, true
	case "sep":
		return time.September, true
	case "oct":
		return time.October, true
	case "nov":
		return time.November, true
	case "dec":
		return time.December, true
	}
	return 0, false
}

// TODO: allow this to match partial months
// janu should match janurary
func monthName(name string) (month time.Month, charCount int, ok bool) {
	for i := 1; i < 13; i++ {
		m := time.Month(i)
		if strings.HasPrefix(name, strings.ToLower(m.String())) {
			return m, len(m.String()), true
		}
	}
	return 0, 0, false
}

// Specifier	Description
// %a			Abbreviated weekday name (Sun..Sat)
// %b			Abbreviated month name (Jan..Dec)
// %c			Month, numeric (0..12)
// %D			Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
// %d			Day of the month, numeric (00..31)
// %e			Day of the month, numeric (0..31)
// %f			Microseconds (000000..999999)
// %H			Hour (00..23)
// %h			Hour (01..12)
// %I			Hour (01..12)
// %i			Minutes, numeric (00..59)
// %j			Day of year (001..366)
// %k			Hour (0..23)
// %l			Hour (1..12)
// %M			Month name (January..December)
// %m			Month, numeric (00..12)
// %p			AM or PM
// %r			Time, 12-hour (hh:mm:ss followed by AM or PM)
// %S			Seconds (00..59)
// %s			Seconds (00..59)
// %T			Time, 24-hour (hh:mm:ss)
// %U			Week (00..53), where Sunday is the first day of the week; WEEK() mode 0
// %u			Week (00..53), where Monday is the first day of the week; WEEK() mode 1
// %V			Week (01..53), where Sunday is the first day of the week; WEEK() mode 2; used with %X
// %v			Week (01..53), where Monday is the first day of the week; WEEK() mode 3; used with %x
// %W			Weekday name (Sunday..Saturday)
// %w			Day of the week (0=Sunday..6=Saturday)
// %X			Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
// %x			Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
// %Y			Year, numeric, four digits
// %y			Year, numeric (two digits)
// %%			A literal % character
