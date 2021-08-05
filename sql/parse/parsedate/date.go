package parsedate

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
func ParseDateWithFormat(date, format string) (interface{}, error) {
	parsers, err := parseFormatter(format)
	if err != nil {
		return nil, err
	}

	// trim all leading and trailing whitespace
	date = strings.TrimSpace(date)

	// conver to all lowercase
	date = strings.ToLower(date)

	var result datetime
	target := date
	for _, parser := range parsers {
		_, target = takeAll(target, isChar(' '))
		rest, err := parser(&result, target)
		if err != nil {
			return nil, err
		}
		target = rest
	}

	return evaluate(result)
}

func parseFormatter(format string) ([]Parser, error) {
	i := 0
	parsers := make([]Parser, 0, 0)
	for {
		if len(format) <= i {
			break
		}
		if format[i] == '%' {
			if len(format) <= i+1 {
				return nil, fmt.Errorf("\"%%\" found at end of format string")
			}
			parser, ok := spec[format[i+1]]
			if !ok {
				return nil, fmt.Errorf("unknown format specifier \"%c\"", format[i+1])
			}
			parsers = append(parsers, parser)
			i += 2
		} else {
			parsers = append(parsers, literalParser(format[i]))
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

	week        *uint
	hours       *uint
	minutes     *uint
	seconds     *uint
	miliseconds *uint
}

type parseErr struct {
	specifier byte
	tokens    string
}

func (p parseErr) Error() string {
	return fmt.Sprintf("format specifier \"%%%c\" failed to match \"%s\"", p.specifier, p.tokens)
}

type Parser func(result *datetime, chars string) (rest string, err error)

func literalParser(literal byte) Parser {
	return func(dt *datetime, chars string) (rest string, err error) {
		if len(chars) < 1 && literal != ' ' {
			return "", fmt.Errorf("expected literal \"%c\", found empty string", literal)
		}
		_, chars = takeAll(chars, isChar(' '))
		if literal == ' ' {
			return chars, nil
		}
		if chars[0] != literal {
			return "", fmt.Errorf("expected literal \"%c\", got \"%c\"", literal, chars[0])
		}
		return trimPrefix(1, chars), nil
	}
}

// spec defines the formatting directives for parsing and formatting dates.
//
// Reference: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format
var spec = map[byte]Parser{
	// %a	Abbreviated weekday name (Sun..Sat)
	'a': func(result *datetime, chars string) (rest string, err error) {
		if len(chars) < 3 {
			return "", parseErr{'a', chars}
		}
		weekday, ok := weekdayAbbrev(chars[:3])
		if !ok {
			return "", parseErr{'a', chars}
		}
		result.weekday = &weekday
		return trimPrefix(3, chars), nil
	},
	// %b	Abbreviated month name (Jan..Dec)
	'b': func(result *datetime, chars string) (rest string, err error) {
		if len(chars) < 3 {
			return "", parseErr{'b', chars}
		}
		month, ok := monthAbbrev(chars[:3])
		if !ok {
			return "", parseErr{'b', chars}
		}
		result.month = &month
		return trimPrefix(3, chars), nil
	},
	// %c	Month, numeric (0..12)
	'c': func(result *datetime, chars string) (rest string, err error) {
		num, rest, err := takeNumber(chars)
		if err != nil {
			return "", parseErr{'c', chars}
		}
		month := time.Month(num)
		result.month = &month
		return rest, nil
	},
	// %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
	'D': func(result *datetime, chars string) (rest string, err error) {
		num, rest, err := takeNumber(chars)
		if err != nil {
			return "", parseErr{'D', chars}
		}
		result.day = uintPtr(uint(num))
		return trimPrefix(2, rest), nil
	},
	// %d	Day of the month, numeric (00..31)
	'd': func(result *datetime, chars string) (rest string, err error) {
		num, rest, err := takeNumber(chars)
		if err != nil {
			return "", parseErr{'d', chars}
		}
		result.day = uintPtr(uint(num))
		return rest, nil
	},
	// %e	Day of the month, numeric (0..31)
	'e': func(result *datetime, chars string) (rest string, err error) {
		num, rest, err := takeNumber(chars)
		if err != nil {
			return "", parseErr{'e', chars}
		}
		result.day = uintPtr(uint(num))
		return rest, nil
	},
	'f': nil,
	// %H			Hour (00..23)
	'H': func(result *datetime, chars string) (rest string, err error) {
		hour, rest, err := takeNumber(chars)
		if err != nil {
			return "", parseErr{'H', chars}
		}
		result.hours = uintPtr(uint(hour))
		return rest, nil
	},
	'h': nil,
	'I': nil,
	// %i			Minutes, numeric (00..59)
	'i': func(result *datetime, chars string) (rest string, err error) {
		min, rest, err := takeNumber(chars)
		if err != nil {
			return "", parseErr{'i', chars}
		}
		result.minutes = uintPtr(uint(min))
		return rest, nil
	},
	'j': nil,
	'k': nil,
	'l': nil,
	'M': nil,
	'm': nil,
	// %p AM or PM
	'p': func(result *datetime, chars string) (rest string, err error) {
		if len(chars) < 2 {
			return "", parseErr{'p', chars}
		}
		switch chars[:2] {
		case "am":
			result.am = boolPtr(true)
		case "pm":
			result.am = boolPtr(false)
		default:
			return "", parseErr{'p', chars}
		}
		return trimPrefix(2, chars), nil

	},
	'r': nil,
	'S': nil,
	's': func(result *datetime, chars string) (rest string, err error) {
		sec, rest, err := takeNumber(chars)
		if err != nil {
			return "", parseErr{'i', chars}
		}
		result.seconds = uintPtr(uint(sec))
		return rest, nil
	},
	// %T	Time, 24-hour (hh:mm:ss)
	'T': func(result *datetime, chars string) (rest string, err error) {
		hour, rest, err := takeNumber(chars)
		if err != nil {
			return "", parseErr{'T', chars}
		}
		rest, err = literalParser(':')(result, rest)
		if err != nil {
			return "", err
		}
		minute, rest, err := takeNumber(rest)
		if err != nil {
			return "", parseErr{'T', chars}
		}
		rest, err = literalParser(':')(result, rest)
		if err != nil {
			return "", err
		}
		seconds, rest, err := takeNumber(rest)
		if err != nil {
			return "", parseErr{'T', chars}
		}
		result.hours = uintPtr(uint(hour))
		result.minutes = uintPtr(uint(minute))
		result.seconds = uintPtr(uint(seconds))
		return rest, err
	},
	'U': nil,
	'u': nil,
	'V': nil,
	'v': nil,
	'W': nil,
	'w': nil,
	'X': nil,
	'x': nil,
	// %Y	Year, numeric, four digits
	'Y': func(result *datetime, chars string) (rest string, err error) {
		if len(chars) < 4 {
			return "", parseErr{'Y', chars}
		}
		year, rest, err := takeNumber(chars)
		if err != nil {
			return "", parseErr{'Y', chars}
		}
		result.year = uintPtr(uint(year))
		return rest, nil
	},
	'y': nil,
	'%': literalParser('%'),
}

func trimPrefix(count int, str string) string {
	if len(str) > count {
		return str[count:]
	}
	return ""
}

func uintPtr(a uint) *uint { return &a }
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
