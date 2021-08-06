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
		rest, err := parser.Parser(&result, target)
		if err != nil {
			switch parser.typ {
			case parserLiteral:
				return nil, ParseLiteralErr{parser.cause, target, err}
			case parserSpecifier:
				return nil, ParseSpecifierErr{parser.cause, target, err}
			default:
				panic("unreachable. invalid parser type")
			}
		}
		target = rest
	}

	return evaluate(result)
}

type parserKind int

const (
	parserSpecifier parserKind = iota
	parserLiteral
)

type namedParser struct {
	typ   parserKind
	cause byte
	Parser
}

func parseFormatter(format string) ([]namedParser, error) {
	i := 0
	parsers := make([]namedParser, 0, 0)
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
			if parser == nil {
				return nil, fmt.Errorf("format specifier \"%c\" not supported", format[i+1])
			}
			parsers = append(parsers, namedParser{
				Parser: parser,
				typ:    parserSpecifier,
				cause:  format[i+1],
			})
			i += 2
		} else {
			parsers = append(parsers, namedParser{
				Parser: literalParser(format[i]),
				typ:    parserLiteral,
				cause:  format[i],
			})

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

type ParseSpecifierErr struct {
	Specifier byte
	Tokens    string
	Err       error
}

func (p ParseSpecifierErr) Unwrap() error { return p.Err }

func (p ParseSpecifierErr) Error() string {
	return fmt.Sprintf("specifier %%%c failed to parse \"%s\"", p.Specifier, p.Tokens)
}

type ParseLiteralErr struct {
	Literal byte
	Tokens    string
	Err       error
}

func (p ParseLiteralErr) Unwrap() error { return p.Err }

func (p ParseLiteralErr) Error() string {
	return fmt.Sprintf("literal %c not matched in \"%s\"", p.Literal, p.Tokens)
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
			return "", err
		}
		weekday, ok := weekdayAbbrev(chars[:3])
		if !ok {
			return "", err
		}
		result.weekday = &weekday
		return trimPrefix(3, chars), nil
	},
	// %b	Abbreviated month name (Jan..Dec)
	'b': func(result *datetime, chars string) (rest string, err error) {
		if len(chars) < 3 {
			return "", err
		}
		month, ok := monthAbbrev(chars[:3])
		if !ok {
			return "", err
		}
		result.month = &month
		return trimPrefix(3, chars), nil
	},
	// %c	Month, numeric (0..12)
	'c': func(result *datetime, chars string) (rest string, err error) {
		num, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		month := time.Month(num)
		result.month = &month
		return rest, nil
	},
	// %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
	'D': func(result *datetime, chars string) (rest string, err error) {
		num, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		result.day = uintPtr(uint(num))
		return trimPrefix(2, rest), nil
	},
	// %d	Day of the month, numeric (00..31)
	'd': func(result *datetime, chars string) (rest string, err error) {
		num, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		result.day = uintPtr(uint(num))
		return rest, nil
	},
	// %e	Day of the month, numeric (0..31)
	'e': func(result *datetime, chars string) (rest string, err error) {
		num, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		result.day = uintPtr(uint(num))
		return rest, nil
	},
	// %f Microseconds (000000..999999)
	'f': func(result *datetime, chars string) (rest string, err error) {
		num, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		result.microseconds = uintPtr(uint(num))
		return rest, nil
	},
	// %H			Hour (00..23)
	'H': func(result *datetime, chars string) (rest string, err error) {
		hour, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		result.hours = uintPtr(uint(hour))
		return rest, nil
	},
	// %h	Hour (01..12)
	'h': func(result *datetime, chars string) (rest string, err error) {
		num, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		result.hours = uintPtr(uint(num))
		return rest, nil
	},
	// %I Hour (01..12)
	'I': func(result *datetime, chars string) (rest string, err error) {
		hour, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		result.hours = uintPtr(uint(hour))
		return rest, nil
	},
	// %i			Minutes, numeric (00..59)
	'i': func(result *datetime, chars string) (rest string, err error) {
		min, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		result.minutes = uintPtr(uint(min))
		return rest, nil
	},
	// %j	Day of year (001..366)
	'j': func(result *datetime, chars string) (rest string, err error) {
		num, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		result.dayOfYear = uintPtr(uint(num))
		return "", nil
	},
	// %k	Hour (0..23)
	'k': func(result *datetime, chars string) (rest string, err error) {
		num, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		result.hours = uintPtr(uint(num))
		return rest, nil
	},
	// %l	Hour (1..12)
	'l': func(result *datetime, chars string) (rest string, err error) {
		num, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		result.hours = uintPtr(uint(num))
		return rest, nil
	},
	'M': func(result *datetime, chars string) (rest string, err error) {
		month, charCount, ok := monthName(chars)
		if !ok {
			return "", err
		}
		result.month = &month
		return trimPrefix(charCount, chars), nil
	},
	// %m Month, numeric (00..12)
	'm': func(result *datetime, chars string) (rest string, err error) {
		num, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		month := time.Month(num)
		result.month = &month
		return rest, nil
	},
	// %p AM or PM
	'p': parseAmPm,
	// %r	Time, 12-hour (hh:mm:ss followed by AM or PM)
	'r': func(result *datetime, chars string) (rest string, err error) {
		hour, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		rest, err = literalParser(':')(result, rest)
		if err != nil {
			return "", err
		}
		min, rest, err := takeNumber(rest)
		if err != nil {
			return "", err
		}
		rest, err = literalParser(':')(result, rest)
		if err != nil {
			return "", err
		}
		sec, rest, err := takeNumber(rest)
		if err != nil {
			return "", err
		}
		_, rest = takeAll(rest, isChar(' '))
		rest, err = parseAmPm(result, rest)
		if err != nil {
			return "", err
		}
		result.seconds = uintPtr(uint(sec))
		result.minutes = uintPtr(uint(min))
		result.hours = uintPtr(uint(hour))
		return rest, nil
	},
	// %S	Seconds (00..59)
	'S': func(result *datetime, chars string) (rest string, err error) {
		sec, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		result.seconds = uintPtr(uint(sec))
		return rest, nil
	},
	's': func(result *datetime, chars string) (rest string, err error) {
		sec, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		result.seconds = uintPtr(uint(sec))
		return rest, nil
	},
	// %T	Time, 24-hour (hh:mm:ss)
	'T': func(result *datetime, chars string) (rest string, err error) {
		hour, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		rest, err = literalParser(':')(result, rest)
		if err != nil {
			return "", err
		}
		minute, rest, err := takeNumber(rest)
		if err != nil {
			return "", err
		}
		rest, err = literalParser(':')(result, rest)
		if err != nil {
			return "", err
		}
		seconds, rest, err := takeNumber(rest)
		if err != nil {
			return "", err
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
			return "", err
		}
		year, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		result.year = uintPtr(uint(year))
		return rest, nil
	},
	// %y	Year, numeric (two digits)
	'y': func(result *datetime, chars string) (rest string, err error) {
		if len(chars) < 2 {
			return "", err
		}
		year, rest, err := takeNumber(chars)
		if err != nil {
			return "", err
		}
		if year >= 100 {
			return "", err
		}
		if year >= 70 {
			year += 1900
		} else {
			year += 2000
		}
		result.year = uintPtr(uint(year))
		return rest, nil
	},
	'%': literalParser('%'),
}

func parseAmPm(result *datetime, chars string) (rest string, err error) {
	if len(chars) < 2 {
		return "", fmt.Errorf("expected > 2 chars, found %d", len(chars))
	}
	switch chars[:2] {
	case "am":
		result.am = boolPtr(true)
	case "pm":
		result.am = boolPtr(false)
	default:
		return "", err
	}
	return trimPrefix(2, chars), nil
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
