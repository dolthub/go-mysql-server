package dateparse

import (
	"fmt"
	"time"
)

type ParseType int

const (
	Time ParseType = iota
	Date
	None
)

// parser defines a function that processes a string and returns the
// remaining characters unconsumed by the given parser.
//
// The data parsed from the consumed characters should be
// written to the `datetime` struct.
type parser func(result *datetime, chars string) (rest string, parseType ParseType, err error)

func trimPrefix(count int, str string) string {
	if len(str) > count {
		return str[count:]
	}
	return ""
}

func literalParser(literal byte) parser {
	return func(dt *datetime, chars string) (rest string, parseType ParseType, _ error) {
		if len(chars) < 1 && literal != ' ' {
			return "", None, fmt.Errorf("expected literal \"%c\", found empty string", literal)
		}
		chars = takeAllSpaces(chars)
		if literal == ' ' {
			return chars, None, nil
		}
		if chars[0] != literal {
			return "", None, fmt.Errorf("expected literal \"%c\", got \"%c\"", literal, chars[0])
		}
		return trimPrefix(1, chars), None, nil
	}
}

func parseAmPm(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	if len(chars) < 2 {
		return "", None, fmt.Errorf("expected > 2 chars, found %d", len(chars))
	}
	switch chars[:2] {
	case "am":
		result.am = boolPtr(true)
	case "pm":
		result.am = boolPtr(false)
	default:
		return "", None, fmt.Errorf("expected AM or PM, got \"%s\"", chars[:2])
	}
	return trimPrefix(2, chars), Time, nil
}

func parseWeekdayAbbreviation(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	if len(chars) < 3 {
		return "", None, fmt.Errorf("expected at least 3 chars, got %d", len(chars))
	}
	weekday, ok := weekdayAbbrev(chars[:3])
	if !ok {
		return "", None, fmt.Errorf("invalid week abbreviation \"%s\"", chars[:3])
	}
	result.weekday = &weekday
	return trimPrefix(3, chars), Date, nil
}

func parseMonthAbbreviation(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	if len(chars) < 3 {
		return "", None, fmt.Errorf("expected at least 3 chars, got %d", len(chars))
	}
	month, ok := monthAbbrev(chars[:3])
	if !ok {
		return "", None, fmt.Errorf("invalid month abbreviation \"%s\"", chars[:3])
	}
	result.month = &month
	return trimPrefix(3, chars), Date, nil
}

func parseMonthNumeric(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	num, rest, err := takeNumber(chars)
	if err != nil {
		return "", None, err
	}
	month := time.Month(num)
	result.month = &month
	return rest, Date, nil
}

func parseDayOfMonthNumeric(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	num, rest, err := takeNumber(chars)
	if err != nil {
		return "", None, err
	}
	result.day = &num
	return rest, Date, nil
}

func parseMicrosecondsNumeric(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	num, rest, err := takeNumber(chars)
	if err != nil {
		return "", None, err
	}
	result.microseconds = &num
	return rest, Time, nil
}

func parse24HourNumeric(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	hour, rest, err := takeNumber(chars)
	if err != nil {
		return "", None, err
	}
	result.hours = &hour
	return rest, Time, nil
}

func parse12HourNumeric(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	num, rest, err := takeNumber(chars)
	if err != nil {
		return "", None, err
	}
	result.hours = &num
	return rest, Time, nil
}

func parseMinuteNumeric(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	min, rest, err := takeNumber(chars)
	if err != nil {
		return "", None, err
	}
	result.minutes = &min
	return rest, Time, nil
}

func parseMonthName(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	month, charCount, ok := monthName(chars)
	if !ok {
		return "", None, fmt.Errorf("unknown month name, got \"%s\"", chars)
	}
	result.month = &month
	return trimPrefix(charCount, chars), Date, nil
}

func parse12HourTimestamp(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	hour, rest, err := takeNumberAtMostNChars(2, chars)
	if err != nil {
		return "", None, err
	}
	rest, parseType, err = literalParser(':')(result, rest)
	if err != nil {
		return "", parseType, err
	}
	min, rest, err := takeNumberAtMostNChars(2, rest)
	if err != nil {
		return "", None, err
	}
	rest, parseType, err = literalParser(':')(result, rest)
	if err != nil {
		return "", parseType, err
	}
	sec, rest, err := takeNumberAtMostNChars(2, rest)
	if err != nil {
		return "", None, err
	}
	rest = takeAllSpaces(rest)
	rest, parseType, err = parseAmPm(result, rest)
	if err != nil {
		return "", parseType, err
	}
	result.seconds = &sec
	result.minutes = &min
	result.hours = &hour
	return rest, Time, nil
}

func parseSecondsNumeric(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	sec, rest, err := takeNumber(chars)
	if err != nil {
		return "", None, err
	}
	result.seconds = &sec
	return rest, Time, nil
}

func parse24HourTimestamp(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	hour, rest, err := takeNumberAtMostNChars(2, chars)
	if err != nil {
		return "", None, err
	}
	rest, parseType, err = literalParser(':')(result, rest)
	if err != nil {
		return "", parseType, err
	}
	minute, rest, err := takeNumberAtMostNChars(2, rest)
	if err != nil {
		return "", None, err
	}
	rest, parseType, err = literalParser(':')(result, rest)
	if err != nil {
		return "", parseType, err
	}
	seconds, rest, err := takeNumberAtMostNChars(2, rest)
	if err != nil {
		return "", None, err
	}
	result.hours = &hour
	result.minutes = &minute
	result.seconds = &seconds
	return rest, Time, nil
}

func parseYear2DigitNumeric(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	year, rest, err := takeNumberAtMostNChars(2, chars)
	if err != nil {
		return "", None, err
	}
	if year >= 70 {
		year += 1900
	} else {
		year += 2000
	}
	result.year = &year
	return rest, Date, nil
}

func parseYear4DigitNumeric(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	if len(chars) < 4 {
		return "", None, fmt.Errorf("expected at least 4 chars, got %d", len(chars))
	}
	year, rest, err := takeNumber(chars)
	if err != nil {
		return "", None, err
	}
	result.year = &year
	return rest, Date, nil
}

func parseDayNumericWithEnglishSuffix(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	num, rest, err := takeNumber(chars)
	if err != nil {
		return "", None, err
	}
	result.day = &num
	return trimPrefix(2, rest), Date, nil
}

func parseDayOfYearNumeric(result *datetime, chars string) (rest string, parseType ParseType, _ error) {
	num, rest, err := takeNumber(chars)
	if err != nil {
		return "", None, err
	}
	result.dayOfYear = &num
	return rest, Date, nil
}
