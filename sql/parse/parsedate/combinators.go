package parsedate

import (
	"strconv"
	"strings"
)

type predicate func(char rune) bool

func isChar(a rune) predicate { return func(b rune) bool { return a == b } }

func takeAll(str string, match predicate) (captured, rest string) {
	var result strings.Builder
	for i, ch := range str {
		if !match(ch) {
			return result.String(), trimPrefix(i, str)
		}
		result.WriteRune(ch)
	}
	return result.String(), ""
}

func takeAllSpaces(str string) (rest string) {
	_, rest = takeAll(str, isChar(' '))
	return rest
}

func takeNumber(chars string) (num int, rest string, err error) {
	numChars, rest := takeAll(chars, isNumeral)
	parsedNum, err := strconv.ParseInt(numChars, 10, 32)
	if err != nil {
		return 0, "", err
	}
	return int(parsedNum), rest, nil
}

func isNumeral(r rune) bool {
	switch r {
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return true
	}
	return false
}
