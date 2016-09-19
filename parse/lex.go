package parse

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"unicode"
)

type stateFunc func(*Lexer) (stateFunc, error)

type Lexer struct {
	source *bufio.Reader
	state  stateFunc
	tokens []*Token
	idx    uint
	line   uint
	pos    uint
	word   []rune
}

func NewLexer(input io.Reader) *Lexer {
	return &Lexer{
		source: bufio.NewReader(input),
		state:  lexLine,
	}
}

func (l *Lexer) next() (r rune, err error) {
	r, _, err = l.source.ReadRune()
	if err != nil {
		return
	}

	l.word = append(l.word, r)
	l.pos++
	return
}

func (l *Lexer) ignore() {
	l.word = nil
}

func (l *Lexer) backup() error {
	err := l.source.UnreadRune()
	if err != nil {
		return err
	}

	if len(l.word) < 2 {
		l.word = nil
	} else {
		l.word = l.word[0 : len(l.word)-1]
	}

	l.pos--
	return nil
}

func (l *Lexer) peekWord() string {
	return string(l.word)
}

func (l *Lexer) newLine() {
	l.line++
	l.pos = 1
}

func (l *Lexer) emit(typ TokenType) {
	l.tokens = append(l.tokens, NewToken(
		typ,
		l.peekWord(),
		l.line,
		l.pos,
	))
	l.word = nil
}

func (l *Lexer) errorf(format string, args ...interface{}) stateFunc {
	l.tokens = append(l.tokens, NewToken(
		ErrorToken,
		fmt.Sprintf(format, args...),
		l.line,
		l.pos,
	))
	return nil
}

func (l *Lexer) Run() error {
	for l.state != nil {
		var err error
		l.state, err = l.state(l)
		if err == io.EOF {
			l.emit(EOFToken)
			return nil
		} else if err != nil {
			return err
		}
	}

	return nil
}

func (l *Lexer) Next() *Token {
	if l.idx >= uint(len(l.tokens)) {
		return nil
	}
	tk := l.tokens[l.idx]
	l.idx++
	return tk
}

const (
	eof         rune = -1
	comma            = ','
	dot              = '.'
	leftParen        = '('
	rightParen       = ')'
	quote            = '"'
	singleQuote      = '\''
	semiColon        = ';'
	backslash        = '\\'
)

func lexLine(l *Lexer) (stateFunc, error) {
	r, err := l.next()
	if err == io.EOF {
		l.emit(EOFToken)
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	switch true {
	case isSpace(r):
		return lexSpaces, nil
	case isEOL(r):
		return lexEOL, nil
	case isLetter(r):
		return lexIdentifier, nil
	case isAllowedInOp(r):
		return lexOp, nil
	case r == comma:
		l.emit(CommaToken)
		return lexLine, nil
	case r == dot:
		l.emit(DotToken)
		return lexLine, nil
	case r == leftParen:
		l.emit(LeftParenToken)
		return lexLine, nil
	case r == rightParen:
		l.emit(RightParenToken)
		return lexLine, nil
	case r == singleQuote:
		return lexSingleQuote, nil
	case r == quote:
		return lexQuote, nil
	case unicode.IsDigit(r):
		return lexNumber, nil
	case r == semiColon:
		l.emit(EOFToken)
		return nil, nil
	}

	return l.errorf("unexpected character: %q", r), nil
}

func scanDigits(l *Lexer) error {
	for {
		r, err := l.next()
		if err != nil {
			return err
		}

		if !unicode.IsDigit(r) {
			return l.backup()
		}
	}
}

func lexNumber(l *Lexer) (stateFunc, error) {
	if err := scanDigits(l); err != nil {
		return nil, err
	}

	r, err := l.next()
	if err != nil {
		return nil, err
	}

	switch true {
	case r == dot:
		if err := scanDigits(l); err != nil {
			return nil, err
		}

		r, err := l.next()
		if err != nil {
			return nil, err
		}

		if isValidNumberTermination(r) {
			if err := l.backup(); err != nil {
				return nil, err
			}

			l.emit(FloatToken)
			return lexLine, nil
		}
	case isValidNumberTermination(r):
		if err := l.backup(); err != nil {
			return nil, err
		}

		l.emit(IntToken)
		return lexLine, nil
	}

	return l.errorf("invalid number syntax: %q", l.peekWord()), nil
}

var keywords = []string{
	"select", "from", "where", "in", "order", "by", "asc",
	"desc", "and", "or", "distinct", "limit", "offset", "as",
}

func isKeyword(kw string) bool {
	kw = strings.ToLower(kw)
	for _, k := range keywords {
		if k == kw {
			return true
		}
	}
	return false
}

func lexIdentifier(l *Lexer) (stateFunc, error) {
	for {
		r, err := l.next()
		if err != nil {
			return nil, err
		}

		if !isAllowedInIdentifier(r) {
			if err := l.backup(); err != nil {
				return nil, err
			}

			word := l.peekWord()
			var typ = IdentifierToken
			if isKeyword(word) {
				typ = KeywordToken
			}

			l.emit(typ)
			return lexLine, nil
		}
	}
}

var operators = []string{
	"<", ">", ">=", "<=", "=", "<>",
	"+", "-", "*", "/", "%",
}

func isValidOperator(word string) bool {
	for _, op := range operators {
		if op == word {
			return true
		}
	}
	return false
}

func lexOp(l *Lexer) (stateFunc, error) {
	for {
		r, err := l.next()
		if err != nil {
			return nil, err
		}

		if !isAllowedInOp(r) {
			if err := l.backup(); err != nil {
				return nil, err
			}

			op := l.peekWord()
			if !isValidOperator(op) {
				return l.errorf("invalid operator: %q", op), nil
			}

			l.emit(OpToken)
			return lexLine, nil
		}
	}
}

func lexQuote(l *Lexer) (stateFunc, error) {
	return lexString(l, quote)
}

func lexSingleQuote(l *Lexer) (stateFunc, error) {
	return lexString(l, singleQuote)
}

func lexString(l *Lexer, quoteRune rune) (stateFunc, error) {
	var escaped bool
	for {
		r, err := l.next()
		if err != nil {
			return nil, err
		}

		if r == backslash {
			escaped = true
		} else if r == quoteRune && !escaped {
			l.emit(StringToken)
			return lexLine, nil
		} else if escaped {
			escaped = false
		}
	}
}

func lexSpaces(l *Lexer) (stateFunc, error) {
	for {
		r, err := l.next()
		if err != nil {
			return nil, err
		}

		if !isSpace(r) {
			if err := l.backup(); err != nil {
				return nil, err
			}
			l.ignore()
			return lexLine, nil
		}
	}
}

func lexEOL(l *Lexer) (stateFunc, error) {
	// first eol was already scanned
	l.newLine()

	for {
		r, err := l.next()
		if err != nil {
			return nil, err
		}

		if !isEOL(r) {
			if err := l.backup(); err != nil {
				return nil, err
			}
			l.ignore()
			return lexLine, nil
		}

		l.newLine()
	}
}

func isSpace(r rune) bool {
	return r == ' ' || r == '\t'
}

func isLetter(r rune) bool {
	return unicode.IsLetter(r)
}

func isEOL(r rune) bool {
	return r == '\r' || r == '\n'
}

func isAllowedInOp(r rune) bool {
	return strings.IndexRune("<>=+-*/-%", r) >= 0
}

func isAllowedInIdentifier(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

func isValidNumberTermination(r rune) bool {
	return r == comma || r == semiColon || r == leftParen || r == rightParen || isSpace(r) || isEOL(r)
}
