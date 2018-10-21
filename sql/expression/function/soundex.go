package function

import (
	"fmt"
	"strings"
	"unicode"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// Soundex is a function that returns the soundex of a string. Two strings that
// sound almost the same should have identical soundex strings. A standard
// soundex string is four characters long, but the SOUNDEX() function returns
// an arbitrarily long string.
type Soundex struct {
	expression.UnaryExpression
}

// NewSoundex creates a new Soundex expression.
func NewSoundex(e sql.Expression) sql.Expression {
	return &Soundex{expression.UnaryExpression{Child: e}}
}

// Eval implements the Expression interface.
func (s *Soundex) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	v, err := s.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	v, err = sql.Text.Convert(v)
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	var last rune
	for _, c := range strings.ToUpper(v.(string)) {
		if last == 0 && !unicode.IsLetter(c) {
			continue
		}
		code := s.code(c)
		if last == 0 {
			b.WriteRune(c)
			last = code
			continue
		}
		if code == '0' || code == last {
			continue
		}
		b.WriteRune(code)
		last = code
	}
	if b.Len() == 0 {
		return "", nil
	}
	for i := len([]rune(b.String())); i < 4; i++ {
		b.WriteRune('0')
	}
	return b.String(), nil
}

func (s *Soundex) code(c rune) rune {
	switch c {
	case 'B', 'F', 'P', 'V':
		return '1'
	case 'C', 'G', 'J', 'K', 'Q', 'S', 'X', 'Z':
		return '2'
	case 'D', 'T':
		return '3'
	case 'L':
		return '4'
	case 'M', 'N':
		return '5'
	case 'R':
		return '6'
	}
	return '0'
}

func (s *Soundex) String() string {
	return fmt.Sprintf("SOUNDEX(%s)", s.Child)
}

// TransformUp implements the Expression interface.
func (s *Soundex) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	child, err := s.Child.TransformUp(f)
	if err != nil {
		return nil, err
	}
	return f(NewSoundex(child))
}

// Type implements the Expression interface.
func (s *Soundex) Type() sql.Type {
	return s.Child.Type()
}
