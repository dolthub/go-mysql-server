package parse

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"unicode"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-vitess.v1/vt/sqlparser"
)

var (
	errUnexpectedSyntax       = errors.NewKind("expecting %q but got %q instead")
	errInvalidIndexExpression = errors.NewKind("invalid expression to index: %s")
)

type parseFunc func(*bufio.Reader) error

type parseFuncs []parseFunc

func (f parseFuncs) exec(r *bufio.Reader) error {
	for _, fn := range f {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

func expectRune(expected rune) parseFunc {
	return func(rd *bufio.Reader) error {
		r, _, err := rd.ReadRune()
		if err != nil {
			return err
		}

		if r != expected {
			return errUnexpectedSyntax.New(expected, string(r))
		}

		return nil
	}
}

func expect(expected string) parseFunc {
	return func(r *bufio.Reader) error {
		var ident string

		if err := readIdent(&ident)(r); err != nil {
			return err
		}

		if ident == expected {
			return nil
		}

		return errUnexpectedSyntax.New(expected, ident)
	}
}

func skipSpaces(r *bufio.Reader) error {
	for {
		ru, _, err := r.ReadRune()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		if !unicode.IsSpace(ru) {
			return r.UnreadRune()
		}
	}
}

func checkEOF(rd *bufio.Reader) error {
	r, _, err := rd.ReadRune()
	if err == io.EOF {
		return nil
	}

	return errUnexpectedSyntax.New("EOF", r)
}

func optional(steps ...parseFunc) parseFunc {
	return func(rd *bufio.Reader) error {
		for _, step := range steps {
			err := step(rd)
			if err == io.EOF || errUnexpectedSyntax.Is(err) {
				return nil
			}

			if err != nil {
				return err
			}
		}

		return nil
	}
}

func readLetter(r *bufio.Reader, buf *bytes.Buffer) error {
	ru, _, err := r.ReadRune()
	if err != nil {
		if err == io.EOF {
			return nil
		}

		return err
	}

	if !unicode.IsLetter(ru) {
		if err := r.UnreadRune(); err != nil {
			return err
		}
		return nil
	}

	buf.WriteRune(ru)
	return nil
}

func readValidIdentRune(r *bufio.Reader, buf *bytes.Buffer) error {
	ru, _, err := r.ReadRune()
	if err != nil {
		return err
	}

	if !unicode.IsLetter(ru) && !unicode.IsDigit(ru) && ru != '_' {
		if err := r.UnreadRune(); err != nil {
			return err
		}
		return io.EOF
	}

	buf.WriteRune(ru)
	return nil
}

func unreadString(r *bufio.Reader, str string) {
	nr := *r
	r.Reset(io.MultiReader(strings.NewReader(str), &nr))
}

func readIdent(ident *string) parseFunc {
	return func(r *bufio.Reader) error {
		var buf bytes.Buffer
		if err := readLetter(r, &buf); err != nil {
			return err
		}

		for {
			if err := readValidIdentRune(r, &buf); err == io.EOF {
				break
			} else if err != nil {
				return err
			}
		}

		*ident = strings.ToLower(buf.String())
		return nil
	}
}

func oneOf(options ...string) parseFunc {
	return func(r *bufio.Reader) error {
		var ident string
		if err := readIdent(&ident)(r); err != nil {
			return err
		}

		for _, opt := range options {
			if strings.ToLower(opt) == ident {
				return nil
			}
		}

		return errUnexpectedSyntax.New(
			fmt.Sprintf("one of: %s", strings.Join(options, ", ")),
			ident,
		)
	}
}

func readRemaining(val *string) parseFunc {
	return func(r *bufio.Reader) error {
		bytes, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}

		*val = string(bytes)
		return nil
	}
}

func parseExpr(str string) (sql.Expression, error) {
	stmt, err := sqlparser.Parse("SELECT " + str)
	if err != nil {
		return nil, err
	}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, errInvalidIndexExpression.New(str)
	}

	if len(selectStmt.SelectExprs) != 1 {
		return nil, errInvalidIndexExpression.New(str)
	}

	selectExpr, ok := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, errInvalidIndexExpression.New(str)
	}

	return exprToExpression(selectExpr.Expr)
}

func readQuotableIdent(ident *string) parseFunc {
	return func(r *bufio.Reader) error {
		nextChar, err := r.Peek(1)
		if err != nil {
			return err
		}

		var steps parseFuncs
		if nextChar[0] == '`' {
			steps = parseFuncs{
				expectQuote,
				readIdent(ident),
				expectQuote,
			}
		} else {
			steps = parseFuncs{readIdent(ident)}
		}

		return steps.exec(r)
	}
}

func expectQuote(r *bufio.Reader) error {
	ru, _, err := r.ReadRune()
	if err != nil {
		return err
	}

	if ru != '`' {
		return errUnexpectedSyntax.New("`", string(ru))
	}

	return nil
}
