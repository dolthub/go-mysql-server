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
)

var (
	errUnexpectedSyntax       = errors.NewKind("expecting %q but got %q instead")
	errInvalidIndexExpression = errors.NewKind("invalid expression to index: %s")
)

type parseFunc func(*bufio.Reader) error

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

func readIdent(ident *string) parseFunc {
	return func(r *bufio.Reader) error {
		var buf bytes.Buffer
		for {
			ru, _, err := r.ReadRune()
			if err == io.EOF {
				break
			}

			if err != nil {
				return err
			}

			if !unicode.IsLetter(ru) && ru != '_' {
				if err := r.UnreadRune(); err != nil {
					return err
				}
				break
			}

			buf.WriteRune(ru)
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
