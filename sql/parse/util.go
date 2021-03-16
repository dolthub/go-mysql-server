// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	var unusedCount int
	return readSpaces(r, &unusedCount)
}

// readSpaces reads every contiguous space from the reader, populating
// numSpacesRead with the number of spaces read.
func readSpaces(r *bufio.Reader, numSpacesRead *int) error {
	*numSpacesRead = 0
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
		*numSpacesRead++
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

// readLetterOrPoint parses a single rune from the reader and consumes it,
// copying it to the buffer, if it is either a letter or a point
func readLetterOrPoint(r *bufio.Reader, buf *bytes.Buffer) error {
	ru, _, err := r.ReadRune()
	if err != nil {
		if err == io.EOF {
			return nil
		}

		return err
	}

	if !unicode.IsLetter(ru) && ru != '.' {
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

// readValidScopedIdentRune parses a single rune from the reader and consumes
// it, copying it to the buffer, if is a letter, a digit, an underscore or the
// specified separator.
// If the returned error is not nil, the  returned rune equals the null
// character.
func readValidScopedIdentRune(r *bufio.Reader, separator rune) (rune, error) {
	ru, _, err := r.ReadRune()
	if err != nil {
		return 0, err
	}

	if !unicode.IsLetter(ru) && !unicode.IsDigit(ru) && ru != '_' && ru != separator {
		if err := r.UnreadRune(); err != nil {
			return 0, err
		}
		return 0, io.EOF
	}

	return ru, nil
}

func readValidQuotedIdentRune(r *bufio.Reader, buf *bytes.Buffer) error {
	bs, err := r.Peek(2)
	if err != nil {
		return err
	}

	if bs[0] == '`' && bs[1] == '`' {
		if _, _, err := r.ReadRune(); err != nil {
			return err
		}
		if _, _, err := r.ReadRune(); err != nil {
			return err
		}
		buf.WriteRune('`')
		return nil
	}

	if bs[0] == '`' && bs[1] != '`' {
		return io.EOF
	}

	if _, _, err := r.ReadRune(); err != nil {
		return err
	}

	buf.WriteByte(bs[0])

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

// readIdentList reads a scoped identifier, populating the specified slice
// with the different parts of the identifier if it is correctly formed.
// A scoped identifier is a sequence of identifiers separated by the specified
// rune in separator. An identifier is a string of runes whose first character
// is a letter and the following ones are either letters, digits or underscores.
// An example of a correctly formed scoped identifier is "dbName.tableName",
// that would populate the slice with the values ["dbName", "tableName"]
func readIdentList(separator rune, idents *[]string) parseFunc {
	return func(r *bufio.Reader) error {
		var buf bytes.Buffer
		if err := readLetter(r, &buf); err != nil {
			return err
		}

		for {
			currentRune, err := readValidScopedIdentRune(r, separator)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			if currentRune == separator {
				*idents = append(*idents, buf.String())
				buf.Reset()
			} else {
				buf.WriteRune(currentRune)
			}
		}

		if readString := buf.String(); len(readString) > 0 {
			*idents = append(*idents, readString)
		}
		return nil
	}
}

func readQuotedIdent(ident *string) parseFunc {
	return func(r *bufio.Reader) error {
		var buf bytes.Buffer
		if err := readValidQuotedIdentRune(r, &buf); err != nil {
			return err
		}

		for {
			if err := readValidQuotedIdentRune(r, &buf); err == io.EOF {
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
				readQuotedIdent(ident),
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

// maybe tries to read the specified string, consuming the reader if the string
// is found. The `matched` boolean is set to true if the string is found
func maybe(matched *bool, str string) parseFunc {
	return func(rd *bufio.Reader) error {
		*matched = false
		strLength := len(str)

		data, err := rd.Peek(strLength)
		if err != nil {
			// If there are not enough runes, what we expected was not there, which
			// is not an error per se.
			if len(data) < strLength {
				return nil
			}

			return err
		}

		if strings.ToLower(string(data)) == str {
			_, err := rd.Discard(strLength)
			if err != nil {
				return err
			}

			*matched = true
			return nil
		}

		return nil
	}
}

// multiMaybe tries to read the specified strings, one after the other,
// separated by an arbitrary number of spaces. It consumes the reader if and
// only if all the strings are found.
func multiMaybe(matched *bool, strings ...string) parseFunc {
	return func(rd *bufio.Reader) error {
		*matched = false
		var read string
		for _, str := range strings {
			if err := maybe(matched, str)(rd); err != nil {
				return err
			}

			if !*matched {
				unreadString(rd, read)
				return nil
			}

			var numSpaces int
			if err := readSpaces(rd, &numSpaces); err != nil {
				return err
			}

			read = read + str
			for i := 0; i < numSpaces; i++ {
				read = read + " "
			}
		}
		*matched = true
		return nil
	}
}

// maybeList reads a list of strings separated by the specified separator, with
// a rune indicating the opening of the list and another one specifying its
// closing.
// For example, readList('(', ',', ')', list) parses "(uno,  dos,tres)" and
// populates list with the array of strings ["uno", "dos", "tres"]
// If the opening is not found, this does not consumes any rune from the
// reader. If there is a parsing error after some elements were found, the list
// is partially populated with the correct fields
func maybeList(opening, separator, closing rune, list *[]string) parseFunc {
	return func(rd *bufio.Reader) error {
		r, _, err := rd.ReadRune()
		if err != nil {
			return err
		}

		if r != opening {
			return rd.UnreadRune()
		}

		for {
			var newItem string
			err := parseFuncs{
				skipSpaces,
				readIdent(&newItem),
				skipSpaces,
			}.exec(rd)

			if err != nil {
				return err
			}

			r, _, err := rd.ReadRune()
			if err != nil {
				return err
			}

			switch r {
			case closing:
				*list = append(*list, newItem)
				return nil
			case separator:
				*list = append(*list, newItem)
				continue
			default:
				return errUnexpectedSyntax.New(
					fmt.Sprintf("%v or %v", separator, closing),
					string(r),
				)
			}
		}
	}
}

// A qualifiedName represents an identifier of type "db_name.table_name"
type qualifiedName struct {
	qualifier string
	name      string
}

// readQualifiedIdentifierList reads a comma-separated list of qualifiedNames.
// Any number of spaces between the qualified names are accepted. The qualifier
// may be empty, in which case the period is optional.
// An example of a correctly formed list is:
// "my_db.myview, db_2.mytable ,   aTable"
func readQualifiedIdentifierList(list *[]qualifiedName) parseFunc {
	return func(rd *bufio.Reader) error {
		for {
			var newItem []string
			err := parseFuncs{
				skipSpaces,
				readIdentList('.', &newItem),
				skipSpaces,
			}.exec(rd)

			if err != nil {
				return err
			}

			if len(newItem) < 1 || len(newItem) > 2 {
				return errUnexpectedSyntax.New(
					"[qualifier.]name",
					strings.Join(newItem, "."),
				)
			}

			var qualifier, name string

			if len(newItem) == 1 {
				qualifier = ""
				name = newItem[0]
			} else {
				qualifier = newItem[0]
				name = newItem[1]
			}

			*list = append(*list, qualifiedName{qualifier, name})

			r, _, err := rd.ReadRune()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}

			switch r {
			case ',':
				continue
			default:
				return rd.UnreadRune()
			}
		}
	}
}
