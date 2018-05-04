package parse

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"unicode"

	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-vitess.v0/vt/sqlparser"
)

var (
	errUnexpectedSyntax       = errors.NewKind("expecting %q but got %q instead")
	errInvalidIndexExpression = errors.NewKind("invalid expression to index: %s")
)

type parseFunc func(*bufio.Reader) error

func parseCreateIndex(s string) (sql.Node, error) {
	r := bufio.NewReader(strings.NewReader(s))

	var name, table, driver string
	var exprs []string
	var config = make(map[string]string)
	steps := []parseFunc{
		expect("create"),
		skipSpaces,
		expect("index"),
		skipSpaces,
		readIdent(&name),
		skipSpaces,
		expect("on"),
		skipSpaces,
		readIdent(&table),
		skipSpaces,
		optional(
			expect("using"),
			skipSpaces,
			readIdent(&driver),
			skipSpaces,
		),
		readExprs(&exprs),
		skipSpaces,
		optional(
			expect("with"),
			skipSpaces,
			readKeyValue(config),
			skipSpaces,
		),
		checkEOF,
	}

	for _, step := range steps {
		if err := step(r); err != nil {
			return nil, err
		}
	}

	var indexExprs = make([]sql.Expression, len(exprs))
	for i, e := range exprs {
		var err error
		indexExprs[i], err = parseIndexExpr(e)
		if err != nil {
			return nil, err
		}
	}

	return plan.NewCreateIndex(
		name,
		plan.NewUnresolvedTable(table),
		indexExprs,
		driver,
		config,
	), nil
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

func readKeyValue(kv map[string]string) parseFunc {
	return func(rd *bufio.Reader) error {
		r, _, err := rd.ReadRune()
		if err != nil {
			return err
		}

		if r != '(' {
			return errUnexpectedSyntax.New("(", string(r))
		}

		for {
			var key, value string
			steps := []parseFunc{
				skipSpaces,
				readIdent(&key),
				skipSpaces,
				expectRune('='),
				skipSpaces,
				readValue(&value),
				skipSpaces,
			}

			for _, step := range steps {
				if err := step(rd); err != nil {
					return err
				}
			}

			r, _, err := rd.ReadRune()
			if err != nil {
				return err
			}

			switch r {
			case ')':
				kv[key] = value
				return nil
			case ',':
				kv[key] = value
				continue
			default:
				return errUnexpectedSyntax.New(", or )", string(r))
			}
		}
	}
}

func readValue(val *string) parseFunc {
	return func(rd *bufio.Reader) error {
		var buf bytes.Buffer
		var singleQuote, doubleQuote, ignoreNext bool
		var first = true
		for {
			r, _, err := rd.ReadRune()
			if err == io.EOF {
				break
			}

			if err != nil {
				return err
			}

			if singleQuote || doubleQuote {
				switch true {
				case ignoreNext:
					ignoreNext = false
				case r == '\\':
					ignoreNext = true
					continue
				case r == '\'' && singleQuote:
					singleQuote = false
					continue
				case r == '"' && doubleQuote:
					doubleQuote = false
					continue
				}
			} else if first && (r == '\'' || r == '"') {
				if r == '\'' {
					singleQuote = true
				} else {
					doubleQuote = true
				}
				first = false
				continue
			} else if !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '_' {
				if err := rd.UnreadRune(); err != nil {
					return err
				}
				break
			}

			buf.WriteRune(r)
		}

		*val = strings.ToLower(buf.String())
		return nil
	}
}

func parseDropIndex(str string) (sql.Node, error) {
	r := bufio.NewReader(strings.NewReader(str))

	var name, table string
	steps := []parseFunc{
		expect("drop"),
		skipSpaces,
		expect("index"),
		skipSpaces,
		readIdent(&name),
		skipSpaces,
		expect("on"),
		skipSpaces,
		readIdent(&table),
		skipSpaces,
		checkEOF,
	}

	for _, step := range steps {
		if err := step(r); err != nil {
			return nil, err
		}
	}

	return plan.NewDropIndex(
		name,
		plan.NewUnresolvedTable(table),
	), nil
}

func parseIndexExpr(str string) (sql.Expression, error) {
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

func readExprs(exprs *[]string) parseFunc {
	return func(rd *bufio.Reader) error {
		var buf bytes.Buffer
		r, _, err := rd.ReadRune()
		if err != nil {
			if err == io.EOF {
				return errUnexpectedSyntax.New("(", "EOF")
			}
			return err
		}

		if r != '(' {
			return errUnexpectedSyntax.New("(", string(r))
		}

		var level int
		var hasNonIdentChars bool
		var singleQuote, doubleQuote bool
		var ignoreNext bool
		for {
			r, _, err := rd.ReadRune()
			if err != nil {
				return err
			}

			switch true {
			case singleQuote || doubleQuote:
				switch true {
				case ignoreNext:
					ignoreNext = false
				case r == '\\':
					ignoreNext = true
				case r == '"' && doubleQuote:
					doubleQuote = false
				case r == '\'' && singleQuote:
					singleQuote = false
				}
			case unicode.IsLetter(r) || r == '_':
			case r == '(':
				level++
				hasNonIdentChars = true
			case r == ')':
				level--
				if level < 0 {
					if hasNonIdentChars && len(*exprs) > 0 {
						return errUnexpectedSyntax.New(")", buf.String())
					}

					*exprs = append(*exprs, buf.String())
					buf.Reset()
					return nil
				}
			case r == '"':
				hasNonIdentChars = true
				doubleQuote = true
			case r == '\'':
				hasNonIdentChars = true
				singleQuote = true
			case r == ',' && level == 0:
				if hasNonIdentChars {
					return errUnexpectedSyntax.New(",", ")")
				}

				*exprs = append(*exprs, buf.String())
				buf.Reset()
				continue
			case !unicode.IsLetter(r) && r != '_' && !unicode.IsSpace(r):
				hasNonIdentChars = true
			}

			buf.WriteRune(r)
		}
	}
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
