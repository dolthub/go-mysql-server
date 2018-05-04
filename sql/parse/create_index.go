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

	var name, table string
	var exprs []string
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
		readExprs(&exprs),
		skipSpaces,
	}

	for _, step := range steps {
		if err := step(r); err != nil {
			return nil, err
		}
	}

	// TODO: parse using
	// TODO: parse config

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
		"",
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
			return err
		}

		if r != '(' {
			return errUnexpectedSyntax.New("(", r)
		}

		var level int
		var hasNonIdentChars bool
		for {
			r, _, err := rd.ReadRune()
			if err != nil {
				return err
			}

			switch true {
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
			if err := r.UnreadRune(); err != nil {
				return err
			}
			return nil
		}
	}
}
