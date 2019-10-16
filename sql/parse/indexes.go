package parse

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"unicode"

	"github.com/src-d/go-mysql-server/sql/plan"

	"github.com/src-d/go-mysql-server/sql"
)

func parseShowIndex(s string) (sql.Node, error) {
	r := bufio.NewReader(strings.NewReader(s))

	var table string
	err := parseFuncs{
		expect("show"),
		skipSpaces,
		oneOf("index", "indexes", "keys"),
		skipSpaces,
		oneOf("from", "in"),
		skipSpaces,
		readIdent(&table),
		skipSpaces,
		checkEOF,
	}.exec(r)

	if err != nil {
		return nil, err
	}

	return plan.NewShowIndexes(
		sql.UnresolvedDatabase(""),
		table,
		nil,
	), nil
}

func parseCreateIndex(ctx *sql.Context, s string) (sql.Node, error) {
	r := bufio.NewReader(strings.NewReader(s))

	var name, table, driver string
	var exprs []string
	var config = make(map[string]string)
	err := parseFuncs{
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
		expect("using"),
		skipSpaces,
		readIdent(&driver),
		skipSpaces,
		readExprs(&exprs),
		skipSpaces,
		optional(
			expect("with"),
			skipSpaces,
			readKeyValue(config),
			skipSpaces,
		),
		checkEOF,
	}.exec(r)

	if err != nil {
		return nil, err
	}

	var indexExprs = make([]sql.Expression, len(exprs))
	for i, e := range exprs {
		var err error
		indexExprs[i], err = parseExpr(ctx, e)
		if err != nil {
			return nil, err
		}
	}

	return plan.NewCreateIndex(
		name,
		plan.NewUnresolvedTable(table, ""),
		indexExprs,
		driver,
		config,
	), nil
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
			err := parseFuncs{
				skipSpaces,
				readIdent(&key),
				skipSpaces,
				expectRune('='),
				skipSpaces,
				readValue(&value),
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
	err := parseFuncs{
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
	}.exec(r)

	if err != nil {
		return nil, err
	}

	return plan.NewDropIndex(
		name,
		plan.NewUnresolvedTable(table, ""),
	), nil
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
