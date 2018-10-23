package parse

import (
	"bufio"
	"strings"
)

import (
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

var errUnsupportedShowCreateQuery = errors.NewKind("Unsupported query: SHOW CREATE %s")

func parseShowCreate(s string) (sql.Node, error) {
	r := bufio.NewReader(strings.NewReader(s))

	var thingToShow string
	err := parseFuncs{
		expect("show"),
		skipSpaces,
		expect("create"),
		skipSpaces,
		readIdent(&thingToShow),
		skipSpaces,
	}.exec(r)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(thingToShow) {
	case "table":
		var name string

		err := parseFuncs{
			readQuotableIdent(&name),
			skipSpaces,
			checkEOF,
		}.exec(r)
		if err != nil {
			return nil, err
		}

		return plan.NewShowCreateTable(
			sql.UnresolvedDatabase("").Name(),
			nil,
			name), nil
	case "database", "schema":
		var ifNotExists bool
		var next string

		nextByte, err := r.Peek(1)
		if err != nil {
			return nil, err
		}

		// If ` is the next character, it's a db name. Otherwise it may be
		// a table name or IF NOT EXISTS.
		if nextByte[0] == '`' {
			if err := readQuotableIdent(&next)(r); err != nil {
				return nil, err
			}
		} else {
			if err := readIdent(&next)(r); err != nil {
				return nil, err
			}

			if next == "if" {
				ifNotExists = true
				err := parseFuncs{
					skipSpaces,
					expect("not"),
					skipSpaces,
					expect("exists"),
					skipSpaces,
					readQuotableIdent(&next),
				}.exec(r)
				if err != nil {
					return nil, err
				}
			}
		}

		err = parseFuncs{
			skipSpaces,
			checkEOF,
		}.exec(r)
		if err != nil {
			return nil, err
		}

		return plan.NewShowCreateDatabase(
			sql.UnresolvedDatabase(next),
			ifNotExists,
		), nil
	default:
		return nil, errUnsupportedShowCreateQuery.New(thingToShow)
	}
}
