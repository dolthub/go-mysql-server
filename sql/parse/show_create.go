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
			readIdent(&name),
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
				readIdent(&next),
			}.exec(r)
			if err != nil {
				return nil, err
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
