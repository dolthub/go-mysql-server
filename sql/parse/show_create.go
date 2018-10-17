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
	var name string
	steps := []parseFunc{
		expect("show"),
		skipSpaces,
		expect("create"),
		skipSpaces,
		readIdent(&thingToShow),
		skipSpaces,
		readIdent(&name),
		skipSpaces,
		checkEOF,
	}

	for _, step := range steps {
		if err := step(r); err != nil {
			return nil, err
		}
	}

	switch strings.ToLower(thingToShow) {
	case "table":
		return plan.NewShowCreateTable(
			sql.UnresolvedDatabase("").Name(),
			nil,
			name), nil
	default:
		return nil, errUnsupportedShowCreateQuery.New(name)
	}
}
