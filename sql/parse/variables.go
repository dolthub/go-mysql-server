package parse

import (
	"bufio"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func parseShowVariables(ctx *sql.Context, s string) (sql.Node, error) {
	var pattern string

	r := bufio.NewReader(strings.NewReader(s))
	for _, fn := range []parseFunc{
		expect("show"),
		skipSpaces,
		func(in *bufio.Reader) error {
			var s string
			readIdent(&s)(in)
			switch s {
			case "global", "session":
				skipSpaces(in)
				return expect("variables")(in)
			case "variables":
				return nil
			}
			return errUnexpectedSyntax.New("show [global | session] variables", s)
		},
		skipSpaces,
		func(in *bufio.Reader) error {
			if expect("like")(in) == nil {
				skipSpaces(in)
				readValue(&pattern)(in)
			}
			return nil
		},
		skipSpaces,
		checkEOF,
	} {
		if err := fn(r); err != nil {
			return nil, err
		}
	}

	return plan.NewShowVariables(ctx.Session.GetAll(), pattern), nil
}
