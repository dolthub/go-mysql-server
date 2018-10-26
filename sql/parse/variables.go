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
			if err := readIdent(&s)(in); err != nil {
				return err
			}

			switch s {
			case "global", "session":
				if err := skipSpaces(in); err != nil {
					return err
				}

				return expect("variables")(in)
			case "variables":
				return nil
			}
			return errUnexpectedSyntax.New("show [global | session] variables", s)
		},
		skipSpaces,
		func(in *bufio.Reader) error {
			if expect("like")(in) == nil {
				if err := skipSpaces(in); err != nil {
					return err
				}

				if err := readValue(&pattern)(in); err != nil {
					return err
				}
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
