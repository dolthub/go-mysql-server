package parse

import (
	"bufio"
	"strings"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

var (
	errInvalidDescribeFormat = errors.NewKind("invalid format %q for DESCRIBE, supported formats: %s")
	describeSupportedFormats = []string{"tree"}
)

func parseDescribeQuery(ctx *sql.Context, s string) (sql.Node, error) {
	r := bufio.NewReader(strings.NewReader(s))

	var format, query string
	steps := []parseFunc{
		oneOf("describe", "desc", "explain"),
		skipSpaces,
		expect("format"),
		skipSpaces,
		expectRune('='),
		skipSpaces,
		readIdent(&format),
		skipSpaces,
		readRemaining(&query),
	}

	for _, step := range steps {
		if err := step(r); err != nil {
			return nil, err
		}
	}

	if format != "tree" {
		return nil, errInvalidDescribeFormat.New(
			format,
			strings.Join(describeSupportedFormats, ", "),
		)
	}

	child, err := Parse(ctx, query)
	if err != nil {
		return nil, err
	}

	return plan.NewDescribeQuery(format, child), nil
}
