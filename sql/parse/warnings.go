package parse

import (
	"bufio"
	"strconv"
	"strings"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

var errInvalidIndex = errors.NewKind("invalid %s index %d (index must be non-negative)")

func parseShowWarnings(ctx *sql.Context, s string) (sql.Node, error) {
	var (
		offstr string
		cntstr string
	)

	r := bufio.NewReader(strings.NewReader(s))
	for _, fn := range []parseFunc{
		expect("show"),
		skipSpaces,
		expect("warnings"),
		skipSpaces,
		func(in *bufio.Reader) error {
			if expect("limit")(in) == nil {
				skipSpaces(in)
				readValue(&cntstr)(in)
				skipSpaces(in)
				if expectRune(',')(in) == nil {
					if readValue(&offstr)(in) == nil {
						offstr, cntstr = cntstr, offstr
					}
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

	var (
		node   sql.Node = plan.ShowWarnings(ctx.Session.Warnings())
		offset int
		count  int
		err    error
	)
	if offstr != "" {
		if offset, err = strconv.Atoi(offstr); err != nil {
			return nil, err
		}
		if offset < 0 {
			return nil, errInvalidIndex.New("offset", offset)
		}
	}
	node = plan.NewOffset(int64(offset), node)
	if cntstr != "" {
		if count, err = strconv.Atoi(cntstr); err != nil {
			return nil, err
		}
		if count < 0 {
			return nil, errInvalidIndex.New("count", count)
		}
		if count > 0 {
			node = plan.NewLimit(int64(count), node)
		}
	}

	return node, nil
}
