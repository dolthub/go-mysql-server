package parse

import (
	"bufio"
	"strings"
	// "io"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/plan"

	"gopkg.in/src-d/go-errors.v1"
	"vitess.io/vitess/go/vt/sqlparser"
)

var ErrMalformedCreateView = errors.NewKind("view definition %#v is not a SELECT query")

// Parses
// CREATE [OR REPLACE] VIEW view_name [(col1, col2, ...)] AS select_statement
// and returns a NewCreateView node in case of success
func parseCreateView(ctx *sql.Context, s string) (sql.Node, error) {
	r := bufio.NewReader(strings.NewReader(s))

	var (
		viewName, subquery string
		columns            []string
		isReplace          bool
	)

	err := parseFuncs{
		expect("create"),
		skipSpaces,
		multiMaybe(&isReplace, "or", "replace"),
		skipSpaces,
		expect("view"),
		skipSpaces,
		readIdent(&viewName),
		skipSpaces,
		maybeList('(', ',', ')', &columns),
		skipSpaces,
		expect("as"),
		skipSpaces,
		readRemaining(&subquery),
		checkEOF,
	}.exec(r)

	if err != nil {
		return nil, err
	}

	subqueryStatement, err := sqlparser.Parse(subquery)
	if err != nil {
		return nil, err
	}

	selectStatement, ok := subqueryStatement.(*sqlparser.Select)
	if !ok {
		return nil, ErrMalformedCreateView.New(subqueryStatement)
	}

	subqueryNode, err := convertSelect(ctx, selectStatement)
	if err != nil {
		return nil, err
	}

	subqueryAlias := plan.NewSubqueryAlias(viewName, subqueryNode)

	return plan.NewCreateView(
		sql.UnresolvedDatabase(""), viewName, columns, subqueryAlias,
	), nil
}
