package parse

import (
	"bufio"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/plan"

	"gopkg.in/src-d/go-errors.v1"
	"vitess.io/vitess/go/vt/sqlparser"
)

var ErrMalformedViewName = errors.NewKind("the view name '%s' is not correct")
var ErrMalformedCreateView = errors.NewKind("view definition %#v is not a SELECT query")

// Parses
// CREATE [OR REPLACE] VIEW [db_name.]view_name AS select_statement
// and returns a NewCreateView node in case of success
func parseCreateView(ctx *sql.Context, s string) (sql.Node, error) {
	r := bufio.NewReader(strings.NewReader(s))

	var (
		databaseName, viewName string
		scopedName             []string
		subquery               string
		columns                []string
		isReplace              bool
	)

	err := parseFuncs{
		expect("create"),
		skipSpaces,
		multiMaybe(&isReplace, "or", "replace"),
		skipSpaces,
		expect("view"),
		skipSpaces,
		readScopedIdent('.', &scopedName),
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

	if len(scopedName) < 1 || len(scopedName) > 2 {
		return nil, ErrMalformedViewName.New(strings.Join(scopedName, "."))
	}

	// TODO(agarciamontoro): Add support for explicit column names
	if len(columns) != 0 {
		return nil, ErrUnsupportedSyntax.New("the view creation must not specify explicit column names")
	}

	if len(scopedName) == 1 {
		viewName = scopedName[0]
	}
	if len(scopedName) == 2 {
		databaseName = scopedName[0]
		viewName = scopedName[1]
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
		sql.UnresolvedDatabase(databaseName), viewName, columns, subqueryAlias, isReplace,
	), nil
}
