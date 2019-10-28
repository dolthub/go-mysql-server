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
var ErrViewsToDropNotFound = errors.NewKind("the list of views to drop must contain at least one view")

// parseCreateView parses
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
		readIdentList('.', &scopedName),
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

// parseDropView parses
// DROP VIEW [IF EXISTS] [db_name1.]view_name1 [, [db_name2.]view_name2, ...]
// [RESTRICT] [CASCADE]
// and returns a DropView node in case of success. As per MySQL specification,
// RESTRICT and CASCADE, if given, are parsed and ignored.
func parseDropView(ctx *sql.Context, s string) (sql.Node, error) {
	r := bufio.NewReader(strings.NewReader(s))

	var (
		views      []QualifiedName
		ifExists   bool
		unusedBool bool
	)

	err := parseFuncs{
		expect("drop"),
		skipSpaces,
		expect("view"),
		skipSpaces,
		multiMaybe(&ifExists, "if", "exists"),
		skipSpaces,
		readQualifiedIdentifierList(&views),
		skipSpaces,
		maybe(&unusedBool, "restrict"),
		skipSpaces,
		maybe(&unusedBool, "cascade"),
		checkEOF,
	}.exec(r)

	if err != nil {
		return nil, err
	}

	if len(views) < 1 {
		return nil, ErrViewsToDropNotFound.New()
	}

	plans := make([]sql.Node, len(views))
	for i, view := range views {
		plans[i] = plan.NewSingleDropView(sql.UnresolvedDatabase(view.db), view.name)
	}

	return plan.NewDropView(plans, ifExists), nil
}
