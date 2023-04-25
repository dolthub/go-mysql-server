package optbuilder

import (
	goerrors "errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"strings"
)

// Parse parses the given SQL sentence and returns the corresponding node.
func Parse(ctx *sql.Context, cat sql.Catalog, query string) (sql.Node, error) {
	n, _, _, err := parse(ctx, cat, query, false)
	return n, err
}

func ParseOne(ctx *sql.Context, cat sql.Catalog, query string) (sql.Node, string, string, error) {
	return parse(ctx, cat, query, true)
}

func parse(ctx *sql.Context, cat sql.Catalog, query string, multi bool) (sql.Node, string, string, error) {
	span, ctx := ctx.Span("parse", trace.WithAttributes(attribute.String("query", query)))
	defer span.End()

	s := strings.TrimSpace(query)
	if strings.HasSuffix(s, ";") {
		s = s[:len(s)-1]
	}

	var stmt sqlparser.Statement
	var err error
	var parsed string
	var remainder string

	parsed = s
	if !multi {
		stmt, err = sqlparser.Parse(s)
	} else {
		var ri int
		stmt, ri, err = sqlparser.ParseOne(s)
		if ri != 0 && ri < len(s) {
			parsed = s[:ri]
			parsed = strings.TrimSpace(parsed)
			if strings.HasSuffix(parsed, ";") {
				parsed = parsed[:len(parsed)-1]
			}
			remainder = s[ri:]
		}
	}

	if err != nil {
		if goerrors.Is(err, sqlparser.ErrEmpty) {
			ctx.Warn(0, "query was empty after trimming comments, so it will be ignored")
			return plan.NothingImpl, parsed, remainder, nil
		}
		return nil, parsed, remainder, sql.ErrSyntaxError.New(err.Error())
	}

	b := &PlanBuilder{ctx: ctx, cat: cat}
	outScope := b.build(nil, stmt, s)

	return outScope.node, parsed, remainder, err
}
