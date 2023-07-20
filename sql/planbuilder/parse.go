package planbuilder

import (
	goerrors "errors"
	"strings"
	"unicode"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

const maxAnalysisIterations = 8

// ErrMaxAnalysisIters is thrown when the analysis iterations are exceeded
var ErrMaxAnalysisIters = errors.NewKind("exceeded max analysis iterations (%d)")

// Parse parses the given SQL sentence and returns the corresponding node.
func Parse(ctx *sql.Context, cat sql.Catalog, query string) (ret sql.Node, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case parseErr:
				err = r.err
			default:
				panic(r)
			}
		}
	}()
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
	// trim spaces and empty statements
	s = strings.TrimRightFunc(s, func(r rune) bool {
		return r == ';' || unicode.IsSpace(r)
	})

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
			// trim spaces and empty statements
			parsed = strings.TrimRightFunc(parsed, func(r rune) bool {
				return r == ';' || unicode.IsSpace(r)
			})
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

func (b *PlanBuilder) Parse(ctx *sql.Context, query string, multi bool) (ret sql.Node, parsed, remainder string, err error) {
	b.nesting++
	if b.nesting > maxAnalysisIterations {
		return nil, "", "", ErrMaxAnalysisIters.New(maxAnalysisIterations)
	}
	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case parseErr:
				err = r.err
			default:
				panic(r)
			}
		}
	}()
	span, ctx := ctx.Span("parse", trace.WithAttributes(attribute.String("query", query)))
	defer span.End()

	s := strings.TrimSpace(query)
	// trim spaces and empty statements
	s = strings.TrimRightFunc(s, func(r rune) bool {
		return r == ';' || unicode.IsSpace(r)
	})

	var stmt sqlparser.Statement

	parsed = s
	if !multi {
		stmt, err = sqlparser.Parse(s)
	} else {
		var ri int
		stmt, ri, err = sqlparser.ParseOne(s)
		if ri != 0 && ri < len(s) {
			parsed = s[:ri]
			parsed = strings.TrimSpace(parsed)
			// trim spaces and empty statements
			parsed = strings.TrimRightFunc(parsed, func(r rune) bool {
				return r == ';' || unicode.IsSpace(r)
			})
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

	outScope := b.build(nil, stmt, s)

	return outScope.node, parsed, remainder, err
}
