// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planbuilder

import (
	goerrors "errors"
	"strings"
	"unicode"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

const maxAnalysisIterations = 8

// ErrMaxAnalysisIters is thrown when the analysis iterations are exceeded
var ErrMaxAnalysisIters = errors.NewKind("exceeded max analysis iterations (%d)")

// Parse parses the given SQL |query| using the default parsing settings and returns the corresponding node.
func Parse(ctx *sql.Context, cat sql.Catalog, query string) (ret sql.Node, err error) {
	sqlMode := sql.LoadSqlMode(ctx)
	var parserOpts ast.ParserOptions
	if err != nil {
		parserOpts = ast.ParserOptions{}
	} else {
		parserOpts = sqlMode.ParserOptions()
	}
	return ParseWithOptions(ctx, cat, query, parserOpts)
}

func ParseWithOptions(ctx *sql.Context, cat sql.Catalog, query string, options ast.ParserOptions) (ret sql.Node, err error) {
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
	ret, _, _, err = parse(ctx, cat, query, false, options)
	return
}

func ParseOne(ctx *sql.Context, cat sql.Catalog, query string) (sql.Node, string, string, error) {
	sqlMode := sql.LoadSqlMode(ctx)
	return parse(ctx, cat, query, true, sqlMode.ParserOptions())
}

func parse(ctx *sql.Context, cat sql.Catalog, query string, multi bool, options ast.ParserOptions) (sql.Node, string, string, error) {
	span, ctx := ctx.Span("parse", trace.WithAttributes(attribute.String("query", query)))
	defer span.End()

	s := strings.TrimSpace(query)
	// trim spaces and empty statements
	s = strings.TrimRightFunc(s, func(r rune) bool {
		return r == ';' || unicode.IsSpace(r)
	})

	var stmt ast.Statement
	var err error
	var parsed string
	var remainder string

	parsed = s
	if !multi {
		stmt, err = ast.ParseWithOptions(s, options)
	} else {
		var ri int
		stmt, ri, err = ast.ParseOneWithOptions(s, options)
		if ri != 0 && ri < len(s) {
			parsed = s[:ri]
			parsed = strings.TrimSpace(parsed)
			// trim spaces and empty statements
			parsed = strings.TrimRightFunc(parsed, func(r rune) bool {
				return r == ';' || unicode.IsSpace(r)
			})
			remainder = s[ri:]
		}
		return nil, parsed, remainder, err
	}

	if err != nil {
		if goerrors.Is(err, ast.ErrEmpty) {
			ctx.Warn(0, "query was empty after trimming comments, so it will be ignored")
			return plan.NothingImpl, parsed, remainder, nil
		}
		return nil, parsed, remainder, sql.ErrSyntaxError.New(err.Error())
	}

	b := New(ctx, cat)
	outScope := b.build(nil, stmt, s)

	return outScope.node, parsed, remainder, err
}

func (b *Builder) Parse(query string, multi bool) (ret sql.Node, parsed, remainder string, err error) {
	b.nesting++
	if b.nesting > maxAnalysisIterations {
		return nil, "", "", ErrMaxAnalysisIters.New(maxAnalysisIterations)
	}
	defer func() {
		b.nesting--
		if r := recover(); r != nil {
			switch r := r.(type) {
			case parseErr:
				err = r.err
			default:
				panic(r)
			}
		}
	}()
	span, ctx := b.ctx.Span("parse", trace.WithAttributes(attribute.String("query", query)))
	defer span.End()

	s := strings.TrimSpace(query)
	// trim spaces and empty statements
	s = strings.TrimRightFunc(s, func(r rune) bool {
		return r == ';' || unicode.IsSpace(r)
	})

	var stmt ast.Statement

	parsed = s
	if !multi {
		stmt, err = ast.ParseWithOptions(s, b.parserOpts)
	} else {
		var ri int
		stmt, ri, err = ast.ParseOneWithOptions(s, b.parserOpts)
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
		if goerrors.Is(err, ast.ErrEmpty) {
			ctx.Warn(0, "query was empty after trimming comments, so it will be ignored")
			return plan.NothingImpl, parsed, remainder, nil
		}
		return nil, parsed, remainder, sql.ErrSyntaxError.New(err.Error())
	}

	outScope := b.build(nil, stmt, s)

	return outScope.node, parsed, remainder, err
}

func (b *Builder) ParseOne(query string) (ret sql.Node, err error) {
	ret, _, _, err = b.Parse(query, false)
	return ret, err
}

func (b *Builder) BindOnly(stmt ast.Statement, s string) (ret sql.Node, err error) {
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
	outScope := b.build(nil, stmt, s)
	return outScope.node, err
}

func ParseOnly(ctx *sql.Context, query string, multi bool) (ast.Statement, string, string, error) {
	sqlMode := sql.LoadSqlMode(ctx)
	options := sqlMode.ParserOptions()

	s := strings.TrimSpace(query)
	// trim spaces and empty statements
	s = strings.TrimRightFunc(s, func(r rune) bool {
		return r == ';' || unicode.IsSpace(r)
	})

	var stmt ast.Statement
	var parsed string
	var remainder string
	var err error

	parsed = s
	if !multi {
		stmt, err = ast.ParseWithOptions(s, options)
	} else {
		var ri int
		stmt, ri, err = ast.ParseOneWithOptions(s, options)
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
	return stmt, query, remainder, err
}
