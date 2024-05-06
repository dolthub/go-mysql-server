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
func Parse(ctx *sql.Context, cat sql.Catalog, query string) (sql.Node, error) {
	return ParseWithOptions(ctx, cat, query, sql.LoadSqlMode(ctx).ParserOptions())
}

func ParseWithOptions(ctx *sql.Context, cat sql.Catalog, query string, options ast.ParserOptions) (sql.Node, error) {
	// TODO: need correct parser
	b := New(ctx, cat, sql.NewMysqlParser())
	b.SetParserOptions(options)
	node, _, _, err := b.Parse(query, false)
	return node, err
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

	stmt, parsed, remainder, err := b.parser.ParseWithOptions(query, ';', multi, b.parserOpts)
	if err != nil {
		if goerrors.Is(err, ast.ErrEmpty) {
			ctx.Warn(0, "query was empty after trimming comments, so it will be ignored")
			return plan.NothingImpl, parsed, remainder, nil
		}
		return nil, parsed, remainder, sql.ErrSyntaxError.New(err.Error())
	}

	outScope := b.build(nil, stmt, parsed)

	return outScope.node, parsed, remainder, err
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
