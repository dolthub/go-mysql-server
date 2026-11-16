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
	"runtime/trace"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"go.opentelemetry.io/otel/attribute"
	otel "go.opentelemetry.io/otel/trace"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

const maxAnalysisIterations = 8

// ErrMaxAnalysisIters is thrown when the analysis iterations are exceeded
var ErrMaxAnalysisIters = errors.NewKind("exceeded max analysis iterations (%d)")

func (b *Builder) Parse(query string, qFlags *sql.QueryFlags, multi bool) (ret sql.Node, parsed, remainder string, qProps *sql.QueryFlags, err error) {
	defer trace.StartRegion(b.ctx, "ParseOnly").End()
	b.nesting++
	if b.nesting > maxAnalysisIterations {
		return nil, "", "", nil, ErrMaxAnalysisIters.New(maxAnalysisIterations)
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
	span, ctx := b.ctx.Span("parse",
		otel.WithAttributes(attribute.String("query", b.ctx.RedactQueryForTrace(query))))
	defer span.End()

	if b.authQueryState != nil {
		if err = b.authQueryState.Error(); err != nil {
			b.handleErr(err)
		}
	}

	isTrigger := b.triggerCtx != nil && (b.triggerCtx.Call || b.triggerCtx.LoadOnly)
	stmt, parsed, remainder, err := b.parseStatement(ctx, query, multi, isTrigger)
	if err != nil {
		if goerrors.Is(err, ast.ErrEmpty) {
			ctx.Warn(0, "query was empty after trimming comments, so it will be ignored")
			return plan.NothingImpl, parsed, remainder, nil, nil
		}
		return nil, parsed, remainder, nil, sql.ErrSyntaxError.New(err.Error())
	}

	if qFlags != nil {
		b.qFlags = qFlags
	}

	outScope := b.build(nil, stmt, parsed)

	return outScope.node, parsed, remainder, b.qFlags, err
}

// parseStatement parses |query| with the builder's parser options. The parse trees of stored trigger statements, which
// are parsed again for every statement that might fire them, are cached in the session, keyed by the statement and the
// parser options it was parsed with.
func (b *Builder) parseStatement(ctx *sql.Context, query string, multi, storedTrigger bool) (stmt ast.Statement, parsed, remainder string, err error) {
	var key string
	if storedTrigger {
		parsed = sql.RemoveSpaceAndDelimiter(query, ';')
		// The same text parses differently under these options. A NUL byte doesn't start a statement that parses.
		key = parsed
		if b.parserOpts.AnsiQuotes {
			key = "\x00ansi_quotes\x00" + key
		}
		if b.parserOpts.PipesAsConcat {
			key = "\x00pipes_as_concat\x00" + key
		}
		if stmt, ok := ctx.Session.GetCachedQuery(key); ok {
			return stmt, parsed, "", nil
		}
	}
	stmt, parsed, remainder, err = b.parser.ParseWithOptions(ctx, query, ';', multi, b.parserOpts)
	if err == nil && storedTrigger {
		ctx.Session.CacheQuery(key, stmt)
	}
	return stmt, parsed, remainder, err
}

// TriggerTarget returns the table and the event named by a stored CREATE TRIGGER statement, read from its parse tree
// without building it. Building resolves the trigger's table, which is much more expensive than parsing, and the parse
// tree is cached in the session. |isTrigger| is false when |query| parses but is not a CREATE TRIGGER statement.
func (b *Builder) TriggerTarget(query string) (table, event string, isTrigger bool, err error) {
	stmt, _, _, err := b.parseStatement(b.ctx, query, false, true)
	if err != nil {
		return "", "", false, err
	}
	ddl, ok := stmt.(*ast.DDL)
	if !ok || ddl.TriggerSpec == nil || strings.ToLower(ddl.Action) != ast.CreateStr {
		return "", "", false, nil
	}
	return ddl.Table.Name.String(), ddl.TriggerSpec.Event, true, nil
}

func (b *Builder) BindOnly(stmt ast.Statement, s string, queryFlags *sql.QueryFlags) (sql.Node, *sql.QueryFlags, error) {
	outScope, err := b.bindOnly(stmt, s, queryFlags)
	if err != nil {
		return nil, b.qFlags, err
	}
	return outScope.node, b.qFlags, err
}

func (b *Builder) bindOnly(stmt ast.Statement, s string, queryFlags *sql.QueryFlags) (_ *scope, err error) {
	defer trace.StartRegion(b.ctx, "BindOnly").End()
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
	if b.authQueryState != nil {
		if err = b.authQueryState.Error(); err != nil {
			b.handleErr(err)
		}
	}
	if queryFlags != nil {
		b.qFlags = queryFlags
	}
	outScope := b.build(nil, stmt, s)
	return outScope, err
}
