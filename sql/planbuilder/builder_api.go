// Copyright 2026 Dolthub, Inc.
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
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
)

// Builder turns SQL into sql.Nodes. Its methods do not panic with a parseErr: anything that can fail returns an error.
// The binding logic lives on panicBuilder, which reports errors by panicking and is reached through run. Only this file
// may name the |inner| field, which TestBuilderInnerOnlyUsedInAPIFile enforces.
type Builder struct {
	inner panicBuilder
}

// run calls |f| with the panicking builder and returns any error it raised with handleErr.
func (b *Builder) run(f func(*panicBuilder) error) (err error) {
	defer recoverParseErr(&err)
	return f(&b.inner)
}

func (b *Builder) Parse(query string, qFlags *sql.QueryFlags, multi bool) (ret sql.Node, parsed, remainder string, qProps *sql.QueryFlags, err error) {
	err = b.run(func(b *panicBuilder) (err error) {
		ret, parsed, remainder, qProps, err = b.parse(query, qFlags, multi)
		return err
	})
	return ret, parsed, remainder, qProps, err
}

func (b *Builder) BindOnly(stmt ast.Statement, s string, queryFlags *sql.QueryFlags) (ret sql.Node, flags *sql.QueryFlags, err error) {
	err = b.run(func(b *panicBuilder) error {
		outScope, err := b.bindOnly(stmt, s, queryFlags)
		flags = b.qFlags
		if err != nil {
			return err
		}
		ret = outScope.node
		return nil
	})
	return ret, flags, err
}

// ResolveSchemaDefaults resolves any column default value expressions for the specified |schema|, for the table
// named |tableName| and returns the schema with the default value expressions resolved, or an error if any of them
// can't be resolved. Note that any GetField expressions in the column default value expressions have not had their
// indexes corrected yet.
func (b *Builder) ResolveSchemaDefaults(db string, tableName string, schema sql.Schema) (ret sql.Schema, err error) {
	err = b.run(func(b *panicBuilder) error {
		ret = b.resolveSchemaDefaultsForTable(db, tableName, schema)
		return nil
	})
	return ret, err
}

// SetBindings resolves |bindings| and makes them available to later binding.
func (b *Builder) SetBindings(bindings map[string]ast.Expr) error {
	return b.run(func(b *panicBuilder) error {
		bindingExprs := make(map[string]sql.Expression)
		for i, bv := range bindings {
			bindingExprs[i] = b.buildScalar(&scope{}, bv)
		}
		b.SetBindingsWithExpr(bindingExprs)
		return nil
	})
}

// ConvertVal converts a parsed literal to a sql.Expression.
func (b *Builder) ConvertVal(v *ast.SQLVal) (ret sql.Expression, err error) {
	err = b.run(func(b *panicBuilder) error {
		ret = b.ConvertVal(v)
		return nil
	})
	return ret, err
}

// Below methods are used in Doltgres. TODO: maybe find way to not expose these methods

func (b *Builder) BuildScalarWithTable(expr ast.Expr, tableExpr ast.TableExpr) (ret sql.Expression, err error) {
	err = b.run(func(b *panicBuilder) error {
		ret = b.buildScalarWithTable(expr, tableExpr)
		return nil
	})
	return ret, err
}

func (b *Builder) BuildColumnDefaultValueWithTable(defExpr ast.Expr, tableExpr ast.TableExpr, typ sql.Type, nullable bool) (ret *sql.ColumnDefaultValue, err error) {
	err = b.run(func(b *panicBuilder) error {
		ret = b.buildColumnDefaultValueWithTable(defExpr, tableExpr, typ, nullable)
		return nil
	})
	return ret, err
}

func (b *Builder) SetBindingsWithExpr(e map[string]sql.Expression) { b.inner.SetBindingsWithExpr(e) }
func (b *Builder) SetDebug(val bool)                               { b.inner.SetDebug(val) }
func (b *Builder) SetParserOptions(opts ast.ParserOptions)         { b.inner.SetParserOptions(opts) }
func (b *Builder) BindCtx() *BindvarContext                        { return b.inner.BindCtx() }
func (b *Builder) ViewCtx() *ViewContext                           { return b.inner.ViewCtx() }
func (b *Builder) ProcCtx() *ProcContext                           { return b.inner.ProcCtx() }
func (b *Builder) TriggerCtx() *TriggerContext                     { return b.inner.TriggerCtx() }
func (b *Builder) Reset()                                          { b.inner.Reset() }
func (b *Builder) DisableAuth()                                    { b.inner.DisableAuth() }
func (b *Builder) EnableAuth()                                     { b.inner.EnableAuth() }
