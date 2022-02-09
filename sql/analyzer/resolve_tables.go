// Copyright 2020-2021 Dolthub, Inc.
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

package analyzer

import (
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

const dualTableName = "dual"

var dualTableSchema = sql.NewPrimaryKeySchema(sql.Schema{
	{Name: "dummy", Source: dualTableName, Type: sql.LongText, Nullable: false},
})
var dualTable = func() sql.Table {
	t := memory.NewTable(dualTableName, dualTableSchema)

	ctx := sql.NewEmptyContext()

	// Need to run through the proper inserting steps to add data to the dummy table.
	inserter := t.Inserter(ctx)
	inserter.StatementBegin(ctx)
	_ = inserter.Insert(sql.NewEmptyContext(), sql.NewRow("x"))
	_ = inserter.StatementComplete(ctx)
	_ = inserter.Close(ctx)
	return t
}()

// isDualTable returns whether the given table is the "dual" table.
func isDualTable(t sql.Table) bool {
	if t == nil {
		return false
	}
	return t.Name() == dualTableName && t.Schema().Equals(dualTableSchema.Schema)
}

func resolveTables(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("resolve_tables")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if n.Resolved() {
			return n, nil
		}
		t, ok := n.(*plan.UnresolvedTable)
		if !ok {
			return n, nil
		}
		return resolveTable(ctx, t, a)
	})
}

func resolveTable(ctx *sql.Context, t *plan.UnresolvedTable, a *Analyzer) (sql.Node, error) {
	name := t.Name()
	db := t.Database
	if db == "" {
		db = ctx.GetCurrentDatabase()
	}

	if t.AsOf != nil {
		// This is necessary to use functions in AS OF expressions. Because function resolution happens after table
		// resolution, we resolve any functions in the AsOf here in order to evaluate them immediately. A better solution
		// might be to defer evaluating the expression until later in the analysis, but that requires bigger changes.
		asOfExpr, err := expression.TransformUp(t.AsOf, resolveFunctionsInExpr(ctx, a))
		if err != nil {
			return nil, err
		}

		if !asOfExpr.Resolved() {
			return nil, sql.ErrInvalidAsOfExpression.New(asOfExpr.String())
		}

		asOf, err := asOfExpr.Eval(ctx, nil)
		if err != nil {
			return nil, err
		}

		rt, database, err := a.Catalog.TableAsOf(ctx, db, name, asOf)
		if err != nil {
			return handleTableLookupFailure(err, name, db, a, t)
		}

		a.Log("table resolved: %q as of %s", rt.Name(), asOf)
		return plan.NewResolvedTable(rt, database, asOf), nil
	}

	rt, database, err := a.Catalog.Table(ctx, db, name)
	if err != nil {
		return handleTableLookupFailure(err, name, db, a, t)
	}

	a.Log("table resolved: %s", t.Name())
	return plan.NewResolvedTable(rt, database, nil), nil
}

// setTargetSchemas fills in the target schema for any nodes in the tree that operate on a table node but also want to
// store supplementary schema information. This is useful for lazy resolution of column default values.
func setTargetSchemas(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("set_target_schema")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		t, ok := n.(sql.SchemaTarget)
		if !ok {
			return n, nil
		}

		table := getResolvedTable(n)
		if table == nil {
			return n, nil
		}

		return t.WithTargetSchema(table.Schema())
	})
}

func handleTableLookupFailure(err error, tableName string, dbName string, a *Analyzer, t *plan.UnresolvedTable) (sql.Node, error) {
	if sql.ErrDatabaseNotFound.Is(err) {
		if tableName == dualTableName {
			a.Log("table resolved: %q", t.Name())
			return plan.NewResolvedTable(dualTable, nil, nil), nil
		}
		if dbName == "" {
			return nil, sql.ErrNoDatabaseSelected.New()
		}
	} else if sql.ErrTableNotFound.Is(err) || sql.ErrDatabaseAccessDeniedForUser.Is(err) || sql.ErrTableAccessDeniedForUser.Is(err) {
		if tableName == dualTableName {
			a.Log("table resolved: %s", t.Name())
			return plan.NewResolvedTable(dualTable, nil, nil), nil
		}
	}

	return nil, err
}
