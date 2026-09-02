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
	"context"
	"testing"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestScalarFunctionTableAliasColumnName(t *testing.T) {
	db := memory.NewDatabase("mydb")
	ctx := sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), memory.NewDBProvider(db))))
	ctx.SetCurrentDatabase("mydb")

	tests := []struct {
		name             string
		postgresAliasing bool
		expectedColumn   string
	}{
		{name: "PostgreSQL compatibility", postgresAliasing: true, expectedColumn: "k"},
		{name: "MySQL compatibility", postgresAliasing: false, expectedColumn: "abs"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cat := tableFunctionTestCatalog{
				MapCatalog: sql.MapCatalog{
					Databases: map[string]sql.Database{"mydb": db},
					Funcs:     function.NewRegistry(),
				},
				overrides: sql.EngineOverrides{Builder: sql.BuilderOverrides{ScalarFunctionAliasAsColumn: tt.postgresAliasing}},
			}
			b := New(ctx, cat, nil)
			outScope := b.buildTableFunc(b.newScope(), tableFuncExpr("abs", "k", ast.NewIntVal([]byte("1"))))

			require.Equal(t, "k", outScope.node.(sql.Nameable).Name())
			require.Equal(t, tt.expectedColumn, outScope.node.Schema(ctx)[0].Name)
		})
	}
}

func TestNativeTableFunctionAliasPreservesColumnName(t *testing.T) {
	db := memory.NewDatabase("mydb")
	ctx := sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), memory.NewDBProvider(db))))
	ctx.SetCurrentDatabase("mydb")
	cat := tableFunctionTestCatalog{
		MapCatalog: sql.MapCatalog{
			Databases: map[string]sql.Database{"mydb": db},
			Funcs:     function.NewRegistry(),
		},
		tableFunctions: map[string]sql.TableFunction{"table_func": memory.TableFunc{}},
		overrides:      sql.EngineOverrides{Builder: sql.BuilderOverrides{ScalarFunctionAliasAsColumn: true}},
	}
	b := New(ctx, cat, nil)
	outScope := b.buildTableFunc(b.newScope(), tableFuncExpr(
		"table_func", "k", ast.NewStrVal([]byte("named_column")), ast.NewIntVal([]byte("1"))))

	require.Equal(t, "k", outScope.node.(sql.Nameable).Name())
	require.Equal(t, "named_column", outScope.node.Schema(ctx)[0].Name)
}

func TestRecordReturningFunctionAliasPreservesColumnNames(t *testing.T) {
	db := memory.NewDatabase("mydb")
	ctx := sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), memory.NewDBProvider(db))))
	ctx.SetCurrentDatabase("mydb")
	funcs := function.NewRegistry()
	require.NoError(t, funcs.Register(sql.Function1{
		Name: "record_func",
		Fn: func(_ *sql.Context, _ sql.Expression) sql.Expression {
			return &recordFunctionExpression{Literal: expression.NewLiteral(nil, types.Int64)}
		},
	}))
	cat := tableFunctionTestCatalog{
		MapCatalog: sql.MapCatalog{
			Databases: map[string]sql.Database{"mydb": db},
			Funcs:     funcs,
		},
		overrides: sql.EngineOverrides{Builder: sql.BuilderOverrides{ScalarFunctionAliasAsColumn: true}},
	}
	b := New(ctx, cat, nil)
	outScope := b.buildTableFunc(b.newScope(), tableFuncExpr("record_func", "r", ast.NewIntVal([]byte("1"))))

	require.Equal(t, "r", outScope.node.(sql.Nameable).Name())
	require.Equal(t, []string{"first", "second"}, []string{
		outScope.node.Schema(ctx)[0].Name,
		outScope.node.Schema(ctx)[1].Name,
	})
}

func tableFuncExpr(name, alias string, exprs ...ast.Expr) *ast.TableFuncExpr {
	aliasedExprs := make(ast.SelectExprs, len(exprs))
	for i, expr := range exprs {
		aliasedExprs[i] = &ast.AliasedExpr{Expr: expr}
	}
	return &ast.TableFuncExpr{Name: name, Alias: ast.NewTableIdent(alias), Exprs: aliasedExprs}
}

type tableFunctionTestCatalog struct {
	sql.MapCatalog
	tableFunctions map[string]sql.TableFunction
	overrides      sql.EngineOverrides
}

func (c tableFunctionTestCatalog) TableFunction(_ *sql.Context, name string) (sql.TableFunction, bool) {
	f, ok := c.tableFunctions[name]
	return f, ok
}

func (c tableFunctionTestCatalog) Overrides() sql.EngineOverrides {
	return c.overrides
}

type recordFunctionExpression struct {
	*expression.Literal
}

func (e *recordFunctionExpression) OutParametersSchema() sql.Schema {
	return sql.Schema{
		{Name: "first", Type: types.Int64},
		{Name: "second", Type: types.Int64},
	}
}

func (e *recordFunctionExpression) Unwrap(v any) sql.Row {
	return v.(sql.Row)
}
