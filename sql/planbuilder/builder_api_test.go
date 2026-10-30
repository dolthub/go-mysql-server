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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	sqlparser "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

// TestBuilderInnerOnlyUsedInAPIFile keeps every route from the exported Builder API to panicking code in builder_api.go.
func TestBuilderInnerOnlyUsedInAPIFile(t *testing.T) {
	files, err := os.ReadDir(".")
	require.NoError(t, err)
	fset := token.NewFileSet()
	for _, f := range files {
		name := f.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == "builder_api.go" {
			continue
		}
		parsed, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		ast.Inspect(parsed, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "inner" {
				t.Errorf("%s: Builder.inner may only be used in builder_api.go", fset.Position(sel.Pos()))
			}
			return true
		})
	}
}

func TestSetBindingsReturnsError(t *testing.T) {
	b := New(sql.NewEmptyContext(), sql.MapCatalog{}, nil)
	for _, v := range []string{"NaN", "+Inf", "-Inf"} {
		require.Error(t, b.SetBindings(map[string]sqlparser.Expr{"v1": sqlparser.NewFloatVal([]byte(v))}), v)
	}
}

func TestInsertActiveRestoredAfterError(t *testing.T) {
	db := memory.NewDatabase("mydb")
	pro := memory.NewDBProvider(db)
	vars := variables.NewEngineVariables()
	sess := memory.NewSession(sql.NewBaseSessionWithVariables(vars), pro)
	ctx := sql.NewContextWithVariables(context.Background(), vars, sql.WithSession(sess))
	ctx.SetCurrentDatabase("mydb")

	b := New(ctx, newTestCatalog(db), nil)
	_, _, _, _, err := b.Parse("insert into xy values (1, 2, 3) on duplicate key update y = missing", nil, false)
	require.Error(t, err)
	require.False(t, b.inner.insertActive)
}

func TestBuildProcedureHelperReturnsParseError(t *testing.T) {
	db := memory.NewDatabase("mydb")
	_, _, err := BuildProcedureHelper(sql.NewEmptyContext(), newTestCatalog(db), false, nil, db, nil, sql.StoredProcedureDetails{
		Name:            "p",
		CreateStatement: "create procedure p( begin",
	})
	require.Error(t, err)
	require.True(t, sql.ErrSyntaxError.Is(err))
}
