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
	"github.com/dolthub/go-mysql-server/sql/expression/function"
)

func TestCallsAggregate(t *testing.T) {
	db := memory.NewDatabase("mydb")
	ctx := sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), memory.NewDBProvider(db))))
	ctx.SetCurrentDatabase("mydb")
	call := &ast.FuncExpr{Name: ast.NewColIdent("count"), Exprs: ast.SelectExprs{&ast.AliasedExpr{Expr: ast.NewIntVal([]byte("1"))}}}

	tests := []struct {
		name           string
		callsAggregate func(ctx *sql.Context, schemaName string, name string, args func() []sql.Expression) (bool, error)
		isAggregate    bool
	}{
		{name: "MySQL compatibility", isAggregate: true},
		{name: "aggregate call", isAggregate: true, callsAggregate: func(ctx *sql.Context, schemaName string, name string, args func() []sql.Expression) (bool, error) {
			return true, nil
		}},
		{name: "regular call", isAggregate: false, callsAggregate: func(ctx *sql.Context, schemaName string, name string, args func() []sql.Expression) (bool, error) {
			return len(args()) != 1, nil
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cat := tableFunctionTestCatalog{
				MapCatalog: sql.MapCatalog{
					Databases: map[string]sql.Database{"mydb": db},
					Funcs:     function.NewRegistry(),
				},
			}
			if tt.callsAggregate != nil {
				cat.overrides.Builder.FunctionProvider = callsAggregateTestProvider{
					FunctionProvider: cat.MapCatalog,
					callsAggregate:   tt.callsAggregate,
				}
			}
			b := New(ctx, cat, nil)
			inScope := b.newScope()
			b.buildScalar(inScope, call)
			require.Equal(t, tt.isAggregate, inScope.groupBy != nil)
		})
	}
}

// callsAggregateTestProvider is a function provider whose CallsAggregate is set by each test.
type callsAggregateTestProvider struct {
	sql.FunctionProvider
	callsAggregate func(ctx *sql.Context, schemaName string, name string, args func() []sql.Expression) (bool, error)
}

var _ sql.AggregateCallResolver = callsAggregateTestProvider{}

// CallsAggregate implements the interface sql.AggregateCallResolver.
func (p callsAggregateTestProvider) CallsAggregate(ctx *sql.Context, schema, name string, args func() []sql.Expression) (bool, error) {
	return p.callsAggregate(ctx, schema, name, args)
}
