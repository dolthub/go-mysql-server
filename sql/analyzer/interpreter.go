// Copyright 2022 Dolthub, Inc.
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
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// Interpreter is an interface that implements an interpreter. These are typically used for functions (which may be
// implemented as a set of operations that are interpreted during runtime).
type Interpreter interface {
	SetStatementRunner(ctx *sql.Context, runner StatementRunner) sql.Expression
}

// StatementRunner is essentially an interface that the engine will implement. We cannot directly reference the engine
// here as it will cause an import cycle, so this may be updated to suit any function changes that the engine
// experiences.
type StatementRunner interface {
	QueryWithBindings(ctx *sql.Context, query string, parsed sqlparser.Statement, bindings map[string]sqlparser.Expr, qFlags *sql.QueryFlags) (sql.Schema, sql.RowIter, *sql.QueryFlags, error)
}

// interpreter hands the engine to any interpreter expressions.
func interpreter(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeExprs(n, func(expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		if interp, ok := expr.(Interpreter); ok {
			return interp.SetStatementRunner(ctx, a.Runner), transform.NewTree, nil
		}
		return expr, transform.SameTree, nil
	})
}
