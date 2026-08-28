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

package enginetest_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TestSetReturningFunctionQueries runs end-to-end queries with a set-returning function (an expression that
// returns a RowIter rather than a scalar, like Postgres's unnest or generate_series). The in-memory backend
// doesn't define any set-returning functions natively, so we register a custom one with the engine's catalog.
func TestSetReturningFunctionQueries(t *testing.T) {
	// Project nodes only expand set-returning expressions into rows when flagged with IncludesNestedIters.
	// GMS doesn't set the flag itself; integrators like Doltgres do it in an analyzer rule registered in the
	// global rule sets, which also disables the simple-select fast path in getBatchesForNode (that path runs
	// a fixed rule list and would skip any custom rule). Register an equivalent rule the same way, and
	// restore the globals when the test finishes.
	savedAlwaysBeforeDefault := analyzer.AlwaysBeforeDefault
	savedOnceAfterAll := analyzer.OnceAfterAll
	analyzer.AlwaysBeforeDefault = append(savedAlwaysBeforeDefault[:len(savedAlwaysBeforeDefault):len(savedAlwaysBeforeDefault)],
		analyzer.Rule{Id: analyzer.RuleId(-1), Apply: markSRFProjections})
	analyzer.OnceAfterAll = append(savedOnceAfterAll[:len(savedOnceAfterAll):len(savedOnceAfterAll)],
		analyzer.Rule{Id: analyzer.RuleId(-1), Apply: markSRFProjections})
	defer func() {
		analyzer.AlwaysBeforeDefault = savedAlwaysBeforeDefault
		analyzer.OnceAfterAll = savedOnceAfterAll
	}()

	harness := enginetest.NewDefaultMemoryHarness()
	harness.Setup(setup.MydbData)
	engine, err := harness.NewEngine(t)
	require.NoError(t, err)
	defer engine.Close()

	engine.EngineAnalyzer().Catalog.RegisterFunction(enginetest.NewContext(harness), sql.Function1{
		Name: "srf_seq",
		Fn: func(ctx *sql.Context, e sql.Expression) sql.Expression {
			return &srfSeqExpr{child: e}
		},
	})

	script := queries.ScriptTest{
		Name: "set-returning function expansion",
		SetUpScript: []string{
			"CREATE TABLE srf_t (id int primary key, n int);",
			"INSERT INTO srf_t VALUES (7, 3), (8, 1);",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT srf_seq(3);",
				Expected: []sql.Row{{1}, {2}, {3}},
			},
			{
				Query:    "SELECT id, srf_seq(n) FROM srf_t WHERE id = 7;",
				Expected: []sql.Row{{7, 1}, {7, 2}, {7, 3}},
			},
			{
				// The projection materialized below the sort expands the set-returning function; the final
				// projection must reference the expanded column rather than re-evaluate it, which would
				// multiply the rows again and clobber the sort order.
				Query:    "SELECT id AS source_id, srf_seq(n) AS elem FROM srf_t WHERE id = 7 ORDER BY elem DESC;",
				Expected: []sql.Row{{7, 3}, {7, 2}, {7, 1}},
			},
			{
				// run a second time in the same session
				Query:    "SELECT id AS source_id, srf_seq(n) AS elem FROM srf_t WHERE id = 7 ORDER BY elem DESC;",
				Expected: []sql.Row{{7, 3}, {7, 2}, {7, 1}},
			},
			{
				Query:    "SELECT id AS source_id, srf_seq(n) AS elem FROM srf_t ORDER BY elem DESC, source_id;",
				Expected: []sql.Row{{7, 3}, {7, 2}, {7, 1}, {8, 1}},
			},
			{
				// no ORDER BY: the aliased set-returning function is still materialized below the final projection
				Query:    "SELECT id AS source_id, srf_seq(n) AS elem FROM srf_t WHERE id = 7;",
				Expected: []sql.Row{{7, 1}, {7, 2}, {7, 3}},
			},
		},
	}

	enginetest.TestScriptWithEngine(t, engine, harness, script)
}

// markSRFProjections flags Project nodes whose projections contain a set-returning expression with
// IncludesNestedIters, which makes their iterators expand those expressions into rows.
func markSRFProjections(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, sel analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, node, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		p, ok := n.(*plan.Project)
		if !ok || p.IncludesNestedIters {
			return n, transform.SameTree, nil
		}
		for _, e := range p.Projections {
			hasSRF := transform.InspectExpr(ctx, e, func(ctx *sql.Context, e sql.Expression) bool {
				rie, ok := e.(sql.RowIterExpression)
				return ok && rie.ReturnsRowIter()
			})
			if hasSRF {
				return p.WithIncludesNestedIters(true), transform.NewTree, nil
			}
		}
		return n, transform.SameTree, nil
	})
}

// srfSeqExpr is a set-returning function that yields the integers 1 through n. It must be expanded into rows
// via EvalRowIter; evaluating it as a scalar is an error.
type srfSeqExpr struct {
	child sql.Expression
}

var _ sql.Expression = (*srfSeqExpr)(nil)
var _ sql.RowIterExpression = (*srfSeqExpr)(nil)

func (s *srfSeqExpr) Resolved() bool {
	return s.child.Resolved()
}

func (s *srfSeqExpr) String() string {
	return fmt.Sprintf("srf_seq(%s)", s.child.String())
}

func (s *srfSeqExpr) Type(*sql.Context) sql.Type {
	return types.Int64
}

func (s *srfSeqExpr) IsNullable(*sql.Context) bool {
	return false
}

func (s *srfSeqExpr) Eval(*sql.Context, sql.Row) (interface{}, error) {
	return nil, fmt.Errorf("srf_seq must be evaluated as a RowIter")
}

func (s *srfSeqExpr) Children() []sql.Expression {
	return []sql.Expression{s.child}
}

func (s *srfSeqExpr) WithChildren(_ *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}
	return &srfSeqExpr{child: children[0]}, nil
}

func (s *srfSeqExpr) EvalRowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	v, err := s.child.Eval(ctx, r)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return sql.RowsToRowIter(), nil
	}
	n, _, err := types.Int64.Convert(ctx, v)
	if err != nil {
		return nil, err
	}
	var rows []sql.Row
	for i := int64(1); i <= n.(int64); i++ {
		rows = append(rows, sql.Row{i})
	}
	return sql.RowsToRowIter(rows...), nil
}

func (s *srfSeqExpr) ReturnsRowIter() bool {
	return true
}
