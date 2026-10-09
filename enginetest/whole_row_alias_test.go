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
	"testing"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestWholeRowReferencePrecedesSelectAlias(t *testing.T) {
	for _, wholeRows := range []bool{false, true} {
		name := "without whole-row references"
		if wholeRows {
			name = "with whole-row references"
		}
		t.Run(name, func(t *testing.T) {
			harness := enginetest.NewDefaultMemoryHarness()
			harness.NewDatabases("mydb")
			overrides := sql.EngineOverrides{}
			if wholeRows {
				// GMS delegates the whole-row representation to the integrator. A
				// one-column tuple lets these SQL queries exercise the hook directly.
				overrides.Builder.ParseTableAsColumn = func(_ *sql.Context, _ string, fields []sql.Expression, _ []*sql.Column) (sql.Expression, error) {
					return expression.NewTuple(fields...), nil
				}
			}
			a := analyzer.NewBuilder(harness.Provider()).AddOverrides(overrides).Build()
			engine := sqle.New(a, &sqle.Config{IncludeRootAccount: true})
			defer engine.Close()
			script := queries.ScriptTest{
				Name: "relation name collides with a SELECT alias",
				SetUpScript: []string{
					"CREATE TABLE whole_row_input (value BIGINT);",
					"INSERT INTO whole_row_input VALUES (NULL), (1), (2);",
				},
				Assertions: []queries.ScriptTestAssertion{
					{
						Query: "SELECT value + 100 AS e, e, e IS NULL FROM whole_row_input AS e ORDER BY value;",
						Expected: []sql.Row{
							{nil, nil, true},
							{int64(101), int64(1), false},
							{int64(102), int64(2), false},
						},
					},
					{
						Query:    "SELECT value + 100 AS e, e FROM (SELECT value FROM whole_row_input) AS e WHERE value IS NOT NULL ORDER BY value;",
						Expected: []sql.Row{{int64(101), int64(1)}, {int64(102), int64(2)}},
					},
				},
			}
			if !wholeRows {
				for i := range script.Assertions {
					script.Assertions[i].Expected = nil
					script.Assertions[i].ExpectedErr = sql.ErrMisusedAlias
				}
			}
			enginetest.TestScriptWithEngine(t, engine, harness, script)
		})
	}
}
