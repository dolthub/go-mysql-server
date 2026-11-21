// Copyright 2026 BlueboxC
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

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

func TestDolt12220IgnoredGeneratedDefaults(t *testing.T) {
	script := queries.ScriptTest{
		Name:        "regression (analyzer): materialize ignored defaults before generated columns",
		SetUpScript: []string{"CREATE TABLE g (a INT PRIMARY KEY,n INT NOT NULL,m INT,g INT AS (n+1) STORED)"},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "INSERT IGNORE INTO g(a,m) VALUES (1,1)", SkipResultsCheck: true, ExpectedWarning: 1364, ExpectedWarningsCount: 1},
			{Query: "INSERT IGNORE INTO g(a,m) VALUES (2,2)", SkipResultsCheck: true, ExpectedWarning: 1364, ExpectedWarningsCount: 1},
			{Query: "SELECT CAST(a AS CHAR),CAST(n AS CHAR),CAST(m AS CHAR),CAST(g AS CHAR) FROM g ORDER BY a", Expected: []sql.Row{{"1", "0", "1", "1"}, {"2", "0", "2", "1"}}},
		},
	}
	t.Run("normal", func(t *testing.T) { enginetest.TestScript(t, enginetest.NewDefaultMemoryHarness(), script) })
	t.Run("prepared", func(t *testing.T) {
		// Warning assertions use the ordinary runner; execute the prepared INSERTs explicitly.
		prepared := script
		prepared.Assertions = append([]queries.ScriptTestAssertion(nil), script.Assertions...)
		for i := range prepared.Assertions {
			prepared.Assertions[i].ExpectedWarning = 0
			prepared.Assertions[i].ExpectedWarningsCount = 0
		}
		enginetest.TestScriptPrepared(t, enginetest.NewDefaultMemoryHarness(), prepared)
	})
}
