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

func TestDolt12223BooleanConversions(t *testing.T) {
	script := queries.ScriptTest{
		Name:        "regression (types): convert YEAR ENUM and SET booleans like integers",
		SetUpScript: []string{"CREATE TABLE y (y YEAR,e ENUM('x','y'),s SET('x','y'))"},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "INSERT INTO y VALUES (TRUE,TRUE,TRUE),(FALSE,TRUE,FALSE)", SkipResultsCheck: true},
			{Query: "SELECT CAST(y AS CHAR),CAST(e AS CHAR),CAST(s AS CHAR) FROM y ORDER BY y", Expected: []sql.Row{{"0", "x", ""}, {"2001", "x", "x"}}},
		},
	}
	t.Run("normal", func(t *testing.T) { enginetest.TestScript(t, enginetest.NewDefaultMemoryHarness(), script) })
	t.Run("prepared", func(t *testing.T) { enginetest.TestScriptPrepared(t, enginetest.NewDefaultMemoryHarness(), script) })
}
