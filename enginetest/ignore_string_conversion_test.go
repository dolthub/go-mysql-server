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

func TestDolt12218IgnoredStringConversions(t *testing.T) {
	script := queries.ScriptTest{
		Name:        "regression (insert): convert ignored strings before truncation",
		SetUpScript: []string{"CREATE TABLE v (b VARCHAR(4))", "CREATE TABLE b (v VARBINARY(4))", "CREATE TABLE u (v VARCHAR(4))"},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "INSERT IGNORE INTO v VALUES ('éééééé')", SkipResultsCheck: true},
			{Query: "INSERT IGNORE INTO v VALUES (CAST('2020-01-01' AS DATETIME))", SkipResultsCheck: true},
			{Query: "INSERT IGNORE INTO v VALUES (123456),(X'616263646566')", SkipResultsCheck: true},
			{Query: "INSERT IGNORE INTO b VALUES (X'616263646566')", SkipResultsCheck: true},
			{Query: "INSERT INTO u VALUES ('old'),('data')", SkipResultsCheck: true},
			{Query: "UPDATE IGNORE u SET v=CAST('2020-01-01' AS DATETIME)", SkipResultsCheck: true},
			{Query: "SELECT b FROM v ORDER BY b", Expected: []sql.Row{{"1234"}, {"2020"}, {"abcd"}, {"éééé"}}},
			{Query: "SELECT HEX(v) FROM b", Expected: []sql.Row{{"61626364"}}},
			{Query: "SELECT v FROM u", Expected: []sql.Row{{"2020"}, {"2020"}}},
		},
	}
	t.Run("normal", func(t *testing.T) { enginetest.TestScript(t, enginetest.NewDefaultMemoryHarness(), script) })
	t.Run("prepared", func(t *testing.T) { enginetest.TestScriptPrepared(t, enginetest.NewDefaultMemoryHarness(), script) })
}
