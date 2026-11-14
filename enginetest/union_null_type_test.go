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

func TestDolt12221NullUnionTypes(t *testing.T) {
	script := queries.ScriptTest{
		Name:        "regression (planbuilder): preserve floating and temporal NULL union types",
		SetUpScript: []string{"CREATE TABLE u AS SELECT NULL AS authored UNION ALL SELECT 1E0", "CREATE TABLE d AS SELECT CAST('2020-01-01' AS DATETIME) AS authored UNION ALL SELECT NULL", "CREATE TABLE b (x BIT(8))"},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "INSERT INTO b SELECT 1E0 UNION ALL SELECT NULL", SkipResultsCheck: true},
			{Query: "SELECT column_name,column_type FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name IN ('d','u') ORDER BY table_name", Expected: []sql.Row{{"authored", "datetime"}, {"authored", "double"}}},
			{Query: "SELECT HEX(x) FROM b ORDER BY x", Expected: []sql.Row{{nil}, {"1"}}},
			{Query: "SELECT CAST(authored AS CHAR) FROM u ORDER BY authored", Expected: []sql.Row{{nil}, {"1"}}},
			{Query: "SELECT CAST(x AS CHAR) FROM (SELECT NULL AS x UNION ALL SELECT 1) t ORDER BY x", Expected: []sql.Row{{nil}, {"1"}}},
		},
	}
	t.Run("normal", func(t *testing.T) { enginetest.TestScript(t, enginetest.NewDefaultMemoryHarness(), script) })
	t.Run("prepared", func(t *testing.T) { enginetest.TestScriptPrepared(t, enginetest.NewDefaultMemoryHarness(), script) })
}
