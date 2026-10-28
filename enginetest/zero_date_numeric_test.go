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

func TestDolt12222ZeroDateNumeric(t *testing.T) {
	script := queries.ScriptTest{
		Name:        "regression (types): preserve zero date numeric conversions",
		SetUpScript: []string{"CREATE TABLE z (d DATETIME,dd DATE)"},
		Assertions: []queries.ScriptTestAssertion{
			{Query: "INSERT IGNORE INTO z VALUES ('0000-00-00 00:00:00','0000-00-00')", SkipResultsCheck: true},
			{Query: "INSERT INTO z VALUES ('2020-01-02 03:04:05','2020-01-02')", SkipResultsCheck: true},
			{Query: "SELECT CAST(CAST(d AS SIGNED) AS CHAR),CAST(CAST(dd AS SIGNED) AS CHAR) FROM z ORDER BY d", Expected: []sql.Row{{"0", "0"}, {"20200102030405", "20200102"}}},
		},
	}
	t.Run("normal", func(t *testing.T) { enginetest.TestScript(t, enginetest.NewDefaultMemoryHarness(), script) })
	t.Run("prepared", func(t *testing.T) { enginetest.TestScriptPrepared(t, enginetest.NewDefaultMemoryHarness(), script) })
}
