// Copyright 2020-2022 Dolthub, Inc.
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
)

// TestAdditionalQueries runs the additional query tests defined in queries/additional_tests.go
func TestAdditionalQueries(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	engine, err := harness.NewEngine(t)
	if err != nil {
		t.Fatal(err)
	}

	// Run all the additional tests
	for _, test := range queries.AdditionalQueryTests {
		t.Run(test.Query, func(t *testing.T) {
			// Skip tests marked as SkipPrepared with a comment explaining why
			if test.SkipPrepared {
				t.Skip("This test is skipped because it requires additional implementation work to pass")
			}

			enginetest.TestQueryWithEngine(t, harness, engine, test)
		})
	}
}

// TestAdditionalQueriesPrepared runs the additional query tests with prepared statements
func TestAdditionalQueriesPrepared(t *testing.T) {
	harness := enginetest.NewDefaultMemoryHarness()
	if harness.IsUsingServer() {
		t.Skip("prepared test depend on context for current sql_mode information, but it does not get updated when using ServerEngine")
	}

	engine, err := harness.NewEngine(t)
	if err != nil {
		t.Fatal(err)
	}

	// Run all the additional tests that aren't marked as SkipPrepared
	for _, test := range queries.AdditionalQueryTests {
		if test.SkipPrepared {
			continue
		}

		t.Run(test.Query, func(t *testing.T) {
			enginetest.TestPreparedQueryWithEngine(t, harness, engine, test)
		})
	}
}
