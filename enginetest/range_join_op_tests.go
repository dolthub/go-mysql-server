// Copyright 2023 Dolthub, Inc.
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

package enginetest

import (
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql/memo"

	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
)

var biasedrangeCosters = map[string]memo.Coster{
	"inner":        memo.NewInnerBiasedCoster(),
	"lookup":       memo.NewLookupBiasedCoster(),
	"hash":         memo.NewHashBiasedCoster(),
	"merge":        memo.NewMergeBiasedCoster(),
	"partial":      memo.NewPartialBiasedCoster(),
	"slidingRange": memo.NewSlidingRangeBiasedCoster(),
}

func TestRangeJoinOps(t *testing.T, harness Harness) {
	for _, tt := range rangeJoinOpTests {
		t.Run(tt.name, func(t *testing.T) {
			e := mustNewEngine(t, harness)
			defer e.Close()
			for _, setup := range tt.setup {
				for _, statement := range setup {
					if sh, ok := harness.(SkippingHarness); ok {
						if sh.SkipQueryTest(statement) {
							t.Skip()
						}
					}
					ctx := NewContext(harness)
					RunQueryWithContext(t, e, harness, ctx, statement)
				}
			}
			for k, c := range biasedrangeCosters {
				e.Analyzer.Coster = c
				for _, tt := range tt.tests {
					evalJoinCorrectness(t, harness, e, fmt.Sprintf("%s join: %s", k, tt.Query), tt.Query, tt.Expected, tt.Skip)
				}
			}
		})
	}
}

func TestRangeJoinOpsPrepared(t *testing.T, harness Harness) {
	for _, tt := range joinOpTests {
		t.Run(tt.name, func(t *testing.T) {
			e := mustNewEngine(t, harness)
			defer e.Close()
			for _, setup := range tt.setup {
				for _, statement := range setup {
					if sh, ok := harness.(SkippingHarness); ok {
						if sh.SkipQueryTest(statement) {
							t.Skip()
						}
					}
					ctx := NewContext(harness)
					RunQueryWithContext(t, e, harness, ctx, statement)
				}
			}

			for k, c := range biasedrangeCosters {
				e.Analyzer.Coster = c
				for _, tt := range tt.tests {
					evalJoinCorrectnessPrepared(t, harness, e, fmt.Sprintf("%s join: %s", k, tt.Query), tt.Query, tt.Expected, tt.Skip)
				}
			}
		})
	}
}

var rangeJoinOpTests = []struct {
	name  string
	setup [][]string
	tests []JoinOpTests
}{
	{
		name: "simple range join",
		setup: [][]string{
			setup.MydbData[0],
			{
				"create table vals (val int primary key)",
				"create table ranges (min int primary key, max int, unique key(min,max))",
				"insert into vals values (0), (1), (2), (3), (4), (5), (6)",
				"insert into ranges values (0,2), (1,3), (2,4), (3,5), (4,6)",
			},
		},
		tests: []JoinOpTests{
			{
				Query: "select * from vals join ranges on val between min and max",
				Expected: []sql.Row{
					{0, 0, 2},
					{1, 0, 2},
					{1, 1, 3},
					{2, 0, 2},
					{2, 1, 3},
					{2, 2, 4},
					{3, 1, 3},
					{3, 2, 4},
					{3, 3, 5},
					{4, 2, 4},
					{4, 3, 5},
					{4, 4, 6},
					{5, 3, 5},
					{5, 4, 6},
					{6, 4, 6},
				},
			},
		},
	},
}
