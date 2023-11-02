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

package analyzer

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestExprFlatten(t *testing.T) {
	tests := []struct {
		name     string
		in       sql.Expression
		exp      string
		leftover string
	}{
		{
			name: "ands",
			in: and(
				and(
					eq(gf(0, "xy", "x"), lit(1)),
					eq(gf(1, "xy", "y"), lit(2)),
				),
				gt(gf(1, "xy", "y"), lit(0)),
			),
			exp: `
(1: and
  (3: xy.x = 1)
  (4: xy.y = 2)
  (5: xy.y > 0))`,
		},
		{
			name: "ands with or",
			in: and(
				and(
					eq(gf(0, "xy", "x"), lit(1)),
					eq(gf(1, "xy", "y"), lit(2)),
				),
				or(
					gt(gf(1, "xy", "y"), lit(0)),
					lt(gf(1, "xy", "y"), lit(7)),
				),
			),
			exp: `
(1: and
  (3: xy.x = 1)
  (4: xy.y = 2)
  (5: or
    (7: and
      (6: xy.y > 0))
    (8: and
      (7: xy.y < 7))))`,
		},
		{
			name: "ors with and",
			in: or(
				or(
					eq(gf(0, "xy", "x"), lit(1)),
					eq(gf(1, "xy", "y"), lit(2)),
				),
				and(
					gt(gf(0, "xy", "y"), lit(0)),
					lt(gf(0, "xy", "y"), lit(7)),
				),
			),
			exp: `
(1: or
  (4: and
    (3: xy.x = 1))
  (5: and
    (4: xy.y = 2))
  (5: and
    (6: xy.y > 0)
    (7: xy.y < 7)))`,
		},
		{
			name: "include top-level leaf",
			in:   gt(gf(0, "xy", "y"), lit(0)),
			exp:  "(1: xy.y > 0)",
		},
		{
			name:     "exclude top-level leaf",
			in:       expression.NewLike(gf(0, "xy", "x"), lit(1), nil),
			exp:      ``,
			leftover: "xy.x LIKE 1",
		},
		{
			name: "exclude leaves",
			in: and(
				and(
					eq(gf(0, "xy", "x"), lit(1)),
					expression.NewLike(gf(0, "xy", "x"), lit(1), nil),
				),
				and(
					and(
						gt(gf(0, "xy", "y"), lit(0)),
						lt(gf(0, "xy", "y"), lit(7)),
					),
					expression.NewLike(gf(1, "xy", "y"), lit(1), nil),
				),
			),
			exp: `
(1: and
  (3: xy.x = 1)
  (7: xy.y > 0)
  (8: xy.y < 7))
`,
			leftover: `
AND
 ├─ xy.x LIKE 1
 └─ xy.y LIKE 1`,
		},
		{
			name: "exclude simple OR",
			in: and(
				or(
					eq(gf(0, "xy", "x"), lit(1)),
					expression.NewLike(gf(0, "xy", "x"), lit(1), nil),
				),
				and(
					gt(gf(0, "xy", "y"), lit(0)),
					lt(gf(0, "xy", "y"), lit(7)),
				),
			),
			exp: `
(1: and
  (5: xy.y > 0)
  (6: xy.y < 7))
`,
			leftover: `
Or
 ├─ Eq
 │   ├─ xy.x:0!null
 │   └─ 1 (bigint)
 └─ xy.x LIKE 1`,
		},
		{
			name: "exclude nested OR",
			in: and(
				or(
					and(
						eq(gf(0, "xy", "x"), lit(1)),
						expression.NewLike(gf(0, "xy", "x"), lit(1), nil),
					),
					eq(gf(0, "xy", "x"), lit(2)),
				),
				and(
					gt(gf(0, "xy", "y"), lit(0)),
					lt(gf(0, "xy", "y"), lit(7)),
				),
			),
			exp: `
(1: and
  (7: xy.y > 0)
  (8: xy.y < 7))
`,
			leftover: `
Or
 ├─ AND
 │   ├─ Eq
 │   │   ├─ xy.x:0!null
 │   │   └─ 1 (bigint)
 │   └─ xy.x LIKE 1
 └─ Eq
     ├─ xy.x:0!null
     └─ 2 (bigint)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newIndexCoster()
			root, leftover := c.flatten(tt.in)
			costTree := formatIndexFilter(root)
			require.Equal(t, strings.TrimSpace(tt.exp), strings.TrimSpace(costTree), costTree)
			if leftover != nil {
				leftoverCmp := sql.DebugString(leftover)
				require.Equal(t, strings.TrimSpace(tt.leftover), strings.TrimSpace(leftoverCmp), leftoverCmp)
			} else {
				require.Equal(t, "", tt.leftover)
			}
		})
	}
}
