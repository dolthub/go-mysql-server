// Copyright 2020-2021 Dolthub, Inc.
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

package function

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"
)

func TestLocate(t *testing.T) {
	intPtr := func(i int) *int {
		return &i
	}

	testCases := []struct {
		Name     string
		Substr   string
		Str      string
		Start    *int
		Expected int32
	}{
		{
			Name:     "locate",
			Substr:   "o",
			Str:      "locate",
			Expected: 2,
		},
		{
			Name:     "locate not found",
			Substr:   "notinlocate",
			Str:      "locate",
			Expected: 0,
		},
		{
			Name:     "locate with start before",
			Substr:   "ocate",
			Str:      "locate",
			Start:    intPtr(1),
			Expected: 2,
		},
		{
			Name:     "locate with start after",
			Substr:   "o",
			Str:      "locate",
			Start:    intPtr(3),
			Expected: 0,
		},
		{
			Name:     "locate with start after 1st iteration",
			Substr:   "o",
			Str:      "locate the second o",
			Start:    intPtr(5),
			Expected: 15,
		},
		{
			Name:     "locate is case insensitive",
			Substr:   "c",
			Str:      "LOCATE",
			Expected: 3,
		},
		{
			Name:     "locate with start 0",
			Substr:   "o",
			Str:      "locate",
			Start:    intPtr(0),
			Expected: 0,
		},
		{
			Name:     "locate empty substring",
			Substr:   "",
			Str:      "locate",
			Expected: 1,
		},
		{
			Name:     "locate empty substring",
			Substr:   "",
			Str:      "locate",
			Expected: 1,
		},
		{
			Name:     "locate all empty",
			Substr:   "",
			Str:      "",
			Expected: 1,
		},
		{
			Name:     "locate all empty with start > 1",
			Substr:   "",
			Str:      "",
			Start:    intPtr(2),
			Expected: 0,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			require := require.New(t)

			exprs := []sql.Expression{
				expression.NewGetField(0, sql.Text, "substr", false),
				expression.NewGetField(1, sql.LongText, "str", false),
			}
			row := sql.Row{tt.Substr, tt.Str}

			if tt.Start != nil {
				exprs = append(exprs, expression.NewGetField(2, sql.Int32, "start", false))
				row = append(row, *tt.Start)
			}

			f, err := NewLocate(exprs...)
			require.NoError(err)

			result, err := f.Eval(sql.NewEmptyContext(), row)
			require.NoError(err)
			require.Equal(tt.Expected, result)
		})
	}
}
