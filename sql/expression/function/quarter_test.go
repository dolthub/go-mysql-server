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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestTime_Quarter(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := NewQuarter(ctx, expression.NewGetField(0, types.LongText, "foo", false))
	nowTime := time.Now().UTC()

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{
			name:     "null date",
			row:      sql.NewRow(nil),
			expected: nil,
		},
		{
			name:     "1",
			row:      sql.NewRow(1),
			expected: nil,
		},
		{
			name:     "1.1",
			row:      sql.NewRow(1.1),
			expected: nil,
		},
		{
			name: "invalid type",
			row:  sql.NewRow([]byte{0, 1, 2}),
			err:  false,
		},
		{
			name:     "date as string",
			row:      sql.NewRow(stringDate),
			expected: 1,
		},
		{
			name:     "another date as string",
			row:      sql.NewRow("2008-08-01"),
			expected: 3,
		},
		{
			name:     "january",
			row:      sql.NewRow("2008-01-01"),
			expected: 1,
		},
		{
			name:     "february",
			row:      sql.NewRow("2008-02-01"),
			expected: 1,
		},
		{
			name:     "march",
			row:      sql.NewRow("2008-03-01"),
			expected: 1,
		},
		{
			name:     "april",
			row:      sql.NewRow("2008-04-01"),
			expected: 2,
		},
		{
			name:     "may",
			row:      sql.NewRow("2008-05-01"),
			expected: 2,
		},
		{
			name:     "june",
			row:      sql.NewRow("2008-06-01"),
			expected: 2,
		},
		{
			name:     "july",
			row:      sql.NewRow("2008-07-01"),
			expected: 3,
		},
		{
			name:     "august",
			row:      sql.NewRow("2008-08-01"),
			expected: 3,
		},
		{
			name:     "septemeber",
			row:      sql.NewRow("2008-09-01"),
			expected: 3,
		},
		{
			name:     "october",
			row:      sql.NewRow("2008-10-01"),
			expected: 4,
		},
		{
			name:     "november",
			row:      sql.NewRow("2008-11-01"),
			expected: 4,
		},
		{
			name:     "december",
			row:      sql.NewRow("2008-12-01"),
			expected: 4,
		},
		{
			name:     "date as time",
			row:      sql.NewRow(nowTime),
			expected: (int(nowTime.Month())-1)/3 + 1,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			val, err := f.Eval(ctx, tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, val)
			}
		})
	}
}
