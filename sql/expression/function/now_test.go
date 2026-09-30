// Copyright 2026 Dolthub, Inc.
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

func TestNow(t *testing.T) {
	f, _ := NewNow(sql.NewEmptyContext(), expression.NewGetField(0, types.LongText, "foo", false))
	date := time.Date(
		2021,      // year
		1,         // month
		1,         // day
		8,         // hour
		30,        // min
		15,        // sec
		123456789, // nsec
		time.UTC,  // location (UTC)
	)

	testCases := []struct {
		name     string
		row      sql.Row
		expected any
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, true},
		{"different int type", sql.NewRow(int8(0)), time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second(), 0, time.UTC), false},
		{"precision of -1", sql.NewRow(-1), nil, true},
		{"precision of 0", sql.NewRow(0), time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second(), 0, time.UTC), false},
		{"precision of 1", sql.NewRow(1), time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second(), 100000000, time.UTC), false},
		{"precision of 2", sql.NewRow(2), time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second(), 120000000, time.UTC), false},
		{"precision of 3", sql.NewRow(3), time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second(), 123000000, time.UTC), false},
		{"precision of 4", sql.NewRow(4), time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second(), 123400000, time.UTC), false},
		{"precision of 5", sql.NewRow(5), time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second(), 123450000, time.UTC), false},
		{"precision of 6", sql.NewRow(6), time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second(), 123456000, time.UTC), false},
		{"precision of 7 which is too high", sql.NewRow(7), nil, true},
		{"incorrect type", sql.NewRow("notanint"), nil, true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			sql.RunWithNowFunc(func() time.Time {
				return date
			}, func() error {
				ctx := sql.NewEmptyContext()
				require := require.New(t)
				val, err := f.Eval(ctx, tt.row)
				if tt.err {
					require.Error(err)
				} else {
					require.NoError(err)
					require.Equal(tt.expected, val)
				}
				return nil
			})
		})
	}
}
