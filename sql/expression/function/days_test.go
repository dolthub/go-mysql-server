// Copyright 2024 Dolthub, Inc.
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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestToDays(t *testing.T) {
	tests := []struct {
		arg  sql.Expression
		exp  interface{}
		err  bool
		skip bool
	}{
		{
			arg: expression.NewLiteral(nil, types.Null),
			exp: nil,
		},
		{
			arg: expression.NewLiteral("notadate", types.Text),
			exp: nil,
		},
		{
			arg: expression.NewLiteral("-10", types.Int32),
			exp: nil,
		},
		{
			arg: expression.NewLiteral("0", types.Int32),
			exp: nil,
		},
		{
			arg: expression.NewLiteral("0000-00-00", types.Text),
			exp: nil,
		},
		{
			arg: expression.NewLiteral("0000-01-01", types.Text),
			exp: 1,
		},
		{
			arg: expression.NewLiteral("0000-01-02", types.Text),
			exp: 2,
		},
		{
			arg: expression.NewLiteral("1999-11-05", types.Text),
			exp: 730428,
		},
		{
			// Leap Year before leap day
			arg: expression.NewLiteral("2000-01-01", types.Text),
			exp: 730485,
		},
		{
			// Leap Year on leap day
			arg: expression.NewLiteral("2000-02-29", types.Text),
			exp: 730544,
		},
		{
			// Leap Year after leap day
			arg: expression.NewLiteral("2000-12-31", types.Text),
			exp: 730850,
		},

		{
			arg: expression.NewLiteral("0000-00-00 00:00:00", types.Text),
			exp: nil,
		},
		{
			arg: expression.NewLiteral("0000-01-01 00:00:00", types.Text),
			exp: 1,
		},
		{
			arg: expression.NewLiteral("0000-01-01 12:59:59", types.Text),
			exp: 1,
		},

		{
			arg: expression.NewLiteral(time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC), types.Date),
			exp: nil,
		},
		{
			arg: expression.NewLiteral(time.Date(1, 0, 0, 0, 0, 0, 0, time.UTC), types.Date),
			exp: 335,
		},
		{
			arg: expression.NewLiteral(time.Date(1, 0, 1, 0, 0, 0, 0, time.UTC), types.Date),
			exp: 336,
		},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("to_days(%v)", tt.arg)
		t.Run(name, func(t *testing.T) {
			f := NewToDays(tt.arg)
			res, err := f.Eval(sql.NewEmptyContext(), nil)
			if tt.err {
				require.Error(t, err)
				return
			}
			require.Equal(t, tt.exp, res)
		})
	}
}

func TestFromDays(t *testing.T) {
	tests := []struct {
		arg  sql.Expression
		exp  interface{}
		err  bool
		skip bool
	}{
		{
			arg: expression.NewLiteral(nil, types.Null),
			exp: nil,
		},
		{
			arg: expression.NewLiteral("notadate", types.Text),
			exp: "0000-00-00",
		},
		{
			arg: expression.NewLiteral(-10, types.Int32),
			exp: "0000-00-00",
		},
		{
			arg: expression.NewLiteral("366", types.Text),
			exp: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			arg: expression.NewLiteral(366.13, types.Float32),
			exp: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			// This should round down always
			skip: true,
			arg:  expression.NewLiteral(366.9999, types.Float32),
			exp:  time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		},

		{
			arg: expression.NewLiteral(0, types.Int32),
			exp: "0000-00-00",
		},
		{
			arg: expression.NewLiteral(1, types.Int32),
			exp: "0000-00-00",
		},
		{
			// Last day of year 0
			arg: expression.NewLiteral(365, types.Int32),
			exp: "0000-00-00",
		},
		{
			// First day of year 1
			arg: expression.NewLiteral(365+1, types.Int32),
			exp: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			// Last day of year 3
			arg: expression.NewLiteral(4*365, types.Int32),
			exp: time.Date(3, 12, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			// First day of year 4
			arg: expression.NewLiteral(4*365+1, types.Int32),
			exp: time.Date(4, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			// First leap day ever
			arg: expression.NewLiteral(4*365+31+29, types.Int32),
			exp: time.Date(4, 2, 29, 0, 0, 0, 0, time.UTC),
		},
		{
			// Last day of year 99
			arg: expression.NewLiteral(100*365+24, types.Int32),
			exp: time.Date(99, 12, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			// First day of year 100
			arg: expression.NewLiteral(100*365+24+1, types.Int32),
			exp: time.Date(100, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			// No leap day in year 100
			arg: expression.NewLiteral(100*365+24+31+29, types.Int32),
			exp: time.Date(100, 3, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			// Last day of year 399
			arg: expression.NewLiteral(400*365+97-1, types.Int32),
			exp: time.Date(399, 12, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			// First day of year 400
			arg: expression.NewLiteral(400*365+97, types.Int32),
			exp: time.Date(400, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			// 1999-11-05
			arg: expression.NewLiteral(730428, types.Int32),
			exp: time.Date(1999, 11, 5, 0, 0, 0, 0, time.UTC),
		},

		{
			// 1900-01-01
			arg: expression.NewLiteral(693961, types.Int32),
			exp: time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			// 1900-03-01
			arg: expression.NewLiteral(694020, types.Int32),
			exp: time.Date(1900, 3, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			// 1900-12-31
			arg: expression.NewLiteral(694325, types.Int32),
			exp: time.Date(1900, 12, 31, 0, 0, 0, 0, time.UTC),
		},

		{
			// 2000-01-01
			arg: expression.NewLiteral(730485, types.Int32),
			exp: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			// 2000-02-29
			arg: expression.NewLiteral(730544, types.Int32),
			exp: time.Date(2000, 2, 29, 0, 0, 0, 0, time.UTC),
		},
		{
			// 2000-12-31
			arg: expression.NewLiteral(730850, types.Int32),
			exp: time.Date(2000, 12, 31, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("from_days(%v)", tt.arg)
		t.Run(name, func(t *testing.T) {
			if tt.skip {
				t.Skip()
			}
			f := NewFromDays(tt.arg)
			res, err := f.Eval(sql.NewEmptyContext(), nil)
			if tt.err {
				require.Error(t, err)
				return
			}
			require.Equal(t, tt.exp, res)
		})
	}
}

func TestLastDay(t *testing.T) {
	tests := []struct {
		arg  sql.Expression
		exp  interface{}
		err  bool
		skip bool
	}{
		{
			arg: expression.NewLiteral(nil, types.Null),
			exp: nil,
		},
		{
			arg: expression.NewLiteral("notadate", types.Text),
			exp: nil,
		},
		{
			arg: expression.NewLiteral(-10, types.Int32),
			exp: nil,
		},
		{
			arg: expression.NewLiteral(366.13, types.Float32),
			exp: nil,
		},
		{
			arg: expression.NewLiteral("0004-02-01", types.Text),
			exp: time.Date(4, 2, 29, 0, 0, 0, 0, time.UTC),
		},
		{
			skip: true, // we should parse 00 days
			arg:  expression.NewLiteral("0001-11-00", types.Text),
			exp:  time.Date(1, 11, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			arg: expression.NewLiteral("2001-02-30", types.Text),
			exp: nil,
		},
		{
			arg: expression.NewLiteral("0001-01-31", types.Text),
			exp: time.Date(1, 1, 31, 0, 0, 0, 0, time.UTC),
		},

		{
			arg: expression.NewLiteral("0001-01-01", types.Text),
			exp: time.Date(1, 1, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			arg: expression.NewLiteral("0001-02-01", types.Text),
			exp: time.Date(1, 2, 28, 0, 0, 0, 0, time.UTC),
		},
		{
			arg: expression.NewLiteral("0001-03-01", types.Text),
			exp: time.Date(1, 3, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			arg: expression.NewLiteral("0001-04-01", types.Text),
			exp: time.Date(1, 4, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			arg: expression.NewLiteral("0001-05-01", types.Text),
			exp: time.Date(1, 5, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			arg: expression.NewLiteral("0001-06-01", types.Text),
			exp: time.Date(1, 6, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			arg: expression.NewLiteral("0001-07-01", types.Text),
			exp: time.Date(1, 7, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			arg: expression.NewLiteral("0001-08-01", types.Text),
			exp: time.Date(1, 8, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			arg: expression.NewLiteral("0001-09-01", types.Text),
			exp: time.Date(1, 9, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			arg: expression.NewLiteral("0001-10-01", types.Text),
			exp: time.Date(1, 10, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			arg: expression.NewLiteral("0001-11-01", types.Text),
			exp: time.Date(1, 11, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			arg: expression.NewLiteral("0001-12-01", types.Text),
			exp: time.Date(1, 12, 31, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("last_day(%v)", tt.arg)
		t.Run(name, func(t *testing.T) {
			if tt.skip {
				t.Skip()
			}
			f := NewLastDay(tt.arg)
			res, err := f.Eval(sql.NewEmptyContext(), nil)
			if tt.err {
				require.Error(t, err)
				return
			}
			require.Equal(t, tt.exp, res)
		})
	}
}
