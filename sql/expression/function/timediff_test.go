// Copyright 2021 Dolthub, Inc.
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
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestTimeDiff(t *testing.T) {
	ctx := sql.NewEmptyContext()
	testCases := []struct {
		name     string
		from     sql.Expression
		to       sql.Expression
		expected string
		err      bool
	}{
		{
			"invalid type text",
			expression.NewLiteral("hello there", sql.Text),
			expression.NewConvert(expression.NewLiteral("01:00:00", sql.Text), expression.ConvertToTime),
			"",
			true,
		},
		//TODO: handle Date properly
		/*{
			"invalid type date",
			expression.NewConvert(expression.NewLiteral("2020-01-03", sql.Text), expression.ConvertToDate),
			expression.NewConvert(expression.NewLiteral("2020-01-04", sql.Text), expression.ConvertToDate),
			"",
			true,
		},*/
		{
			"type mismatch 1",
			expression.NewLiteral(time.Date(2008, time.December, 29, 1, 1, 1, 2, time.Local), sql.Timestamp),
			expression.NewConvert(expression.NewLiteral("01:00:00", sql.Text), expression.ConvertToTime),
			"",
			true,
		},
		{
			"type mismatch 2",
			expression.NewLiteral("00:00:00.2", sql.Text),
			expression.NewLiteral("2020-10-10 10:10:10", sql.Text),
			"",
			true,
		},
		{
			"valid mismatch",
			expression.NewLiteral(time.Date(2008, time.December, 29, 1, 1, 1, 2, time.Local), sql.Timestamp),
			expression.NewLiteral(time.Date(2008, time.December, 30, 1, 1, 1, 2, time.Local), sql.Datetime),
			"-24:00:00",
			false,
		},
		{
			"timestamp types 1",
			expression.NewLiteral(time.Date(2018, time.May, 2, 0, 0, 0, 0, time.Local), sql.Timestamp),
			expression.NewLiteral(time.Date(2018, time.May, 2, 0, 0, 1, 0, time.Local), sql.Timestamp),
			"-00:00:01",
			false,
		},
		{
			"timestamp types 2",
			expression.NewLiteral(time.Date(2008, time.December, 31, 23, 59, 59, 1, time.Local), sql.Timestamp),
			expression.NewLiteral(time.Date(2008, time.December, 30, 1, 1, 1, 2, time.Local), sql.Timestamp),
			"46:58:57.999999",
			false,
		},
		{
			"time types 1",
			expression.NewConvert(expression.NewLiteral("00:00:00.1", sql.Text), expression.ConvertToTime),
			expression.NewConvert(expression.NewLiteral("00:00:00.2", sql.Text), expression.ConvertToTime),
			"-00:00:00.100000",
			false,
		},
		{
			"time types 2",
			expression.NewLiteral("00:00:00.2", sql.Text),
			expression.NewLiteral("00:00:00.4", sql.Text),
			"-00:00:00.200000",
			false,
		},
		{
			"datetime types",
			expression.NewLiteral(time.Date(2008, time.December, 29, 0, 0, 0, 0, time.Local), sql.Datetime),
			expression.NewLiteral(time.Date(2008, time.December, 30, 0, 0, 0, 0, time.Local), sql.Datetime),
			"-24:00:00",
			false,
		},
		{
			"datetime string types",
			expression.NewLiteral("2008-12-29 00:00:00", sql.Text),
			expression.NewLiteral("2008-12-30 00:00:00", sql.Text),
			"-24:00:00",
			false,
		},
		{
			"datetime string mix types",
			expression.NewLiteral(time.Date(2008, time.December, 29, 0, 0, 0, 0, time.UTC), sql.Datetime),
			expression.NewLiteral("2008-12-30 00:00:00", sql.Text),
			"-24:00:00",
			false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			diff := NewTimeDiff(tt.from, tt.to)
			result, err := diff.Eval(ctx, nil)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}
}

func TestDateDiff(t *testing.T) {
	dt, _ := time.Parse("2006-Jan-02", "2019-Dec-31")
	testCases := []struct {
		name     string
		e1Type   sql.Type
		e2Type   sql.Type
		row      sql.Row
		expected interface{}
		err      *errors.Kind
	}{
		{"time and text types, ", sql.Datetime, sql.Text, sql.NewRow(dt, "2019-12-28"), int64(3), nil},
		{"text types, diff day, less than 24 hours time diff", sql.Text, sql.Text, sql.NewRow("2007-12-31 23:58:59", "2007-12-30 23:59:59"), int64(1), nil},
		{"text types, same day, 23:59:59 time diff", sql.Text, sql.Text, sql.NewRow("2007-12-30 23:59:59", "2007-12-30 00:00:00"), int64(0), nil},
		{"text types, diff day, 1 min time diff", sql.Text, sql.Text, sql.NewRow("2007-12-31 00:00:59", "2007-12-30 23:59:59"), int64(1), nil},
		{"text types, negative result", sql.Text, sql.Text, sql.NewRow("2010-11-30 22:59:59", "2010-12-31 23:59:59"), int64(-31), nil},
		{"text types, positive result", sql.Text, sql.Text, sql.NewRow("2007-12-31 23:59:59", "2007-12-30"), int64(1), nil},
		{"text types, negative result", sql.Text, sql.Text, sql.NewRow("2010-11-30 23:59:59", "2010-12-31"), int64(-31), nil},
		{"text types, day difference result", sql.Text, sql.Text, sql.NewRow("2017-06-25", "2017-06-15"), int64(10), nil},
		{"text types, year difference result", sql.Text, sql.Text, sql.NewRow("2017-06-25", "2016-06-15"), int64(375), nil},
		{"text types, format with /", sql.Text, sql.Text, sql.NewRow("2007/12/22", "2007/12/20"), int64(2), nil},
		{"text types, positive result", sql.Text, sql.Text, sql.NewRow("2007-12-31", "2007-12-29 23:59:59"), int64(2), nil},
		{"text types, negative result", sql.Text, sql.Text, sql.NewRow("2010-11-02", "2010-11-30 23:59:59"), int64(-28), nil},
	}

	for _, tt := range testCases {
		args0 := expression.NewGetField(0, tt.e1Type, "", false)
		args1 := expression.NewGetField(1, tt.e2Type, "", false)
		f := NewDateDiff(args0, args1)

		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := f.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err != nil {
				require.Error(err)
				require.True(tt.err.Is(err))
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}
}
