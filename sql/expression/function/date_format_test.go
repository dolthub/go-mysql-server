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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestDateFormatting(t *testing.T) {
	dt := time.Date(2020, 2, 3, 4, 5, 6, 7000, time.UTC)
	tests := []struct {
		formatStr string
		expected  string
		expectErr bool
	}{
		{"%a", "Mon", false},         // Abbreviated weekday name (Sun to Sat)
		{"%b", "Feb", false},         // Abbreviated month name (Jan to Dec)
		{"%c", "2", false},           // Numeric month name (0 to 12)
		{"%D", "3rd", false},         // Day of the month as a numeric value, followed by suffix (1st, 2nd, 3rd, ...)
		{"%d", "03", false},          // Day of the month as a numeric value (00 to 31)
		{"%e", "3", false},           // Day of the month as a numeric value (0 to 31)
		{"%f", "000007", false},      // Microseconds (000000 to 999999)
		{"%H", "04", false},          // Hour (00 to 23)
		{"%h", "04", false},          // Hour (00 to 12)
		{"%I", "04", false},          // Hour (00 to 12)
		{"%i", "05", false},          // Minutes (00 to 59)
		{"%j", "034", false},         // Day of the year (001 to 366)
		{"%k", "4", false},           // Hour (0 to 23)
		{"%l", "4", false},           // Hour (1 to 12)
		{"%M", "February", false},    // Month name in full (January to December)
		{"%m", "02", false},          // Month name as a numeric value (00 to 12)
		{"%p", "AM", false},          // AM or PM
		{"%r", "04:05:06 AM", false}, // Time in 12 hour AM or PM format (hh:mm:ss AM/PM)
		{"%S", "06", false},          // Seconds (00 to 59)
		{"%s", "06", false},          // Seconds (00 to 59)
		{"%T", "04:05:06", false},    // Time in 24 hour format (hh:mm:ss)
		{"%W", "Monday", false},      // Weekday name in full (Sunday to Saturday)
		{"%w", "1", false},           // Day of the week where Sunday=0 and Saturday=6
		{"%Y", "2020", false},        // Year as a numeric, 4-digit value
		{"%y", "20", false},          // Year as a numeric, 2-digit value
		{"%U", "05", false},          // Week where Sunday is the first day of the week (00 to 53)
		{"%u", "06", false},          // Week where Monday is the first day of the week (00 to 53)
		{"%V", "05", false},          // Week where Sunday is the first day of the week (01 to 53). Used with %X
		{"%v", "06", false},          // Week where Monday is the first day of the week (01 to 53). Used with %X
		{"%X", "2020", false},        // Year for the week where Sunday is the first day of the week. Used with %V
		{"%x", "2020", false},        // Year for the week where Monday is the first day of the week. Used with %V
	}

	for _, test := range tests {
		t.Run(dt.String()+test.formatStr, func(t *testing.T) {
			result, err := formatDate(test.formatStr, dt)

			if test.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				t.Log(result)
				assert.Equal(t, test.expected, result)
			}
		})
	}
}

func TestUnsupportedSpecifiers(t *testing.T) {
	testFunc := func(t *testing.T, b byte) {
		if _, ok := specifierToFunc[b]; !ok {
			name := fmt.Sprintf("%%%s", string(b))
			t.Run(name, func(t *testing.T) {
				result, err := formatDate(name, time.Now())
				assert.NoError(t, err)
				assert.Equal(t, string(b), result)
			})
		}
	}

	capToLower := byte('a' - 'A')
	for i := byte('A'); i <= 'Z'; i++ {
		testFunc(t, i)
		testFunc(t, i+capToLower)
	}
}

func TestWeekYearFormatting(t *testing.T) {
	weekYearStartingMonday := "%x-W%v"
	weekYearStartingSunday := "%X-W%V"
	weekStartingMonday := "%u"
	weekStartingSunday := "%U"
	tests := []struct {
		name                  string
		dateStr               string
		expectedWYForMonStart string
		expectedWYForSunStart string
		expectedWForMonStart  string
		expectedWForSunStart  string
	}{
		{"Sat 1 Jan 2005", "2005-01-01", "2004-W53", "2004-W52", "00", "00"},
		{"Sun 2 Jan 2005", "2005-01-02", "2004-W53", "2005-W01", "00", "01"},
		{"Sat 31 Dec 2005", "2005-12-31", "2005-W52", "2005-W52", "52", "52"},
		{"Sun 1 Jan 2006", "2006-01-01", "2005-W52", "2006-W01", "00", "01"},
		{"Mon 2 Jan 2006", "2006-01-02", "2006-W01", "2006-W01", "01", "01"},
		{"Sun 31 Dec 2006", "2006-12-31", "2006-W52", "2006-W53", "52", "53"},
		{"Mon 1 Jan 2007", "2007-01-01", "2007-W01", "2006-W53", "01", "00"},
		{"Sun 30 Dec 2007", "2007-12-30", "2007-W52", "2007-W52", "52", "52"},
		{"Mon 31 Dec 2007", "2007-12-31", "2008-W01", "2007-W52", "53", "52"},
		{"Tue 1 Jan 2008", "2008-01-01", "2008-W01", "2007-W52", "01", "00"},
		{"Sun 28 Dec 2008", "2008-12-28", "2008-W52", "2008-W52", "52", "52"},
		{"Mon 29 Dec 2008", "2008-12-29", "2009-W01", "2008-W52", "53", "52"},
		{"Tue 30 Dec 2008", "2008-12-30", "2009-W01", "2008-W52", "53", "52"},
		{"Wed 31 Dec 2008", "2008-12-31", "2009-W01", "2008-W52", "53", "52"},
		{"Thu 1 Jan 2009", "2009-01-01", "2009-W01", "2008-W52", "01", "00"},
		{"Thu 31 Dec 2009", "2009-12-31", "2009-W53", "2009-W52", "53", "52"},
		{"Fri 1 Jan 2010", "2010-01-01", "2009-W53", "2009-W52", "00", "00"},
		{"Sat 2 Jan 2010", "2010-01-02", "2009-W53", "2009-W52", "00", "00"},
		{"Sun 3 Jan 2010", "2010-01-03", "2009-W53", "2010-W01", "00", "01"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dt, err := time.Parse("2006-01-02", test.dateStr)
			require.NoError(t, err)

			mondayWYStartResult, err := formatDate(weekYearStartingMonday, dt)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedWYForMonStart, mondayWYStartResult)

			sundayWYStartResult, err := formatDate(weekYearStartingSunday, dt)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedWYForSunStart, sundayWYStartResult)

			mondayWStartResult, err := formatDate(weekStartingMonday, dt)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedWForMonStart, mondayWStartResult)

			sundayWStartResult, err := formatDate(weekStartingSunday, dt)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedWForSunStart, sundayWStartResult)
		})
	}
}

func TestDateFormatEval(t *testing.T) {
	dt := time.Date(2020, 2, 3, 4, 5, 6, 7000, time.UTC)
	dateLit := expression.NewLiteral(dt, sql.Datetime)
	format := expression.NewLiteral("%Y-%m-%d %H:%i:%s.%f", sql.Text)
	nullLiteral := expression.NewLiteral(nil, sql.Null)

	dateFormat := NewDateFormat(sql.NewEmptyContext(), dateLit, format)
	res, err := dateFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, "2020-02-03 04:05:06.000007", res)

	dateFormat = NewDateFormat(sql.NewEmptyContext(), dateLit, nil)
	res, err = dateFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, nil, nil)

	dateFormat = NewDateFormat(sql.NewEmptyContext(), nil, format)
	res, err = dateFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, nil, nil)

	dateFormat = NewDateFormat(sql.NewEmptyContext(), dateLit, nullLiteral)
	res, err = dateFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, nil, nil)

	dateFormat = NewDateFormat(sql.NewEmptyContext(), nullLiteral, format)
	res, err = dateFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, nil, nil)
}
