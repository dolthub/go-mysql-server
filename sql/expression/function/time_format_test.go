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

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestTimeFormatting(t *testing.T) {
	dt := time.Date(2020, 2, 3, 4, 5, 6, 7000, time.UTC)
	tests := []struct {
		formatStr string
		expected  string
		expectErr bool
	}{
		{"%f", "000007", false},               // Microseconds (000000 to 999999)
		{"%h %p--%f", "04 AM--000007", false}, // Microseconds (000000 to 999999)
		{"%H", "04", false},                   // Hour (00 to 23)
		{"%h", "04", false},                   // Hour (00 to 12)
		{"%I", "04", false},                   // Hour (00 to 12)
		{"%i", "05", false},                   // Minutes (00 to 59)
		{"%p", "AM", false},                   // AM or PM
		{"%r", "04:05:06 AM", false},          // Time in 12 hour AM or PM format (hh:mm:ss AM/PM)
		{"%S", "06", false},                   // Seconds (00 to 59)
		{"%s", "06", false},                   // Seconds (00 to 59)
		{"%T", "04:05:06", false},             // Time in 24 hour format (hh:mm:ss)
		{"%U", "U", false},                    // Assert that unsupported (date) verbs are ignored
		{"%z", "z", false},                    // Assert that unsupported (unknown) verbs are ignored
	}

	for _, test := range tests {
		t.Run(dt.String()+test.formatStr, func(t *testing.T) {
			result, err := formatTime(test.formatStr, dt)

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

func TestTimeFormatEval(t *testing.T) {
	timeLit := expression.NewLiteral("04:05:06.000007", types.Time)
	format := expression.NewLiteral("%H-%i-%s|%f", types.Text)
	nullLiteral := expression.NewLiteral(nil, types.Null)
	ctx := sql.NewEmptyContext()

	timeFormat := NewTimeFormat(ctx, timeLit, format)
	res, err := timeFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, "04-05-06|000007", res)

	timeFormat = NewTimeFormat(ctx, expression.NewLiteral("25:01:02", types.Time), expression.NewLiteral("%H|%k", types.Text))
	res, err = timeFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, "25|25", res)

	timeFormat = NewTimeFormat(ctx, expression.NewLiteral("05:01:02", types.Time), expression.NewLiteral("%k", types.Text))
	res, err = timeFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, "5", res)

	timeFormat = NewTimeFormat(ctx, timeLit, nil)
	res, err = timeFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Nil(t, res)

	timeFormat = NewTimeFormat(ctx, nil, format)
	res, err = timeFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Nil(t, res)

	timeFormat = NewTimeFormat(ctx, timeLit, nullLiteral)
	res, err = timeFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Nil(t, res)

	timeFormat = NewTimeFormat(ctx, nullLiteral, format)
	res, err = timeFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Nil(t, res)
}

func TestTimeFormatErrorsDoNotPanic(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tests := []struct {
		name   string
		value  interface{}
		format interface{}
	}{
		{"invalid time", "not a time", "%H"},
		{"non-string format", "04:05:06", 1},
		{"incomplete format specifier", "04:05:06", "%"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn := NewTimeFormat(
				ctx,
				expression.NewLiteral(tt.value, types.LongText),
				expression.NewLiteral(tt.format, types.LongText),
			)
			var err error
			assert.NotPanics(t, func() {
				_, err = fn.Eval(ctx, nil)
			})
			assert.Error(t, err)
		})
	}
}
