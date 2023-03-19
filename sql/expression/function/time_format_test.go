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

	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/types"
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

	timeFormat := NewTimeFormat(timeLit, format)
	res, err := timeFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, "04-05-06|000007", res)

	timeFormat = NewTimeFormat(timeLit, nil)
	res, err = timeFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Nil(t, res)

	timeFormat = NewTimeFormat(nil, format)
	res, err = timeFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Nil(t, res)

	timeFormat = NewTimeFormat(timeLit, nullLiteral)
	res, err = timeFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Nil(t, res)

	timeFormat = NewTimeFormat(nullLiteral, format)
	res, err = timeFormat.Eval(nil, nil)
	assert.NoError(t, err)
	assert.Nil(t, res)
}
