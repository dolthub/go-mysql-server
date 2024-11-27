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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestConvertTz(t *testing.T) {
	tests := []struct {
		name           string
		datetime       interface{}
		fromTimeZone   string
		toTimeZone     string
		expectedResult interface{}
	}{
		{
			name:           "Simple timezone conversion",
			datetime:       "2004-01-01 12:00:00",
			fromTimeZone:   "GMT",
			toTimeZone:     "MET",
			expectedResult: time.Date(2004, 1, 1, 13, 0, 0, 0, time.UTC),
		},
		{
			name:           "Simple timezone conversion as datetime object",
			datetime:       time.Date(2004, 1, 1, 12, 0, 0, 0, time.UTC),
			fromTimeZone:   "GMT",
			toTimeZone:     "MET",
			expectedResult: time.Date(2004, 1, 1, 13, 0, 0, 0, time.UTC),
		},
		{
			name:           "Locations going backwards",
			datetime:       "2004-01-01 12:00:00",
			fromTimeZone:   "US/Eastern",
			toTimeZone:     "US/Central",
			expectedResult: time.Date(2004, 1, 1, 11, 0, 0, 0, time.UTC),
		},
		{
			name:           "Locations going forward",
			datetime:       "2004-01-01 12:00:00",
			fromTimeZone:   "US/Central",
			toTimeZone:     "US/Eastern",
			expectedResult: time.Date(2004, 1, 1, 13, 0, 0, 0, time.UTC),
		},
		{
			name:           "Simple time shift",
			datetime:       "2004-01-01 12:00:00",
			fromTimeZone:   "+01:00",
			toTimeZone:     "+10:00",
			expectedResult: time.Date(2004, 1, 1, 21, 0, 0, 0, time.UTC),
		},
		{
			name:           "Simple time shift with minutes",
			datetime:       "2004-01-01 12:00:00",
			fromTimeZone:   "+01:00",
			toTimeZone:     "+10:11",
			expectedResult: time.Date(2004, 1, 1, 21, 11, 0, 0, time.UTC),
		},
		{
			name:           "Different Time Format",
			datetime:       "20100603121212",
			fromTimeZone:   "+01:00",
			toTimeZone:     "+10:00",
			expectedResult: time.Date(2010, 6, 3, 21, 12, 12, 0, time.UTC),
		},
		{
			name:           "From location string to offset string",
			datetime:       "2004-01-01 12:00:00",
			fromTimeZone:   "UTC",
			toTimeZone:     "-10:00",
			expectedResult: time.Date(2004, 1, 1, 2, 0, 0, 0, time.UTC),
		},
		{
			name:           "From offset string to location string",
			datetime:       "2004-01-01 17:00:00",
			fromTimeZone:   "+10:00",
			toTimeZone:     "US/Central",
			expectedResult: time.Date(2004, 1, 1, 1, 0, 0, 0, time.UTC),
		},
		{
			name:           "Bad timezone conversion",
			datetime:       "2004-01-01 12:00:00",
			fromTimeZone:   "GMT",
			toTimeZone:     "HLP",
			expectedResult: nil,
		},
		{
			name:           "Bad Time Returns nils",
			datetime:       "2004-01-01 12:00:00dsa",
			fromTimeZone:   "+01:00",
			toTimeZone:     "+10:00",
			expectedResult: nil,
		},
		{
			name:           "Bad Duration Returns nil",
			datetime:       "2004-01-01 12:00:00",
			fromTimeZone:   "+01:00",
			toTimeZone:     "+10:00:11",
			expectedResult: nil,
		},
		{
			name:           "Negative time shift works accordingly",
			datetime:       "2004-01-02 12:00:00",
			fromTimeZone:   "-01:00",
			toTimeZone:     "+10:11",
			expectedResult: time.Date(2004, 1, 2, 23, 11, 0, 0, time.UTC),
		},
		{
			name:           "Test With negatives and datetime type",
			datetime:       time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC),
			fromTimeZone:   "-01:00",
			toTimeZone:     "+10:00",
			expectedResult: time.Date(2010, 6, 3, 23, 12, 12, 0, time.UTC),
		},
		{
			name:           "No symbol on toTimeZone errors",
			datetime:       time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC),
			fromTimeZone:   "-01:00",
			toTimeZone:     "10:00",
			expectedResult: nil,
		},
		{
			name:           "Test fromTimeZone value: SYSTEM",
			datetime:       time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC),
			fromTimeZone:   "SYSTEM",
			toTimeZone:     "+01:00",
			expectedResult: time.Date(2010, 6, 3, 13, 12, 12, 0, time.UTC),
		},
		{
			name:           "Test toTimeZone value: SYSTEM",
			datetime:       time.Date(2010, 6, 3, 12, 12, 12, 0, time.UTC),
			fromTimeZone:   "+01:00",
			toTimeZone:     "SYSTEM",
			expectedResult: time.Date(2010, 6, 3, 11, 12, 12, 0, time.UTC),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Set the system timezone to a known value so we can test convert_tz with the SYSTEM param
			loc, err := time.LoadLocation("UTC")
			time.Local = loc
			fn := NewConvertTz(expression.NewLiteral(test.datetime, types.Text), expression.NewLiteral(test.fromTimeZone, types.Text), expression.NewLiteral(test.toTimeZone, types.Text))

			res, err := fn.Eval(sql.NewEmptyContext(), sql.UntypedSqlRow{})
			require.NoError(t, err)

			assert.Equal(t, test.expectedResult, res)
		})
	}
}
