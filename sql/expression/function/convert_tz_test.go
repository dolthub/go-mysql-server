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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConvertTz(t *testing.T) {
	tests := []struct {
		name string
		datetime string
		fromTimeZone string
		toTimeZone string
		expectedResult string
	}{
		{
			name: "Simple timezone conversion",
			datetime: "2004-01-01 12:00:00",
			fromTimeZone: "GMT",
			toTimeZone: "MET",
			expectedResult: "2004-01-01 13:00:00",
		},
		{
			name: "Simple time shift",
			datetime: "2004-01-01 12:00:00",
			fromTimeZone: "+01:00",
			toTimeZone: "+10:00",
			expectedResult: "2004-01-01 21:00:00",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fn := NewConvertTz(sql.NewEmptyContext(), expression.NewLiteral(test.datetime, sql.Text), expression.NewLiteral(test.fromTimeZone, sql.Text), expression.NewLiteral(test.toTimeZone, sql.Text))

			res, err := fn.Eval(sql.NewEmptyContext(), sql.Row{})
			require.NoError(t, err)

			assert.Equal(t, test.expectedResult, res)
		})
	}

}