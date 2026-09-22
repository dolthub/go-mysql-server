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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestUTCTimestamp(t *testing.T) {
	date := time.Date(2018, time.December, 2, 16, 25, 0, 0, time.Local)
	testNowFunc := func() time.Time {
		return date
	}

	var ctx *sql.Context
	err := sql.RunWithNowFunc(testNowFunc, func() error {
		ctx = sql.NewEmptyContext()
		return nil
	})
	require.NoError(t, err)

	tests := []struct {
		args      []sql.Expression
		result    time.Time
		expectErr bool
	}{
		{
			args:      nil,
			result:    date,
			expectErr: false,
		},
		{
			args:      []sql.Expression{expression.NewLiteral(0, types.Int8)},
			result:    date,
			expectErr: false,
		},
		{
			args:      []sql.Expression{expression.NewLiteral(0, types.Int64)},
			result:    date,
			expectErr: false,
		},
		{
			args:      []sql.Expression{expression.NewLiteral(6, types.Uint8)},
			result:    date,
			expectErr: false,
		},
		{
			args:      []sql.Expression{expression.NewLiteral(7, types.Int8)},
			result:    time.Time{},
			expectErr: true,
		},
		{
			args:      []sql.Expression{expression.NewLiteral(-1, types.Int8)},
			result:    time.Time{},
			expectErr: true,
		},
		{
			args:      []sql.Expression{expression.NewConvert(expression.NewLiteral("2020-10-10 01:02:03", types.Text), expression.ConvertToDatetime)},
			result:    time.Time{},
			expectErr: true,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprint(test.args), func(t *testing.T) {
			ut, err := NewUTCTimestamp(ctx, test.args...)
			if !test.expectErr {
				require.NoError(t, err)
				val, err := ut.Eval(ctx, nil)
				require.NoError(t, err)
				assert.Equal(t, test.result.UTC(), val)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
