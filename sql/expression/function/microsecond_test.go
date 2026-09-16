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

func TestTime_Microsecond(t *testing.T) {
	ctx := sql.NewEmptyContext()
	f := NewMicrosecond(ctx, expression.NewGetField(0, types.LongText, "foo", false))
	testTime := time.Date(2001, 2, 3, 12, 34, 56, 123456789, time.UTC)

	testCases := []struct {
		name     string
		row      sql.Row
		expected any
		err      bool
	}{
		{"null date", sql.NewRow(nil), nil, false},
		{"invalid type", sql.NewRow([]byte{0, 1, 2}), nil, false},
		{"date as string", sql.NewRow(stringDate), uint64(0), false},
		{"date as time", sql.NewRow(testTime), uint64(123457), false},
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
