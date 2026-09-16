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

func TestCurrTime(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ctx.SetQueryTime(time.Date(2021, 1, 1, 8, 30, 15, 123456789, time.UTC))

	tests := []struct {
		name      string
		precision sql.Expression
		expected  string
	}{
		{name: "default precision", expected: "08:30:15"},
		{name: "zero precision", precision: expression.NewLiteral(0, types.Int64), expected: "08:30:15"},
		{name: "millisecond precision", precision: expression.NewLiteral(3, types.Int64), expected: "08:30:15.123"},
		{name: "microsecond precision", precision: expression.NewLiteral(6, types.Int64), expected: "08:30:15.123456"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := []sql.Expression(nil)
			if test.precision != nil {
				args = append(args, test.precision)
			}
			currentTime, err := NewCurrTime(ctx, args...)
			require.NoError(t, err)

			cloned, err := currentTime.WithChildren(ctx, currentTime.Children()...)
			require.NoError(t, err)
			actual, err := cloned.Eval(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}
