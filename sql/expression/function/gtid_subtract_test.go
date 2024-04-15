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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGtidSubtract(t *testing.T) {
	tests := []struct {
		left, right sql.Expression
		expected    any
		error       string
	}{
		// NULL cases
		{
			left:     nil,
			right:    nil,
			expected: nil,
		},
		{
			left:     newStringLiteral("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57"),
			right:    nil,
			expected: nil,
		},
		{
			left:     nil,
			right:    newStringLiteral("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57"),
			expected: nil,
		},
		{
			left:     expression.NewLiteral(nil, types.Null),
			right:    expression.NewLiteral(nil, types.Null),
			expected: nil,
		},
		{
			left:     newStringLiteral("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57"),
			right:    expression.NewLiteral(nil, types.Null),
			expected: nil,
		},
		{
			left:     expression.NewLiteral(nil, types.Null),
			right:    newStringLiteral("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57"),
			expected: nil,
		},

		// Error cases
		{
			left:  newStringLiteral("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57"),
			right: newStringLiteral("not a parseable SID:not a valid interval"),
			error: "invalid MySQL 5.6 GTID set (\"not a parseable SID:not a valid interval\"): invalid MySQL 5.6 SID \"not a parseable SID\"",
		},
		{
			left:  expression.NewLiteral(42, types.Int32),
			right: expression.NewLiteral(42, types.Int32),
			error: "invalid type: 42",
		},
		{
			left:  newStringLiteral("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57"),
			right: expression.NewLiteral(42, types.Int32),
			error: "invalid type: 42",
		},
		{
			left:  expression.NewLiteral(42, types.Int32),
			right: newStringLiteral("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57"),
			error: "invalid type: 42",
		},

		// MySQL documentation cases
		{
			left:     newStringLiteral("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57"),
			right:    newStringLiteral("3E11FA47-71CA-11E1-9E33-C80AA9429562:21"),
			expected: "3e11fa47-71ca-11e1-9e33-c80aa9429562:22-57",
		},
		{
			left:     newStringLiteral("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57"),
			right:    newStringLiteral("3E11FA47-71CA-11E1-9E33-C80AA9429562:20-25"),
			expected: "3e11fa47-71ca-11e1-9e33-c80aa9429562:26-57",
		},
		{
			left:     newStringLiteral("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57"),
			right:    newStringLiteral("3E11FA47-71CA-11E1-9E33-C80AA9429562:23-24"),
			expected: "3e11fa47-71ca-11e1-9e33-c80aa9429562:21-22:25-57",
		},
		{
			left:     newStringLiteral("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57"),
			right:    newStringLiteral("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57"),
			expected: "",
		},
	}

	for _, test := range tests {
		f := NewGtidSubtract(test.left, test.right)
		t.Run(f.String(), func(t *testing.T) {
			res, err := f.Eval(sql.NewEmptyContext(), nil)
			if test.error != "" {
				require.Equal(t, test.error, err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, res)
			}
		})
	}
}

func newStringLiteral(s string) sql.Expression {
	return expression.NewLiteral(s, types.MustCreateStringWithDefaults(sqltypes.VarChar, 100))
}
