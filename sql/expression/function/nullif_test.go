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

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestNullIf(t *testing.T) {
	testCases := []struct {
		ex1      interface{}
		ex2      interface{}
		expected interface{}
	}{
		{"foo", "bar", "foo"},
		{"foo", "foo", sql.Null},
		{nil, "foo", nil},
		{"foo", nil, "foo"},
		{nil, nil, nil},
		{"", nil, ""},
	}

	f := NewNullIf(
		sql.NewEmptyContext(),
		expression.NewGetField(0, sql.LongText, "ex1", true),
		expression.NewGetField(1, sql.LongText, "ex2", true),
	)
	require.Equal(t, sql.LongText, f.Type())

	var3 := sql.MustCreateStringWithDefaults(sqltypes.VarChar, 3)
	f = NewNullIf(
		sql.NewEmptyContext(),
		expression.NewGetField(0, var3, "ex1", true),
		expression.NewGetField(1, var3, "ex2", true),
	)
	require.Equal(t, var3, f.Type())

	for _, tc := range testCases {
		v, err := f.Eval(sql.NewEmptyContext(), sql.NewRow(tc.ex1, tc.ex2))
		require.NoError(t, err)
		require.Equal(t, tc.expected, v)
	}
}
