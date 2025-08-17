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

	"github.com/stretchr/testify/require"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

func TestIfNull(t *testing.T) {
	testCases := []struct {
		expression     interface{}
		expressionType sql.Type
		value          interface{}
		valueType      sql.Type
		expected       interface{}
		expectedType   sql.Type
	}{
		{"foo", types.LongText, "bar", types.LongText, "foo", types.LongText},
		{"foo", types.LongText, "foo", types.LongText, "foo", types.LongText},
		{nil, types.LongText, "foo", types.LongText, "foo", types.LongText},
		{"foo", types.LongText, nil, types.LongText, "foo", types.LongText},
		{nil, types.LongText, nil, types.LongText, nil, types.LongText},
		{"", types.LongText, nil, types.LongText, "", types.LongText},
		{nil, types.Int8, 128, types.Int64, int64(128), types.Int64},
	}

	for _, tc := range testCases {
		f := NewIfNull(
			expression.NewGetField(0, tc.expressionType, "expression", true),
			expression.NewGetField(1, tc.valueType, "value", true),
		)
		require.Equal(t, tc.expectedType, f.Type())
		v, err := f.Eval(sql.NewEmptyContext(), sql.NewRow(tc.expression, tc.value))
		require.NoError(t, err)
		require.Equal(t, tc.expected, v)
	}
}
