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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestLength(t *testing.T) {
	testCases := []struct {
		name      string
		input     interface{}
		inputType sql.Type
		fn        func(*sql.Context, sql.Expression) sql.Expression
		expected  interface{}
	}{
		{
			"length string",
			"fóo",
			sql.Text,
			NewLength,
			int32(4),
		},
		{
			"length binary",
			[]byte("fóo"),
			sql.Blob,
			NewLength,
			int32(4),
		},
		{
			"length empty",
			"",
			sql.Blob,
			NewLength,
			int32(0),
		},
		{
			"length empty binary",
			[]byte{},
			sql.Blob,
			NewLength,
			int32(0),
		},
		{
			"length nil",
			nil,
			sql.Blob,
			NewLength,
			nil,
		},
		{
			"char_length string",
			"fóo",
			sql.LongText,
			NewCharLength,
			int32(3),
		},
		{
			"char_length binary",
			[]byte("fóo"),
			sql.Blob,
			NewCharLength,
			int32(3),
		},
		{
			"char_length empty",
			"",
			sql.Blob,
			NewCharLength,
			int32(0),
		},
		{
			"char_length empty binary",
			[]byte{},
			sql.Blob,
			NewCharLength,
			int32(0),
		},
		{
			"char_length nil",
			nil,
			sql.Blob,
			NewCharLength,
			nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			result, err := tt.fn(sql.NewEmptyContext(), expression.NewGetField(0, tt.inputType, "foo", false)).Eval(
				sql.NewEmptyContext(),
				sql.Row{tt.input},
			)

			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}
