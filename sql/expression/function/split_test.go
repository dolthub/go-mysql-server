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

func TestSplit(t *testing.T) {
	testCases := []struct {
		name      string
		input     interface{}
		delimiter interface{}
		expected  interface{}
	}{
		{"has delimiter", "a-b-c", "-", []interface{}{"a", "b", "c"}},
		{"regexp delimiter", "a--b----c-d", "-+", []interface{}{"a", "b", "c", "d"}},
		{"does not have delimiter", "a.b.c", "-", []interface{}{"a.b.c"}},
		{"input is nil", nil, "-", nil},
		{"delimiter is nil", "a-b-c", nil, nil},
	}

	f := NewSplit(
		sql.NewEmptyContext(),
		expression.NewGetField(0, sql.LongText, "input", true),
		expression.NewGetField(1, sql.LongText, "delimiter", true),
	)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			v, err := f.Eval(sql.NewEmptyContext(), sql.NewRow(tt.input, tt.delimiter))
			require.NoError(t, err)
			require.Equal(t, tt.expected, v)
		})
	}
}
