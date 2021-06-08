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

func TestJSONUnquote(t *testing.T) {
	require := require.New(t)
	js := NewJSONUnquote(sql.NewEmptyContext(), expression.NewGetField(0, sql.LongText, "json", false))

	testCases := []struct {
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{sql.Row{nil}, nil, false},
		{sql.Row{"\"abc\""}, `abc`, false},
		{sql.Row{"[1, 2, 3]"}, `[1, 2, 3]`, false},
		{sql.Row{"\"\t\u0032\""}, "\t2", false},
		{sql.Row{"\\"}, nil, true},
	}

	for _, tt := range testCases {
		result, err := js.Eval(sql.NewEmptyContext(), tt.row)

		if !tt.err {
			require.NoError(err)
			require.Equal(tt.expected, result)
		} else {
			require.NotNil(err)
		}
	}
}
