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

package aggregation

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestLast(t *testing.T) {
	testCases := []struct {
		name     string
		rows     []sql.Row
		expected interface{}
	}{
		{"no rows", nil, nil},
		{"one row", []sql.Row{{"first"}}, "first"},
		{"three rows", []sql.Row{{"first"}, {"second"}, {"last"}}, "last"},
	}

	agg := NewLast(sql.NewEmptyContext(), expression.NewGetField(0, sql.Text, "", false))
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			result := aggregate(t, agg, tt.rows...)
			require.Equal(t, tt.expected, result)
		})
	}
}
