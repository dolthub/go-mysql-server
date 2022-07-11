// Copyright 2022 Dolthub, Inc.
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

package plan

import (
	"testing"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestInsertIgnoreConversions(t *testing.T) {
	ctx := sql.NewEmptyContext()
	testCases := []struct {
		name      string
		colType   sql.Type
		value     interface{}
		valueType sql.Type
		expected  interface{}
	}{
		{
			name:      "inserting a string into a integer defaults to a 0",
			colType:   sql.Int64,
			value:     "dadasd",
			valueType: sql.Text,
			expected:  int64(0),
		},
		{
			name:      "string too long gets truncated",
			colType:   sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2),
			value:     "dadsa",
			valueType: sql.Text,
			expected:  "da",
		},
		{
			name:      "inserting a string into a datetime results in 0 time",
			colType:   sql.Datetime,
			value:     "dadasd",
			valueType: sql.Text,
			expected:  time.Unix(-62167219200, 0).UTC(),
		},
		{
			name:      "inserting a negative into an unsigned int results in 0",
			colType:   sql.Uint64,
			value:     -1,
			valueType: sql.Int8,
			expected:  uint64(0),
		},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			table := memory.NewTable("foo", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "c1", Source: "foo", Type: tc.colType},
			}), nil)

			insertPlan := NewInsertInto(sql.UnresolvedDatabase(""), NewResolvedTable(table, nil, nil), NewValues([][]sql.Expression{{
				expression.NewLiteral(tc.value, tc.valueType),
			}}), false, []string{"c1"}, []sql.Expression{}, true)

			ri, err := insertPlan.RowIter(ctx, nil)
			require.NoError(t, err)

			row, err := ri.Next(ctx)
			require.NoError(t, err)

			require.Equal(t, row, sql.Row{tc.expected})

			// Validate that the number of warnings are increasing by 1 each time
			require.Equal(t, ctx.WarningCount(), uint16(i+1))
		})
	}
}
