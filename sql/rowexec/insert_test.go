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

package rowexec

import (
	"testing"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestInsertIgnoreConversions(t *testing.T) {
	testCases := []struct {
		name      string
		colType   sql.Type
		value     interface{}
		valueType sql.Type
		expected  interface{}
		err       bool
	}{
		{
			name:      "inserting a string into a integer defaults to a 0",
			colType:   types.Int64,
			value:     "dadasd",
			valueType: types.Text,
			expected:  int64(0),
			err:       true,
		},
		{
			name:      "string too long gets truncated",
			colType:   types.MustCreateStringWithDefaults(sqltypes.VarChar, 2),
			value:     "dadsa",
			valueType: types.Text,
			expected:  "da",
			err:       true,
		},
		{
			name:      "inserting a string into a datetime results in 0 time",
			colType:   types.Datetime,
			value:     "dadasd",
			valueType: types.Text,
			expected:  time.Unix(-62167219200, 0).UTC(),
			err:       true,
		},
		{
			name:      "inserting a negative into an unsigned int results in 0",
			colType:   types.Uint64,
			value:     int64(-1),
			expected:  uint64(1<<64 - 1),
			valueType: types.Uint64,
			err:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := memory.NewDatabase("foo")
			pro := memory.NewDBProvider(db)
			ctx := newContext(pro)

			table := memory.NewTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "c1", Source: "foo", Type: tc.colType},
			}), nil)

			insertPlan := plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewResolvedTable(table, nil, nil), plan.NewValues([][]sql.Expression{{
				expression.NewLiteral(tc.value, tc.valueType),
			}}), false, []string{"c1"}, []sql.Expression{}, true)

			ri, err := DefaultBuilder.Build(ctx, insertPlan, nil)
			require.NoError(t, err)

			row, err := ri.Next(ctx)
			require.NoError(t, err)

			require.Equal(t, sql.UntypedSqlRow{tc.expected}, row)

			var warningCnt int
			if tc.err {
				warningCnt = 1
			}
			require.Equal(t, ctx.WarningCount(), uint16(warningCnt))
		})
	}
}
