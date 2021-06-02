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

package plan

import (
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

var testingTable *memory.Table
var testingTableSize int

func TestLimitPlan(t *testing.T) {
	require := require.New(t)
	table, _ := getTestingTable(t)
	limitPlan := NewLimit(expression.NewLiteral(0, sql.Int8), NewResolvedTable(table, nil, nil))
	require.Equal(1, len(limitPlan.Children()))

	iterator, err := getLimitedIterator(t, 1)
	require.NoError(err)
	require.NotNil(iterator)
}

func TestLimitImplementsNode(t *testing.T) {
	require := require.New(t)
	table, _ := getTestingTable(t)
	limitPlan := NewLimit(expression.NewLiteral(0, sql.Int8), NewResolvedTable(table, nil, nil))
	childSchema := table.Schema()
	nodeSchema := limitPlan.Schema()
	require.True(reflect.DeepEqual(childSchema, nodeSchema))
	require.True(receivesNode(limitPlan))
	require.True(limitPlan.Resolved())
}

func TestLimit0(t *testing.T) {
	_, size := getTestingTable(t)
	testingLimit := 0
	iterator, _ := getLimitedIterator(t, int64(testingLimit))
	testLimitOverflow(t, iterator, testingLimit, size)
}

func TestLimitLessThanTotal(t *testing.T) {
	_, size := getTestingTable(t)
	testingLimit := size - 1
	iterator, _ := getLimitedIterator(t, int64(testingLimit))
	testLimitOverflow(t, iterator, testingLimit, size)
}

func TestLimitEqualThanTotal(t *testing.T) {
	_, size := getTestingTable(t)
	testingLimit := size
	iterator, _ := getLimitedIterator(t, int64(testingLimit))
	testLimitOverflow(t, iterator, testingLimit, size)
}

func TestLimitGreaterThanTotal(t *testing.T) {
	_, size := getTestingTable(t)
	testingLimit := size + 1
	iterator, _ := getLimitedIterator(t, int64(testingLimit))
	testLimitOverflow(t, iterator, testingLimit, size)
}

func testLimitOverflow(t *testing.T, iter sql.RowIter, limit int, dataSize int) {
	require := require.New(t)
	for i := 0; i < limit+1; i++ {
		row, err := iter.Next()
		hint := fmt.Sprintf("Iter#%d : size.%d : limit.%d", i, dataSize, limit)
		if i >= limit || i >= dataSize {
			require.Nil(row, hint)
			require.Equal(io.EOF, err, hint)
		} else {
			require.NotNil(row, hint)
			require.Nil(err, hint)
		}
	}
}

func getTestingTable(t *testing.T) (*memory.Table, int) {
	t.Helper()
	if &testingTable == nil {
		return testingTable, testingTableSize
	}

	childSchema := sql.Schema{
		{Name: "col1", Type: sql.Text},
	}
	testingTable = memory.NewTable("test", childSchema)

	rows := []sql.Row{
		sql.NewRow("11a"),
		sql.NewRow("22a"),
		sql.NewRow("33a"),
	}

	for _, r := range rows {
		require.NoError(t, testingTable.Insert(sql.NewEmptyContext(), r))
	}

	return testingTable, len(rows)
}

func getLimitedIterator(t *testing.T, limitSize int64) (sql.RowIter, error) {
	t.Helper()
	ctx := sql.NewEmptyContext()
	table, _ := getTestingTable(t)
	limitPlan := NewLimit(expression.NewLiteral(limitSize, sql.Int64), NewResolvedTable(table, nil, nil))
	return limitPlan.RowIter(ctx, nil)
}

func receivesNode(n sql.Node) bool {
	return true
}
