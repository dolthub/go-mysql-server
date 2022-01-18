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

package grant_tables

import (
	"fmt"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"

	"github.com/stretchr/testify/require"
)

func TestGrantTableData(t *testing.T) {
	ctx := sql.NewEmptyContext()
	testTable := newGrantTable(
		"test",
		sql.Schema{
			&sql.Column{
				Name:       "pk",
				Type:       sql.Int64,
				Default:    nil,
				Nullable:   false,
				Source:     "test",
				PrimaryKey: true,
			},
			&sql.Column{
				Name:       "val1",
				Type:       sql.Int64,
				Default:    nil,
				Nullable:   false,
				Source:     "test",
				PrimaryKey: true,
			},
			&sql.Column{
				Name:       "val2",
				Type:       sql.Int64,
				Default:    nil,
				Nullable:   false,
				Source:     "test",
				PrimaryKey: true,
			},
		},
		testPK{},
		testSK{},
	)

	inserter := testTable.Inserter(ctx)
	updater := inserter.(sql.RowUpdater)
	deleter := inserter.(sql.RowDeleter)

	row1 := sql.Row{int64(1), int64(1), int64(2)}
	row2a := sql.Row{int64(2), int64(3), int64(4)}
	row2b := sql.Row{int64(2), int64(7), int64(8)}
	row3 := sql.Row{int64(3), int64(5), int64(6)}
	row4 := sql.Row{int64(4), int64(9), int64(0)}
	row5 := sql.Row{int64(5), int64(10), int64(11)}
	row6 := sql.Row{int64(6), int64(12), int64(13)}
	row7 := sql.Row{int64(7), int64(14), int64(15)}
	row8 := sql.Row{int64(8), int64(14), int64(15)}

	require.NoError(t, inserter.Insert(ctx, row1))
	require.NoError(t, inserter.Insert(ctx, row4))
	require.NoError(t, inserter.Insert(ctx, row2a))
	require.NoError(t, inserter.Insert(ctx, row3))
	require.NoError(t, updater.Update(ctx, row2a, row2b))
	require.NoError(t, deleter.Delete(ctx, row4))
	require.Error(t, inserter.Insert(ctx, row1))

	rows, err := sql.RowIterToRows(ctx, testTable.data.ToRowIter())
	require.NoError(t, err)
	require.ElementsMatch(t, []sql.Row{row1, row2b, row3}, rows)

	require.NoError(t, testTable.data.Put(row5))
	require.NoError(t, testTable.data.Put(row6))
	require.NoError(t, testTable.data.Put(row7))
	require.NoError(t, testTable.data.Put(row8))

	require.Equal(t, []sql.Row{row5}, testTable.data.Get(testPK{5}))
	require.True(t, testTable.data.Has(row6))
	require.ElementsMatch(t, []sql.Row{row7, row8}, testTable.data.Get(testSK{15, 14}))
	require.NoError(t, testTable.data.Remove(testSK{15, 14}, nil))
	require.False(t, testTable.data.Has(row7))
	require.False(t, testTable.data.Has(row8))
	require.NoError(t, testTable.data.Remove(nil, row5))
	require.False(t, testTable.data.Has(row5))
}

type testPK struct {
	val int64
}

var _ in_mem_table.InMemTableDataKey = testPK{}

func (tpk testPK) AssignValues(vals ...interface{}) (in_mem_table.InMemTableDataKey, error) {
	if len(vals) != 1 {
		return tpk, fmt.Errorf("wrong number of values")
	}
	val, ok := vals[0].(int64)
	if !ok {
		return tpk, fmt.Errorf("value is not int64")
	}
	return testPK{val}, nil
}

func (tpk testPK) RepresentedColumns() []uint16 {
	return []uint16{0}
}

type testSK struct {
	val2 int64
	val1 int64
}

var _ in_mem_table.InMemTableDataKey = testSK{}

func (tsk testSK) AssignValues(vals ...interface{}) (in_mem_table.InMemTableDataKey, error) {
	if len(vals) != 2 {
		return tsk, fmt.Errorf("wrong number of values")
	}
	val2, ok := vals[0].(int64)
	if !ok {
		return tsk, fmt.Errorf("value is not int64")
	}
	val1, ok := vals[1].(int64)
	if !ok {
		return tsk, fmt.Errorf("value is not int64")
	}
	return testSK{val2, val1}, nil
}

func (tsk testSK) RepresentedColumns() []uint16 {
	return []uint16{2, 1}
}
