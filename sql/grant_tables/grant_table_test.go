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
		&testEntry{},
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

	rows, err := sql.RowIterToRows(ctx, testTable.data.ToRowIter(ctx))
	require.NoError(t, err)
	require.ElementsMatch(t, []sql.Row{row1, row2b, row3}, rows)

	require.NoError(t, testTable.data.Put(ctx, &testEntry{row5}))
	require.NoError(t, testTable.data.Put(ctx, &testEntry{row6}))
	require.NoError(t, testTable.data.Put(ctx, &testEntry{row7}))
	require.NoError(t, testTable.data.Put(ctx, &testEntry{row8}))

	require.Equal(t, []in_mem_table.Entry{&testEntry{row5}}, testTable.data.Get(testPK{5}))
	require.True(t, testTable.data.Has(ctx, &testEntry{row6}))
	require.ElementsMatch(t, []in_mem_table.Entry{&testEntry{row7}, &testEntry{row8}}, testTable.data.Get(testSK{15, 14}))
	require.NoError(t, testTable.data.Remove(ctx, testSK{15, 14}, nil))
	require.False(t, testTable.data.Has(ctx, &testEntry{row7}))
	require.False(t, testTable.data.Has(ctx, &testEntry{row8}))
	require.NoError(t, testTable.data.Remove(ctx, nil, &testEntry{row5}))
	require.False(t, testTable.data.Has(ctx, &testEntry{row5}))
}

type testEntry struct {
	sql.Row
}

var _ in_mem_table.Entry = (*testEntry)(nil)

func (te *testEntry) NewFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Entry, error) {
	return &testEntry{row}, nil
}
func (te *testEntry) UpdateFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Entry, error) {
	return &testEntry{row}, nil
}
func (te *testEntry) ToRow(ctx *sql.Context) sql.Row {
	return te.Row
}
func (te *testEntry) Equals(ctx *sql.Context, otherEntry in_mem_table.Entry) bool {
	otherRow, ok := otherEntry.(*testEntry)
	if !ok {
		return false
	}
	if len(te.Row) != len(otherRow.Row) {
		return false
	}
	for i := range te.Row {
		if te.Row[i] != otherRow.Row[i] {
			return false
		}
	}
	return true
}

type testPK struct {
	val int64
}

var _ in_mem_table.Key = testPK{}

func (tpk testPK) KeyFromEntry(ctx *sql.Context, entry in_mem_table.Entry) (in_mem_table.Key, error) {
	return tpk.KeyFromRow(ctx, entry.(*testEntry).Row)
}
func (tpk testPK) KeyFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Key, error) {
	if len(row) != 3 {
		return tpk, fmt.Errorf("wrong number of values")
	}
	val, ok := row[0].(int64)
	if !ok {
		return tpk, fmt.Errorf("value is not int64")
	}
	return testPK{val}, nil
}

type testSK struct {
	val2 int64
	val1 int64
}

var _ in_mem_table.Key = testSK{}

func (tsk testSK) KeyFromEntry(ctx *sql.Context, entry in_mem_table.Entry) (in_mem_table.Key, error) {
	return tsk.KeyFromRow(ctx, entry.(*testEntry).Row)
}
func (tsk testSK) KeyFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Key, error) {
	if len(row) != 3 {
		return tsk, fmt.Errorf("wrong number of values")
	}
	val2, ok := row[2].(int64)
	if !ok {
		return tsk, fmt.Errorf("value is not int64")
	}
	val1, ok := row[1].(int64)
	if !ok {
		return tsk, fmt.Errorf("value is not int64")
	}
	return testSK{val2, val1}, nil
}
