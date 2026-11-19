// Copyright 2026 Dolthub, Inc.
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
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// recordingUpdater is a RowUpdater that records the rows it is given.
type recordingUpdater struct {
	updated [][2]sql.Row
}

func (u *recordingUpdater) StatementBegin(*sql.Context)              {}
func (u *recordingUpdater) DiscardChanges(*sql.Context, error) error { return nil }
func (u *recordingUpdater) StatementComplete(*sql.Context) error     { return nil }
func (u *recordingUpdater) Close(*sql.Context) error                 { return nil }
func (u *recordingUpdater) Update(_ *sql.Context, old, new sql.Row) error {
	u.updated = append(u.updated, [2]sql.Row{old, new})
	return nil
}

// recordingUnchangedUpdater is a recordingUpdater that also wants the rows an UPDATE left unchanged.
type recordingUnchangedUpdater struct {
	recordingUpdater
	unchanged []sql.Row
}

var _ sql.UnchangedRowUpdater = (*recordingUnchangedUpdater)(nil)

func (u *recordingUnchangedUpdater) UpdateUnchanged(_ *sql.Context, row sql.Row) error {
	u.unchanged = append(u.unchanged, row)
	return nil
}

func TestUpdateIterUnchangedRows(t *testing.T) {
	schema := sql.Schema{
		{Name: "pk", Type: types.Int64, Source: "t", PrimaryKey: true},
		{Name: "v", Type: types.Int64, Source: "t"},
	}
	// Each row from an update's source is the old row followed by the new one.
	source := []sql.Row{
		{int64(1), int64(10), int64(1), int64(11)},
		{int64(2), int64(20), int64(2), int64(20)},
		{int64(3), int64(30), int64(3), int64(30)},
	}
	ctx := sql.NewContext(context.Background())

	drain := func(updater sql.RowUpdater) {
		iter := &updateIter{
			childIter: sql.RowsToRowIter(source...),
			schema:    schema,
			updater:   updater,
		}
		for {
			_, err := iter.Next(ctx)
			if err == io.EOF {
				break
			}

			require.NoError(t, err)
		}
	}

	t.Run("an updater is not given unchanged rows unless it asks for them", func(t *testing.T) {
		updater := &recordingUpdater{}
		drain(updater)
		require.Equal(t, [][2]sql.Row{{{int64(1), int64(10)}, {int64(1), int64(11)}}}, updater.updated)
	})

	t.Run("an updater that asks for unchanged rows is given them", func(t *testing.T) {
		updater := &recordingUnchangedUpdater{}
		drain(updater)
		require.Equal(t, [][2]sql.Row{{{int64(1), int64(10)}, {int64(1), int64(11)}}}, updater.updated)
		require.Equal(t, []sql.Row{{int64(2), int64(20)}, {int64(3), int64(30)}}, updater.unchanged)
	})
}
