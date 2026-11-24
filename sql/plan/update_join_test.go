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

package plan

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

// closeRecordingUpdater records whether it was closed, and fails to close with |closeErr|.
type closeRecordingUpdater struct {
	sql.RowUpdater
	closeErr error
	closed   bool
}

func (u *closeRecordingUpdater) Close(*sql.Context) error {
	u.closed = true
	return u.closeErr
}

// TestUpdatableJoinUpdaterCloseClosesAll checks that every updater is closed even when others fail to close.
func TestUpdatableJoinUpdaterCloseClosesAll(t *testing.T) {
	ctx := sql.NewEmptyContext()
	errA, errC := errors.New("a"), errors.New("c")
	a := &closeRecordingUpdater{closeErr: errA}
	b := &closeRecordingUpdater{}
	c := &closeRecordingUpdater{closeErr: errC}
	u := &updatableJoinUpdater{updaterMap: map[string]sql.RowUpdater{"a": a, "b": b, "c": c}}

	err := u.Close(ctx)
	require.True(t, a.closed)
	require.True(t, b.closed)
	require.True(t, c.closed)
	require.ErrorIs(t, err, errA)
	require.ErrorIs(t, err, errC)

	// A single failure is returned as is, so that its kind can still be matched.
	b.closed = false
	u = &updatableJoinUpdater{updaterMap: map[string]sql.RowUpdater{"a": a, "b": b}}
	require.Same(t, errA, u.Close(ctx))
	require.True(t, b.closed)
}
