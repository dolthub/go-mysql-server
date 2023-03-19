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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gabereiser/go-mysql-server/sql"
)

const (
	alreadyLocked = "already_locked"
	unlocked      = "unlocked"
)

func TestGetLock(t *testing.T) {
	ls := sql.NewLockSubsystem()
	var fn sql.CreateFunc2Args = CreateNewGetLock(ls)
	tf := NewTestFactory(fn)

	user0 := sql.NewEmptyContext()
	err := ls.Lock(user0, alreadyLocked, 0)
	require.NoError(t, err)
	err = ls.Lock(user0, unlocked, 0)
	require.NoError(t, err)
	err = ls.Unlock(user0, unlocked)
	require.NoError(t, err)

	user1 := sql.NewEmptyContext()
	tf.AddSucceeding(nil, nil, nil)
	tf.AddSucceeding(nil, "nil_param_test", nil)
	tf.AddSucceeding(nil, nil, 0)
	tf.AddSucceeding(int8(1), "new_lock", 0)
	tf.AddSucceeding(int8(1), unlocked, 0)
	tf.AddSucceeding(int8(0), alreadyLocked, 0)
	tf.AddFailing(0, 0)
	tf.Test(t, user1, nil)
}

func TestLockIsFree(t *testing.T) {
	ls := sql.NewLockSubsystem()
	isFreeLock := NewIsFreeLock(ls)
	tf := NewTestFactory(isFreeLock)

	user0 := sql.NewEmptyContext()
	err := ls.Lock(user0, alreadyLocked, 0)
	require.NoError(t, err)
	err = ls.Lock(user0, unlocked, 0)
	require.NoError(t, err)
	err = ls.Unlock(user0, unlocked)
	require.NoError(t, err)

	user1 := sql.NewEmptyContext()
	tf.AddSucceeding(nil, nil)
	tf.AddSucceeding(int8(1), "new_lock")
	tf.AddSucceeding(int8(1), unlocked)
	tf.AddSucceeding(int8(0), alreadyLocked)
	tf.AddFailing(0)
	tf.Test(t, user1, nil)
}

func TestLockIsUsed(t *testing.T) {
	ls := sql.NewLockSubsystem()
	isUsed := NewIsUsedLock(ls)
	tf := NewTestFactory(isUsed)

	user0 := sql.NewEmptyContext()
	err := ls.Lock(user0, alreadyLocked, 0)
	require.NoError(t, err)
	err = ls.Lock(user0, unlocked, 0)
	require.NoError(t, err)
	err = ls.Unlock(user0, unlocked)
	require.NoError(t, err)

	user1 := sql.NewEmptyContext()
	tf.AddSucceeding(nil, nil)
	tf.AddSucceeding(nil, "new_lock")
	tf.AddSucceeding(nil, unlocked)
	tf.AddSucceeding(user0.ID(), alreadyLocked)
	tf.AddFailing(0)
	tf.Test(t, user1, nil)
}

func TestReleaseLock(t *testing.T) {
	ls := sql.NewLockSubsystem()
	releaseLock := NewReleaseLock(ls)
	tf := NewTestFactory(releaseLock)

	user0 := sql.NewEmptyContext()
	err := ls.Lock(user0, alreadyLocked, 0)
	require.NoError(t, err)
	err = ls.Lock(user0, unlocked, 0)
	require.NoError(t, err)
	err = ls.Unlock(user0, unlocked)
	require.NoError(t, err)

	tf.AddSucceeding(nil, nil)
	tf.AddSucceeding(int8(1), alreadyLocked)
	tf.AddSucceeding(int8(0), unlocked)
	tf.AddSucceeding(nil, "doesnt_exist")
	tf.AddFailing(0)
	tf.Test(t, user0, nil)
}

func TestReleaseAllLocks(t *testing.T) {
	ls := sql.NewLockSubsystem()

	user0 := sql.NewEmptyContext()
	err := ls.Lock(user0, "lock0", 0)
	require.NoError(t, err)
	err = ls.Lock(user0, "lock1", 0)
	require.NoError(t, err)
	err = ls.Lock(user0, "lock2", 0)
	require.NoError(t, err)
	err = ls.Lock(user0, "lock2", 0)
	require.NoError(t, err)
	err = ls.Lock(user0, "lock2", 0)
	require.NoError(t, err)

	count := 0
	err = user0.IterLocks(func(name string) error {
		count++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	state, owner := ls.GetLockState("lock0")
	assert.Equal(t, sql.LockInUse, state)
	assert.Equal(t, user0.ID(), owner)
	ls.GetLockState("lock1")
	assert.Equal(t, sql.LockInUse, state)
	assert.Equal(t, user0.ID(), owner)
	ls.GetLockState("lock2")
	assert.Equal(t, sql.LockInUse, state)
	assert.Equal(t, user0.ID(), owner)

	_, err = releaseAllLocksForLS(ls)(user0, nil)
	require.NoError(t, err)

	count = 0
	err = user0.IterLocks(func(name string) error {
		count++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	state, owner = ls.GetLockState("lock0")
	assert.Equal(t, sql.LockFree, state)
	assert.Equal(t, uint32(0), owner)
	ls.GetLockState("lock1")
	assert.Equal(t, sql.LockFree, state)
	assert.Equal(t, uint32(0), owner)
	ls.GetLockState("lock2")
	assert.Equal(t, sql.LockFree, state)
	assert.Equal(t, uint32(0), owner)
}

// releaseAllLocksForLS returns the logic to execute when the sql function release_all_locks is executed
func releaseAllLocksForLS(ls *sql.LockSubsystem) func(*sql.Context, sql.Row) (interface{}, error) {
	return func(ctx *sql.Context, _ sql.Row) (interface{}, error) {
		return ls.ReleaseAll(ctx)
	}
}
