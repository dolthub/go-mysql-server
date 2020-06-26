package function

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

const (
	alreadyLocked = "already_locked"
	unlocked = "unlocked"
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
	isFreeLock := NewNamedLockFunc(ls, "is_free_lock", sql.Int8, IsFreeLockFunc)
	tf := NewTestFactory(isFreeLock.Fn)

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
	isUsed := NewNamedLockFunc(ls, "is_used_lock", sql.Uint32, IsUsedLockFunc)
	tf := NewTestFactory(isUsed.Fn)

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
	releaseLock := NewNamedLockFunc(ls, "release_lock", sql.Int8, ReleaseLockFunc)
	tf := NewTestFactory(releaseLock.Fn)

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

	_, err = ReleaseAllLocksForLS(ls)(user0, nil)
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