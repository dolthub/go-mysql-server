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

package sql

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const testLockName = "my_lock"

type lockDiffs struct {
	missing []string
	extra   []string
}

func getLockDiffs(ctx *Context, testLockNames ...string) *lockDiffs {
	var missing []string
	var extra []string

	locks := make(map[string]bool)
	for _, name := range testLockNames {
		locks[name] = true
	}

	userLocks := make(map[string]bool)
	_ = ctx.IterLocks(func(name string) error {
		userLocks[name] = true

		if !locks[name] {
			extra = append(extra, name)
		}
		return nil
	})

	for name := range locks {
		if !userLocks[name] {
			missing = append(missing, name)
		}
	}

	if len(missing) == 0 && len(extra) == 0 {
		return nil
	}

	return &lockDiffs{missing: missing, extra: extra}
}

func TestLock(t *testing.T) {
	ls := NewLockSubsystem()
	user1Ctx := NewEmptyContext()
	user2Ctx := NewEmptyContext()

	err := ls.Lock(user1Ctx, testLockName, 0)
	assert.NoError(t, err)
	assert.Nil(t, getLockDiffs(user1Ctx, testLockName))

	err = ls.Lock(user1Ctx, testLockName, 0)
	assert.NoError(t, err)
	assert.Nil(t, getLockDiffs(user1Ctx, testLockName))

	err = ls.Lock(user2Ctx, testLockName, 0)
	assert.Error(t, err)
	assert.Nil(t, getLockDiffs(user2Ctx))

	err = ls.Unlock(user1Ctx, testLockName)
	assert.NoError(t, err)
	assert.Nil(t, getLockDiffs(user1Ctx, testLockName))

	err = ls.Lock(user2Ctx, testLockName, 0)
	assert.Error(t, err)
	assert.Nil(t, getLockDiffs(user2Ctx))

	err = ls.Unlock(user1Ctx, testLockName)
	assert.NoError(t, err)
	assert.Nil(t, getLockDiffs(user1Ctx))

	err = ls.Lock(user1Ctx, testLockName, 0)
	assert.NoError(t, err)
	assert.Nil(t, getLockDiffs(user1Ctx, testLockName))

	err = ls.Lock(user2Ctx, testLockName, 0)
	assert.Error(t, err)
	assert.Nil(t, getLockDiffs(user2Ctx))
}

func TestRace(t *testing.T) {
	const numGoRoutines = 8

	ls := NewLockSubsystem()
	wg := &sync.WaitGroup{}
	for i := 0; i < numGoRoutines; i++ {
		wg.Add(1)
		go func(ctx *Context) {
			defer wg.Done()

			err := ls.Lock(ctx, testLockName, -1)
			assert.NoError(t, err)
			assert.Nil(t, getLockDiffs(ctx, testLockName))

			defer func() {
				err := ls.Unlock(ctx, testLockName)
				assert.NoError(t, err)
				assert.Nil(t, getLockDiffs(ctx))
			}()

			err = ls.Lock(ctx, testLockName, -1)
			assert.NoError(t, err)
			assert.Nil(t, getLockDiffs(ctx, testLockName))

			defer func() {
				err := ls.Unlock(ctx, testLockName)
				assert.NoError(t, err)
				assert.Nil(t, getLockDiffs(ctx, testLockName))
			}()

			time.Sleep(time.Duration(rand.Int63n(int64(time.Millisecond))))
		}(NewEmptyContext())
	}

	wg.Wait()
}

func TestTimeout(t *testing.T) {
	ls := NewLockSubsystem()
	user1 := NewEmptyContext()
	user2 := NewEmptyContext()

	err := ls.Lock(user1, testLockName, 0)
	assert.NoError(t, err)
	assert.Nil(t, getLockDiffs(user1, testLockName))

	err = ls.Lock(user2, testLockName, time.Millisecond)
	assert.True(t, ErrLockTimeout.Is(err))
	assert.Nil(t, getLockDiffs(user2))
}

func TestErrLockNotOwned(t *testing.T) {
	user1 := NewEmptyContext()
	user2 := NewEmptyContext()
	ls := NewLockSubsystem()

	err := ls.Lock(user1, testLockName, 0)
	assert.NoError(t, err)
	assert.Nil(t, getLockDiffs(user1, testLockName))
	assert.Nil(t, getLockDiffs(user2))

	err = ls.Unlock(user2, testLockName)
	assert.True(t, ErrLockNotOwned.Is(err))
	assert.Nil(t, getLockDiffs(user1, testLockName))
	assert.Nil(t, getLockDiffs(user2))
}

func TestGetLockState(t *testing.T) {
	user1 := NewEmptyContext()
	ls := NewLockSubsystem()

	state, owner := ls.GetLockState(testLockName)
	assert.Equal(t, LockDoesNotExist, state)
	assert.Equal(t, uint32(0), owner)

	err := ls.Lock(user1, testLockName, 0)
	assert.NoError(t, err)
	state, owner = ls.GetLockState(testLockName)
	assert.Equal(t, LockInUse, state)
	assert.Equal(t, user1.Session.ID(), owner)

	err = ls.Unlock(user1, testLockName)
	assert.NoError(t, err)
	state, owner = ls.GetLockState(testLockName)
	assert.Equal(t, LockFree, state)
	assert.Equal(t, uint32(0), owner)
}
