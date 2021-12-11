// Copyright 2021 Dolthub, Inc.
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
	"context"
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBackgroundThreads(t *testing.T) {
	var bThreads *BackgroundThreads
	var err error

	var b []int
	mu := &sync.Mutex{}
	f := func(i int) func(ctx context.Context) {
		return func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					mu.Lock()
					defer mu.Unlock()
					b = append(b, i)
					return
				}
			}
		}
	}

	t.Run("add, close", func(t *testing.T) {
		b = make([]int, 0)
		bThreads = NewBackgroundThreads()
		defer bThreads.Shutdown()

		err = bThreads.Add("first", f(1))
		assert.NoError(t, err)

		err = bThreads.Add("second", f(2))
		assert.NoError(t, err)

		// wait until close to flush
		assert.Equal(t, []int{}, b)

		err = bThreads.Shutdown()
		assert.True(t, errors.Is(err, context.Canceled))

		sort.Ints(b)
		assert.Equal(t, []int{1, 2}, b)
	})

	t.Run("close is idempotent", func(t *testing.T) {
		b = make([]int, 0)
		bThreads = NewBackgroundThreads()
		defer bThreads.Shutdown()

		err = bThreads.Add("first", f(1))
		assert.NoError(t, err)

		err = bThreads.Shutdown()
		assert.True(t, errors.Is(err, context.Canceled))
		err = bThreads.Shutdown()
		assert.True(t, errors.Is(err, context.Canceled))

		sort.Ints(b)
		assert.Equal(t, []int{1}, b)
	})

	t.Run("can't add after closed", func(t *testing.T) {
		b = make([]int, 0)
		bThreads = NewBackgroundThreads()
		defer bThreads.Shutdown()

		err = bThreads.Shutdown()
		assert.True(t, errors.Is(err, context.Canceled))

		err = bThreads.Add("first", f(1))
		assert.True(t, errors.Is(err, ErrCannotAddToClosedBackgroundThreads))

		sort.Ints(b)
		assert.Equal(t, []int{}, b)
	})
}
