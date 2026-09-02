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

package errguard_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/go-mysql-server/errguard"
)

func TestGo(t *testing.T) {
	t.Run("returns the function's error", func(t *testing.T) {
		expected := errors.New("an error")
		eg := new(errgroup.Group)
		errguard.Go(eg, func() error { return expected })
		assert.ErrorIs(t, eg.Wait(), expected)
	})

	t.Run("converts a panic into an error", func(t *testing.T) {
		eg := new(errgroup.Group)
		errguard.Go(eg, func() error { panic("boom") })
		err := eg.Wait()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "panic recovered: boom")
	})
}

func TestRecoverAndLog(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer errguard.RecoverAndLog("test goroutine")
		panic("boom")
	}()
	<-done
}
