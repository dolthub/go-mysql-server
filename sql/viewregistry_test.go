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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	dbName   = "db"
	viewName = "myview"
	mockView = NewView(viewName, nil, "")
)

func newMockRegistry(require *require.Assertions) *ViewRegistry {
	registry := NewViewRegistry()

	err := registry.Register(dbName, mockView)
	require.NoError(err)
	require.Equal(1, len(registry.AllViews()))

	return registry
}

// Tests the creation of an empty ViewRegistry with no views registered.
func TestNewViewRegistry(t *testing.T) {
	require := require.New(t)

	registry := NewViewRegistry()
	require.Equal(0, len(registry.AllViews()))
}

// Tests that registering a non-existing view succeeds.
func TestRegisterNonExistingView(t *testing.T) {
	require := require.New(t)

	registry := newMockRegistry(require)

	actualView, err := registry.View(dbName, viewName)
	require.NoError(err)
	require.Equal(mockView, *actualView)
}

// Tests that registering an existing view fails.
func TestRegisterExistingVIew(t *testing.T) {
	require := require.New(t)

	registry := newMockRegistry(require)

	err := registry.Register(dbName, mockView)
	require.Error(err)
	require.True(ErrExistingView.Is(err))
}

// Tests that deleting an existing view succeeds.
func TestDeleteExistingView(t *testing.T) {
	require := require.New(t)

	registry := newMockRegistry(require)

	err := registry.Delete(dbName, viewName)
	require.NoError(err)
	require.Equal(0, len(registry.AllViews()))
}

// Tests that deleting a non-existing view fails.
func TestDeleteNonExistingView(t *testing.T) {
	require := require.New(t)

	registry := NewViewRegistry()

	err := registry.Delete("random", "randomer")
	require.Error(err)
	require.True(ErrNonExistingView.Is(err))
}

// Tests that retrieving an existing view succeeds and that the view returned
// is the correct one.
func TestGetExistingView(t *testing.T) {
	require := require.New(t)

	registry := newMockRegistry(require)

	actualView, err := registry.View(dbName, viewName)
	require.NoError(err)
	require.Equal(mockView, *actualView)
}

// Tests that retrieving a non-existing view fails.
func TestGetNonExistingView(t *testing.T) {
	require := require.New(t)

	registry := NewViewRegistry()

	actualView, err := registry.View(dbName, viewName)
	require.Error(err)
	require.Nil(actualView)
	require.True(ErrNonExistingView.Is(err))
}

// Tests that retrieving the views registered under a database succeeds,
// returning the list of all the correct views.
func TestViewsInDatabase(t *testing.T) {
	require := require.New(t)

	registry := NewViewRegistry()

	databases := []struct {
		name     string
		numViews int
	}{
		{"db0", 0},
		{"db1", 5},
		{"db2", 10},
	}

	for _, db := range databases {
		for i := 0; i < db.numViews; i++ {
			view := NewView(viewName+strconv.Itoa(i), nil, "")
			err := registry.Register(db.name, view)
			require.NoError(err)
		}

		views := registry.ViewsInDatabase(db.name)
		require.Equal(db.numViews, len(views))
	}
}

var viewKeys = []ViewKey{
	{
		"db1",
		"view1",
	},
	{
		"db1",
		"view2",
	},
	{
		"db2",
		"view1",
	},
}

func registerKeys(registry *ViewRegistry, require *require.Assertions) {
	for _, key := range viewKeys {
		err := registry.Register(key.dbName, NewView(key.viewName, nil, ""))
		require.NoError(err)
	}
	require.Equal(len(viewKeys), len(registry.AllViews()))
}

func TestDeleteExistingList(t *testing.T) {
	require := require.New(t)

	test := func(errIfNotExists bool) {
		registry := NewViewRegistry()

		registerKeys(registry, require)
		err := registry.DeleteList(viewKeys, errIfNotExists)
		require.NoError(err)
		require.Equal(0, len(registry.AllViews()))
	}

	test(true)
	test(false)
}

func TestDeleteNonExistingList(t *testing.T) {
	require := require.New(t)

	test := func(errIfNotExists bool) {
		registry := NewViewRegistry()

		registerKeys(registry, require)
		err := registry.DeleteList([]ViewKey{{"random", "random"}}, errIfNotExists)
		if errIfNotExists {
			require.Error(err)
		} else {
			require.NoError(err)
		}
		require.Equal(len(viewKeys), len(registry.AllViews()))
	}

	test(false)
	test(true)
}

func TestDeletePartiallyExistingList(t *testing.T) {
	require := require.New(t)

	test := func(errIfNotExists bool) {
		registry := NewViewRegistry()

		registerKeys(registry, require)
		toDelete := append(viewKeys, ViewKey{"random", "random"})
		err := registry.DeleteList(toDelete, errIfNotExists)
		if errIfNotExists {
			require.Error(err)
			require.Equal(len(viewKeys), len(registry.AllViews()))
		} else {
			require.NoError(err)
			require.Equal(0, len(registry.AllViews()))
		}
	}

	test(false)
	test(true)
}

func TestExistsOnExistingView(t *testing.T) {
	require := require.New(t)

	registry := newMockRegistry(require)

	require.True(registry.Exists(dbName, viewName))
}

func TestExistsOnNonExistingView(t *testing.T) {
	require := require.New(t)

	registry := newMockRegistry(require)

	require.False(registry.Exists("non", "existing"))
}
