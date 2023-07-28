// Copyright 2023 Dolthub, Inc.
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

package in_mem_table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type user struct {
	username string
	email    string
	sidecar  int
}

func ueq(l, r *user) bool {
	return l.username == r.username && l.email == r.email
}

type usernameKeyer struct{}

func (usernameKeyer) GetKey(u *user) any {
	return u.username
}

type emailKeyer struct{}

func (emailKeyer) GetKey(u *user) any {
	return u.email
}

var keyers = []Keyer[*user]{usernameKeyer{}, emailKeyer{}}

func TestIndexedSetCount(t *testing.T) {
	set := NewIndexedSet(ueq, keyers)
	require.Equal(t, 0, set.Count())
	set.Put(&user{"aaron", "aaron@dolthub.com", 0})
	set.Put(&user{"brian", "brian@dolthub.com", 0})
	require.Equal(t, 2, set.Count())
	set.Put(&user{"dustin", "dustin@dolthub.com", 0})
	require.Equal(t, 3, set.Count())
	set.Clear()
	require.Equal(t, 0, set.Count())

	// IndexedSet allows the same entry multiple times.
	set.Put(&user{"aaron", "aaron@dolthub.com", 0})
	set.Put(&user{"aaron", "aaron@dolthub.com", 0})
	require.Equal(t, 2, set.Count())
}

func TestIndexedSetGet(t *testing.T) {
	set := NewIndexedSet(ueq, keyers)
	u, ok := set.Get(&user{"aaron", "aaron@dolthub.com", 0})
	require.False(t, ok)
	require.Nil(t, u)
	set.Put(&user{"brian", "brian@dolthub.com", 0})
	u, ok = set.Get(&user{"aaron", "aaron@dolthub.com", 0})
	require.False(t, ok)
	require.Nil(t, u)
	set.Put(&user{"aaron", "aaron@dolthub.com", 0})
	u, ok = set.Get(&user{"aaron", "aaron@dolthub.com", 0})
	require.True(t, ok)
	require.NotNil(t, u)
	require.Equal(t, "aaron", u.username)
	require.Equal(t, "aaron@dolthub.com", u.email)
}

func TestIndexedSetRemove(t *testing.T) {
	set := NewIndexedSet(ueq, keyers)
	u, ok := set.Remove(&user{"aaron", "aaron@dolthub.com", 0})
	require.False(t, ok)
	require.Nil(t, u)
	set.Put(&user{"aaron", "aaron@dolthub.com", 0})
	u, ok = set.Remove(&user{"aaron", "aaron@dolthub.com", 0})
	require.True(t, ok)
	require.NotNil(t, u)
	require.Equal(t, "aaron", u.username)
	require.Equal(t, "aaron@dolthub.com", u.email)
	require.Equal(t, 0, set.Count())

	set.Put(&user{"brian", "brian@dolthub.com", 0})
	set.Put(&user{"brian", "brian@dolthub.com", 0})
	set.Put(&user{"brian", "brian+another@dolthub.com", 0})
	u, ok = set.Remove(&user{"brian", "brian@dolthub.com", 0})
	require.True(t, ok)
	require.NotNil(t, u)
	require.Equal(t, "brian", u.username)
	require.Equal(t, "brian@dolthub.com", u.email)
	require.Equal(t, 1, set.Count())

	res := set.GetMany(emailKeyer{}, "brian@dolthub.com")
	require.Len(t, res, 0)
	res = set.GetMany(emailKeyer{}, "brian+another@dolthub.com")
	require.Len(t, res, 1)
}

func TestIndexedSetRemoveMany(t *testing.T) {
	set := NewIndexedSet(ueq, keyers)
	set.Put(&user{"brian", "brian@dolthub.com", 0})
	set.Put(&user{"brian", "brian@dolthub.com", 0})
	set.Put(&user{"brian", "brian+another@dolthub.com", 0})

	set.RemoveMany(usernameKeyer{}, "brian")
	require.Equal(t, 0, set.Count())

	set.Put(&user{"brian", "brian@dolthub.com", 0})
	set.Put(&user{"brian", "brian@dolthub.com", 0})
	set.Put(&user{"brian", "brian+another@dolthub.com", 0})
	set.Put(&user{"aaron", "aaron@dolthub.com", 0})
	set.RemoveMany(emailKeyer{}, "brian@dolthub.com")
	require.Equal(t, 2, set.Count())
	res := set.GetMany(emailKeyer{}, "brian+another@dolthub.com")
	require.Len(t, res, 1)
	res = set.GetMany(usernameKeyer{}, "aaron")
	require.Len(t, res, 1)
}
