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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

var userValueOps = ValueOps[*user]{
	ToRow: func(ctx *sql.Context, u *user) (sql.Row, error) {
		return sql.Row{u.username, u.email}, nil
	},
	FromRow: func(ctx *sql.Context, r sql.Row) (*user, error) {
		if len(r) != 2 {
			return nil, errors.New("invalid schema for user insert")
		}
		username, ok := r[0].(string)
		if !ok {
			return nil, errors.New("invalid schema for user insert")
		}
		email, ok := r[1].(string)
		if !ok {
			return nil, errors.New("invalid schema for user insert")
		}
		return &user{username, email, 0}, nil
	},
	UpdateWithRow: func(ctx *sql.Context, r sql.Row, u *user) (*user, error) {
		if len(r) != 2 {
			return nil, errors.New("invalid schema for user insert")
		}
		username, ok := r[0].(string)
		if !ok {
			return nil, errors.New("invalid schema for user insert")
		}
		email, ok := r[1].(string)
		if !ok {
			return nil, errors.New("invalid schema for user insert")
		}
		uu := *u
		uu.username = username
		uu.email = email
		return &uu, nil
	},
}

func TestTableEditorInsert(t *testing.T) {
	t.Run("InsertRow", func(t *testing.T) {
		set := NewIndexedSet(ueq, keyers)
		ed := &IndexedSetTableEditor[*user]{set, userValueOps}
		ed.StatementBegin(nil)
		require.NoError(t, ed.Insert(nil, sql.Row{"aaron", "aaron@dolthub.com"}))
		require.NoError(t, ed.StatementComplete(nil))
		require.Equal(t, 1, set.Count())
		require.Len(t, set.GetMany(usernameKeyer{}, "aaron"), 1)
	})
	t.Run("InsertDuplicateRow", func(t *testing.T) {
		set := NewIndexedSet(ueq, keyers)
		ed := &IndexedSetTableEditor[*user]{set, userValueOps}
		ed.StatementBegin(nil)
		require.NoError(t, ed.Insert(nil, sql.Row{"aaron", "aaron@dolthub.com"}))
		require.Error(t, ed.Insert(nil, sql.Row{"aaron", "aaron@dolthub.com"}))
	})
	t.Run("InsertBadSchema", func(t *testing.T) {
		set := NewIndexedSet(ueq, keyers)
		ed := &IndexedSetTableEditor[*user]{set, userValueOps}
		ed.StatementBegin(nil)
		require.Error(t, ed.Insert(nil, sql.Row{"aaron", "aaron@dolthub.com", "extra value"}))
		require.Error(t, ed.Insert(nil, sql.Row{123, "aaron@dolthub.com"}))
		require.Error(t, ed.Insert(nil, sql.Row{"aaron", 123}))
	})
}

func TestTableEditorDelete(t *testing.T) {
	t.Run("DeleteRow", func(t *testing.T) {
		set := NewIndexedSet(ueq, keyers)
		set.Put(&user{"aaron", "aaron@dolthub.com", 0})
		set.Put(&user{"brian", "brian@dolthub.com", 0})
		ed := &IndexedSetTableEditor[*user]{set, userValueOps}
		ed.StatementBegin(nil)
		require.NoError(t, ed.Delete(nil, sql.Row{"aaron", "aaron@dolthub.com"}))
		require.NoError(t, ed.StatementComplete(nil))
		require.Equal(t, 1, set.Count())
		require.Len(t, set.GetMany(usernameKeyer{}, "brian"), 1)
	})
	t.Run("NoMatch", func(t *testing.T) {
		set := NewIndexedSet(ueq, keyers)
		set.Put(&user{"aaron", "aaron@dolthub.com", 0})
		set.Put(&user{"brian", "brian@dolthub.com", 0})
		ed := &IndexedSetTableEditor[*user]{set, userValueOps}
		ed.StatementBegin(nil)
		require.NoError(t, ed.Delete(nil, sql.Row{"jason", "jason@dolthub.com"}))
		require.NoError(t, ed.StatementComplete(nil))
		require.Equal(t, 2, set.Count())
		require.Len(t, set.GetMany(usernameKeyer{}, "brian"), 1)
		require.Len(t, set.GetMany(usernameKeyer{}, "aaron"), 1)
	})
	t.Run("OnlyConsidersPrimaryKey", func(t *testing.T) {
		set := NewIndexedSet(ueq, keyers)
		set.Put(&user{"aaron", "aaron@dolthub.com", 0})
		set.Put(&user{"brian", "brian@dolthub.com", 0})
		ed := &IndexedSetTableEditor[*user]{set, userValueOps}
		ed.StatementBegin(nil)
		require.NoError(t, ed.Delete(nil, sql.Row{"aaron", "aaron+alternative@dolthub.com"}))
		require.NoError(t, ed.StatementComplete(nil))
		require.Equal(t, 1, set.Count())
		require.Len(t, set.GetMany(usernameKeyer{}, "brian"), 1)
		require.Len(t, set.GetMany(usernameKeyer{}, "aaron"), 0)
	})
}

func TestTableEditorUpdate(t *testing.T) {
	t.Run("UpdateNonPrimary", func(t *testing.T) {
		set := NewIndexedSet(ueq, keyers)
		set.Put(&user{"aaron", "aaron@dolthub.com", 0})
		set.Put(&user{"brian", "brian@dolthub.com", 0})
		ed := &IndexedSetTableEditor[*user]{set, userValueOps}
		ed.StatementBegin(nil)
		require.NoError(t, ed.Update(nil, sql.Row{"aaron", "aaron@dolthub.com"}, sql.Row{"aaron", "aaron+new@dolthub.com"}))
		require.NoError(t, ed.StatementComplete(nil))
		require.Equal(t, 2, set.Count())
		require.Len(t, set.GetMany(usernameKeyer{}, "brian"), 1)
		require.Len(t, set.GetMany(usernameKeyer{}, "aaron"), 1)
		require.Len(t, set.GetMany(emailKeyer{}, "aaron+new@dolthub.com"), 1)
		require.Len(t, set.GetMany(emailKeyer{}, "aaron@dolthub.com"), 0)
	})
	t.Run("UpdatePrimary", func(t *testing.T) {
		set := NewIndexedSet(ueq, keyers)
		set.Put(&user{"aaron", "aaron@dolthub.com", 0})
		set.Put(&user{"brian", "brian@dolthub.com", 0})
		ed := &IndexedSetTableEditor[*user]{set, userValueOps}
		ed.StatementBegin(nil)
		require.NoError(t, ed.Update(nil, sql.Row{"aaron", "aaron@dolthub.com"}, sql.Row{"aaron.son", "aaron+new@dolthub.com"}))
		require.NoError(t, ed.StatementComplete(nil))
		require.Equal(t, 2, set.Count())
		require.Len(t, set.GetMany(usernameKeyer{}, "brian"), 1)
		require.Len(t, set.GetMany(usernameKeyer{}, "aaron.son"), 1)
		require.Len(t, set.GetMany(usernameKeyer{}, "aaron"), 0)
		require.Len(t, set.GetMany(emailKeyer{}, "aaron+new@dolthub.com"), 1)
		require.Len(t, set.GetMany(emailKeyer{}, "aaron@dolthub.com"), 0)
	})
	t.Run("UpdatePreserveSidecar", func(t *testing.T) {
		// Here we assert that, assuming the UpdateWithRow function is written correctly,
		// the updated entity which is in the IndexedSet has an opportunity to retain
		// data which isn't reflected in the mapping from struct <-> sql.Row.

		set := NewIndexedSet(ueq, keyers)
		set.Put(&user{"aaron", "aaron@dolthub.com", 0})
		set.Put(&user{"brian", "brian@dolthub.com", 1})
		ed := &IndexedSetTableEditor[*user]{set, userValueOps}
		ed.StatementBegin(nil)
		require.NoError(t, ed.Update(nil, sql.Row{"brian", "brian@dolthub.com"}, sql.Row{"brian", "brian+new@dolthub.com"}))
		require.NoError(t, ed.StatementComplete(nil))
		require.Equal(t, 2, set.Count())
		res := set.GetMany(usernameKeyer{}, "brian")
		require.Len(t, res, 1)
		require.Equal(t, 1, res[0].sidecar)
	})
}

const (
	userPetDog  = 1
	userPetCat  = 1 << 1
	userPetFish = 1 << 2
)

var userPetsMultiValueOps = MultiValueOps[*user]{
	ToRows: func(ctx *sql.Context, u *user) ([]sql.Row, error) {
		var res []sql.Row
		if (u.sidecar & userPetDog) == userPetDog {
			res = append(res, sql.Row{u.username, u.email, "dog"})
		}
		if (u.sidecar & userPetCat) == userPetCat {
			res = append(res, sql.Row{u.username, u.email, "cat"})
		}
		if (u.sidecar & userPetFish) == userPetFish {
			res = append(res, sql.Row{u.username, u.email, "fish"})
		}
		return res, nil
	},
	FromRow: func(ctx *sql.Context, r sql.Row) (*user, error) {
		if len(r) != 3 {
			return nil, errors.New("invalid schema for user pet insert")
		}
		username, ok := r[0].(string)
		if !ok {
			return nil, errors.New("invalid schema for user pet insert")
		}
		email, ok := r[1].(string)
		if !ok {
			return nil, errors.New("invalid schema for user pet insert")
		}
		pet, ok := r[2].(string)
		if !ok {
			return nil, errors.New("invalid schema for user pet insert")
		}
		var sidecar int
		if pet == "dog" {
			sidecar = userPetDog
		} else if pet == "cat" {
			sidecar = userPetCat
		} else if pet == "fish" {
			sidecar = userPetFish
		} else {
			return nil, errors.New("invalid schema for user pet insert")
		}
		return &user{username, email, sidecar}, nil
	},
	AddRow: func(ctx *sql.Context, r sql.Row, u *user) (*user, error) {
		if len(r) != 3 {
			return nil, errors.New("invalid schema for user pet insert")
		}
		pet, ok := r[2].(string)
		if !ok {
			return nil, errors.New("invalid schema for user pet insert")
		}
		var sidecar int
		if pet == "dog" {
			sidecar = userPetDog
		} else if pet == "cat" {
			sidecar = userPetCat
		} else if pet == "fish" {
			sidecar = userPetFish
		} else {
			return nil, errors.New("invalid schema for user pet insert")
		}
		uu := *u
		uu.sidecar |= sidecar
		return &uu, nil
	},
	DeleteRow: func(ctx *sql.Context, r sql.Row, u *user) (*user, error) {
		if len(r) != 3 {
			return nil, errors.New("invalid schema for user pet insert")
		}
		pet, ok := r[2].(string)
		if !ok {
			return nil, errors.New("invalid schema for user pet insert")
		}
		var sidecar int
		if pet == "dog" {
			sidecar = userPetDog
		} else if pet == "cat" {
			sidecar = userPetCat
		} else if pet == "fish" {
			sidecar = userPetFish
		} else {
			return nil, errors.New("invalid schema for user pet insert")
		}
		uu := *u
		uu.sidecar &= ^sidecar
		return &uu, nil
	},
}

func TestMultiTableEditorInsert(t *testing.T) {
	t.Run("InsertRow", func(t *testing.T) {
		set := NewIndexedSet(ueq, keyers)
		set.Put(&user{"aaron", "aaron@dolthub.com", 0})
		ed := &MultiIndexedSetTableEditor[*user]{set, userPetsMultiValueOps}
		ed.StatementBegin(nil)
		require.NoError(t, ed.Insert(nil, sql.Row{"aaron", "aaron@dolthub.com", "dog"}))
		require.NoError(t, ed.Insert(nil, sql.Row{"aaron", "aaron@dolthub.com", "fish"}))
		require.NoError(t, ed.StatementComplete(nil))
		require.Equal(t, 1, set.Count())
		res := set.GetMany(usernameKeyer{}, "aaron")
		require.Len(t, res, 1)
		require.Equal(t, userPetDog|userPetFish, res[0].sidecar)
	})
}

func TestMultiTableEditorDelete(t *testing.T) {
	t.Run("DeleteRow", func(t *testing.T) {
		set := NewIndexedSet(ueq, keyers)
		set.Put(&user{"aaron", "aaron@dolthub.com", userPetDog | userPetFish})
		ed := &MultiIndexedSetTableEditor[*user]{set, userPetsMultiValueOps}
		ed.StatementBegin(nil)
		require.NoError(t, ed.Delete(nil, sql.Row{"aaron", "aaron@dolthub.com", "fish"}))
		require.NoError(t, ed.Delete(nil, sql.Row{"aaron", "aaron@dolthub.com", "cat"}))
		require.NoError(t, ed.StatementComplete(nil))
		require.Equal(t, 1, set.Count())
		res := set.GetMany(usernameKeyer{}, "aaron")
		require.Len(t, res, 1)
		require.Equal(t, userPetDog, res[0].sidecar)
	})
}

func TestMultiTableEditorUpdate(t *testing.T) {
	t.Run("UpdateRow", func(t *testing.T) {
		// This test demonstrates that the multi table editor does not edit
		// data on the entity which is not "owned" by the table.
		//
		// TODO: Investigate whether this results in non-compliant
		// behavior with regards to how the grant tables interact with
		// UPDATE statements.

		set := NewIndexedSet(ueq, keyers)
		set.Put(&user{"aaron", "aaron@dolthub.com", userPetDog | userPetFish})
		ed := &MultiIndexedSetTableEditor[*user]{set, userPetsMultiValueOps}
		ed.StatementBegin(nil)
		require.NoError(t, ed.Update(nil, sql.Row{"aaron", "aaron@dolthub.com", "dog"}, sql.Row{"aaron", "aaron+new@dolthub.com", "cat"}))
		require.NoError(t, ed.StatementComplete(nil))
		require.Equal(t, 1, set.Count())
		res := set.GetMany(usernameKeyer{}, "aaron")
		require.Len(t, res, 1)
		require.Equal(t, userPetFish|userPetCat, res[0].sidecar)

		// Here we see the email address was _not_ updated.
		require.Equal(t, "aaron@dolthub.com", res[0].email)

		// Relatedly, here we can see that we throw an error if we try
		// to update a primary key to something that does not already
		// exist.
		set = NewIndexedSet(ueq, keyers)
		set.Put(&user{"aaron", "aaron@dolthub.com", userPetDog | userPetFish})
		ed = &MultiIndexedSetTableEditor[*user]{set, userPetsMultiValueOps}
		ed.StatementBegin(nil)
		require.Error(t, ed.Update(nil, sql.Row{"aaron", "aaron@dolthub.com", "dog"}, sql.Row{"aaron.son", "aaron@dolthub.com", "cat"}))

		// And we simply update the matching entry if we try to change
		// the primary key to something that does exist.
		set = NewIndexedSet(ueq, keyers)
		set.Put(&user{"aaron", "aaron@dolthub.com", userPetDog | userPetCat})
		set.Put(&user{"brian", "brian@dolthub.com", userPetDog})
		ed = &MultiIndexedSetTableEditor[*user]{set, userPetsMultiValueOps}
		ed.StatementBegin(nil)
		require.NoError(t, ed.Update(nil, sql.Row{"aaron", "aaron@dolthub.com", "dog"}, sql.Row{"brian", "brian@dolthub.com", "cat"}))
		require.NoError(t, ed.StatementComplete(nil))
		require.Equal(t, 2, set.Count())
		res = set.GetMany(usernameKeyer{}, "aaron")
		require.Len(t, res, 1)
		require.Equal(t, userPetCat, res[0].sidecar)
		res = set.GetMany(usernameKeyer{}, "brian")
		require.Len(t, res, 1)
		require.Equal(t, userPetCat|userPetDog, res[0].sidecar)
	})
}
