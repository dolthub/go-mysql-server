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

	"github.com/dolthub/go-mysql-server/sql"
)

func TestToRows(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		set := NewIndexedSet(ueq, keyers)
		rows, err := ToRows[*user](nil, &userValueOps, set)
		require.NoError(t, err)
		require.Len(t, rows, 0)
	})
	t.Run("Some", func(t *testing.T) {
		set := NewIndexedSet(ueq, keyers)
		set.Put(&user{"aaron", "aaron@dolthub.com", 0})
		set.Put(&user{"brian", "brian@dolthub.com", 0})
		rows, err := ToRows[*user](nil, &userValueOps, set)
		require.NoError(t, err)
		require.Len(t, rows, 2)
		require.Len(t, rows[0], 2)
	})
}

func TestMultiToRows(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		set := NewIndexedSet(ueq, keyers)
		rows, err := MultiToRows[*user](nil, &userPetsMultiValueOps, set)
		require.NoError(t, err)
		require.Len(t, rows, 0)
	})
	t.Run("Some", func(t *testing.T) {
		set := NewIndexedSet(ueq, keyers)
		set.Put(&user{"aaron", "aaron@dolthub.com", userPetDog | userPetCat})
		set.Put(&user{"brian", "brian@dolthub.com", userPetDog})
		set.Put(&user{"tim", "tim@dolthub.com", 0})
		rows, err := MultiToRows[*user](nil, &userPetsMultiValueOps, set)
		require.NoError(t, err)
		require.Len(t, rows, 3)
		require.Len(t, rows[0], 3)
		require.Contains(t, rows, sql.UntypedSqlRow{"aaron", "aaron@dolthub.com", "dog"})
		require.Contains(t, rows, sql.UntypedSqlRow{"aaron", "aaron@dolthub.com", "cat"})
		require.Contains(t, rows, sql.UntypedSqlRow{"brian", "brian@dolthub.com", "dog"})
	})
}
