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

package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// TestForeignKeyCacheGetEditor verifies that GetEditor only returns a cached editor for a table whose references have
// the same foreign keys as the given editor's. A table reached through different foreign keys has different
// references, since those already enforced by the chain of foreign keys that reached it are left out.
func TestForeignKeyCacheGetEditor(t *testing.T) {
	newEditor := func(fkNames ...string) *plan.ForeignKeyEditor {
		editor := &plan.ForeignKeyEditor{}
		for _, name := range fkNames {
			editor.References = append(editor.References, &plan.ForeignKeyReferenceHandler{
				ForeignKey: sql.ForeignKeyConstraint{Name: name},
			})
		}
		return editor
	}

	cache := newForeignKeyCache()
	noReferences := newEditor()
	cache.AddEditor(noReferences, "mydb", "", "child")
	viaFkA := newEditor("fk_b", "fk_c")
	cache.AddEditor(viaFkA, "mydb", "", "child")

	require.Same(t, noReferences, cache.GetEditor(nil, "mydb", "", "CHILD"))
	require.Same(t, viaFkA, cache.GetEditor(newEditor("fk_b", "fk_c"), "mydb", "", "child"))
	require.Nil(t, cache.GetEditor(newEditor("fk_a", "fk_c"), "mydb", "", "child"))
	require.Nil(t, cache.GetEditor(newEditor("fk_b", "fk_a"), "mydb", "", "child"))
	require.Nil(t, cache.GetEditor(newEditor("fk_b"), "mydb", "", "child"))
	require.Nil(t, cache.GetEditor(newEditor("fk_b", "fk_c"), "mydb", "", "other"))
}
