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

package memory_test

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestIssue361(t *testing.T) {
	name := t.Name()
	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	t.Run("Update", func(*testing.T) {
		table := memory.NewTable(db, name, sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "json", Type: types.JSON, Nullable: false, Source: name},
		}), nil)

		old := sql.NewRow(types.JSONDocument{Val: []string{"foo", "bar"}})
		new := sql.NewRow(types.JSONDocument{Val: []string{"foo"}})

		table.Insert(ctx, old)

		up := table.Updater(ctx)
		up.Update(ctx, old, new) // does not panic
	})

	t.Run("Delete", func(*testing.T) {
		table := memory.NewTable(db, name, sql.NewPrimaryKeySchema(sql.Schema{
			{Name: "json", Type: types.JSON, Nullable: false, Source: name},
		}), nil)

		row := sql.NewRow(types.JSONDocument{Val: []string{"foo", "bar"}})

		table.Insert(ctx, row)

		up := table.Deleter(ctx)
		up.Delete(ctx, row) // does not panic
	})
}

func newContext(provider *memory.DbProvider) *sql.Context {
	return sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), provider)))
}
