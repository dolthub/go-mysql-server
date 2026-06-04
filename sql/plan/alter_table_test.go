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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TestAlterTableCommentSchema verifies that AlterTableComment reports the OkResult
// schema. The node produces a single OkResult row, and renderers that format rows
// against the declared schema fail when the schema describes the table columns
// instead.
// See https://github.com/dolthub/dolt/issues/11164
func TestAlterTableCommentSchema(t *testing.T) {
	db := memory.NewDatabase("mydb")
	// The table schema is intentionally not the OkResult schema so the test fails if
	// Schema returns the table columns again.
	tbl := memory.NewTable(nil, db, "t", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int32, Source: "t", PrimaryKey: true},
	}), nil)

	atc := NewAlterTableComment(NewResolvedTable(tbl, db, nil), "c")
	require.Equal(t, types.OkResultSchema, atc.Schema(nil))
}
