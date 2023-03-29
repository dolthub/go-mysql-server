// Copyright 2022 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/go-mysql-server/memory"
)

// DualTableName is empty string because no table with empty name can be created
const DualTableName = ""

// DualTableSchema has a single column with empty name because no table can be created with empty string column name or
// no alias name can be empty string. This avoids any alias name to be considered as GetField of dual table.
var DualTableSchema = sql.NewPrimaryKeySchema(sql.Schema{
	{Name: "", Source: DualTableName, Type: types.LongText, Nullable: false},
})

// IsDualTable returns whether the given table is the "dual" table.
func IsDualTable(t sql.Table) bool {
	if t == nil {
		return false
	}
	return strings.ToLower(t.Name()) == DualTableName && t.Schema().Equals(DualTableSchema.Schema)
}

var dualTable = func() sql.Table {
	t := memory.NewTable(DualTableName, DualTableSchema, nil)

	ctx := sql.NewEmptyContext()

	// Need to run through the proper inserting steps to add data to the dummy table.
	inserter := t.Inserter(ctx)
	inserter.StatementBegin(ctx)
	_ = inserter.Insert(sql.NewEmptyContext(), sql.NewRow("x"))
	_ = inserter.StatementComplete(ctx)
	_ = inserter.Close(ctx)
	return t
}()

// NewDualSqlTable creates a new Dual table.
func NewDualSqlTable() sql.Table {
	return dualTable
}
