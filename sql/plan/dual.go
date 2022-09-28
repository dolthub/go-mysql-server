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
	"github.com/dolthub/go-mysql-server/sql"
	"strings"

	"github.com/dolthub/go-mysql-server/memory"
)

const DualTableName = ""

var DualTableSchema = sql.NewPrimaryKeySchema(sql.Schema{
	{Name: "dummy", Source: DualTableName, Type: sql.LongText, Nullable: false},
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
