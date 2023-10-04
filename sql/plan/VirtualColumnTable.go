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

package plan

import "github.com/dolthub/go-mysql-server/sql"

// VirtualColumnTable is a sql.TableNode that combines a ResolvedTable with a Project, the latter of which is used 
// to add the values of virtual columns to the table.
type VirtualColumnTable struct {
	*ResolvedTable
	projections []sql.Expression
}

var _ sql.TableNode = (*VirtualColumnTable)(nil)

// NewVirtualColumnTable creates a new VirtualColumnTable.
func NewVirtualColumnTable(table *ResolvedTable, projections []sql.Expression) *VirtualColumnTable {
	return &VirtualColumnTable{ResolvedTable: table, projections: projections}
}
