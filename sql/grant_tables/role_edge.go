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

package grant_tables

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"
)

// RoleEdge represents a role to user mapping from the roles_edges Grant Table.
type RoleEdge struct {
	FromHost        string
	FromUser        string
	ToHost          string
	ToUser          string
	WithAdminOption bool
}

var _ in_mem_table.Entry = (*RoleEdge)(nil)

// NewFromRow implements the interface in_mem_table.Entry.
func (r *RoleEdge) NewFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Entry, error) {
	if err := roleEdgesTblSchema.CheckRow(row); err != nil {
		return nil, err
	}
	return &RoleEdge{
		FromHost:        row[roleEdgesTblColIndex_FROM_HOST].(string),
		FromUser:        row[roleEdgesTblColIndex_FROM_USER].(string),
		ToHost:          row[roleEdgesTblColIndex_TO_HOST].(string),
		ToUser:          row[roleEdgesTblColIndex_TO_USER].(string),
		WithAdminOption: row[roleEdgesTblColIndex_WITH_ADMIN_OPTION].(string) == "Y",
	}, nil
}

// UpdateFromRow implements the interface in_mem_table.Entry.
func (r *RoleEdge) UpdateFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Entry, error) {
	return r.NewFromRow(ctx, row)
}

// ToRow implements the interface in_mem_table.Entry.
func (r *RoleEdge) ToRow(ctx *sql.Context) sql.Row {
	row := make(sql.Row, len(roleEdgesTblSchema))
	row[roleEdgesTblColIndex_FROM_HOST] = r.FromHost
	row[roleEdgesTblColIndex_FROM_USER] = r.FromUser
	row[roleEdgesTblColIndex_TO_HOST] = r.ToHost
	row[roleEdgesTblColIndex_TO_USER] = r.ToUser
	if r.WithAdminOption {
		row[roleEdgesTblColIndex_WITH_ADMIN_OPTION] = "Y"
	} else {
		row[roleEdgesTblColIndex_WITH_ADMIN_OPTION] = "N"
	}
	return row
}

// Equals implements the interface in_mem_table.Entry.
func (r *RoleEdge) Equals(ctx *sql.Context, otherEntry in_mem_table.Entry) bool {
	otherRoleEdge, ok := otherEntry.(*RoleEdge)
	if !ok {
		return false
	}
	return *r == *otherRoleEdge
}
