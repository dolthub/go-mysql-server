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

package mysql_db

import (
	"fmt"

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/in_mem_table"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

const roleEdgesTblName = "role_edges"

var (
	errRoleEdgePkEntry = fmt.Errorf("the primary key for the `role_edges` table was given an unknown entry")
	errRoleEdgePkRow   = fmt.Errorf("the primary key for the `role_edges` table was given a row belonging to an unknown schema")
	errRoleEdgeFkEntry = fmt.Errorf("the `from` secondary key for the `role_edges` table was given an unknown entry")
	errRoleEdgeFkRow   = fmt.Errorf("the `from` secondary key for the `role_edges` table was given a row belonging to an unknown schema")
	errRoleEdgeTkEntry = fmt.Errorf("the `to` secondary key for the `role_edges` table was given an unknown entry")
	errRoleEdgeTkRow   = fmt.Errorf("the `to` secondary key for the `role_edges` table was given a row belonging to an unknown schema")

	roleEdgesTblSchema sql.Schema
)

// RoleEdgesPrimaryKey is a key that represents the primary key for the "role_edges" Grant Table.
type RoleEdgesPrimaryKey struct {
	FromHost string
	FromUser string
	ToHost   string
	ToUser   string
}

// RoleEdgesFromKey is a secondary key that represents the "from" columns on the "role_edges" Grant Table.
type RoleEdgesFromKey struct {
	FromHost string
	FromUser string
}

// RoleEdgesToKey is a secondary key that represents the "to" columns on the "role_edges" Grant Table.
type RoleEdgesToKey struct {
	ToHost string
	ToUser string
}

var _ in_mem_table.Key = RoleEdgesPrimaryKey{}
var _ in_mem_table.Key = RoleEdgesFromKey{}
var _ in_mem_table.Key = RoleEdgesToKey{}

// KeyFromEntry implements the interface in_mem_table.Key.
func (k RoleEdgesPrimaryKey) KeyFromEntry(ctx *sql.Context, entry in_mem_table.Entry) (in_mem_table.Key, error) {
	roleEdge, ok := entry.(*RoleEdge)
	if !ok {
		return nil, errRoleEdgePkEntry
	}
	return RoleEdgesPrimaryKey{
		FromHost: roleEdge.FromHost,
		FromUser: roleEdge.FromUser,
		ToHost:   roleEdge.ToHost,
		ToUser:   roleEdge.ToUser,
	}, nil
}

// KeyFromRow implements the interface in_mem_table.Key.
func (k RoleEdgesPrimaryKey) KeyFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Key, error) {
	if len(row) != len(roleEdgesTblSchema) {
		return k, errRoleEdgePkRow
	}
	fromHost, ok := row[roleEdgesTblColIndex_FROM_HOST].(string)
	if !ok {
		return k, errRoleEdgePkRow
	}
	fromUser, ok := row[roleEdgesTblColIndex_FROM_USER].(string)
	if !ok {
		return k, errRoleEdgePkRow
	}
	toHost, ok := row[roleEdgesTblColIndex_TO_HOST].(string)
	if !ok {
		return k, errRoleEdgePkRow
	}
	toUser, ok := row[roleEdgesTblColIndex_TO_USER].(string)
	if !ok {
		return k, errRoleEdgePkRow
	}
	return RoleEdgesPrimaryKey{
		FromHost: fromHost,
		FromUser: fromUser,
		ToHost:   toHost,
		ToUser:   toUser,
	}, nil
}

// KeyFromEntry implements the interface in_mem_table.Key.
func (k RoleEdgesFromKey) KeyFromEntry(ctx *sql.Context, entry in_mem_table.Entry) (in_mem_table.Key, error) {
	roleEdge, ok := entry.(*RoleEdge)
	if !ok {
		return nil, errRoleEdgeFkEntry
	}
	return RoleEdgesFromKey{
		FromHost: roleEdge.FromHost,
		FromUser: roleEdge.FromUser,
	}, nil
}

// KeyFromRow implements the interface in_mem_table.Key.
func (k RoleEdgesFromKey) KeyFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Key, error) {
	if len(row) != len(roleEdgesTblSchema) {
		return k, errRoleEdgeFkRow
	}
	fromHost, ok := row[roleEdgesTblColIndex_FROM_HOST].(string)
	if !ok {
		return k, errRoleEdgeFkRow
	}
	fromUser, ok := row[roleEdgesTblColIndex_FROM_USER].(string)
	if !ok {
		return k, errRoleEdgeFkRow
	}
	return RoleEdgesFromKey{
		FromHost: fromHost,
		FromUser: fromUser,
	}, nil
}

// KeyFromEntry implements the interface in_mem_table.Key.
func (k RoleEdgesToKey) KeyFromEntry(ctx *sql.Context, entry in_mem_table.Entry) (in_mem_table.Key, error) {
	roleEdge, ok := entry.(*RoleEdge)
	if !ok {
		return nil, errRoleEdgeTkEntry
	}
	return RoleEdgesToKey{
		ToHost: roleEdge.ToHost,
		ToUser: roleEdge.ToUser,
	}, nil
}

// KeyFromRow implements the interface in_mem_table.Key.
func (k RoleEdgesToKey) KeyFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Key, error) {
	if len(row) != len(roleEdgesTblSchema) {
		return k, errRoleEdgeTkRow
	}
	toHost, ok := row[roleEdgesTblColIndex_TO_HOST].(string)
	if !ok {
		return k, errRoleEdgeTkRow
	}
	toUser, ok := row[roleEdgesTblColIndex_TO_USER].(string)
	if !ok {
		return k, errRoleEdgeTkRow
	}
	return RoleEdgesToKey{
		ToHost: toHost,
		ToUser: toUser,
	}, nil
}

// init creates the schema for the "role_edges" Grant Table.
func init() {
	// Types
	char32_utf8_bin := types.MustCreateString(sqltypes.Char, 32, sql.Collation_utf8_bin)
	char255_ascii_general_ci := types.MustCreateString(sqltypes.Char, 255, sql.Collation_ascii_general_ci)
	enum_N_Y_utf8_general_ci := types.MustCreateEnumType([]string{"N", "Y"}, sql.Collation_utf8_general_ci)

	// Column Templates
	char32_utf8_bin_not_null_default_empty := &sql.Column{
		Type:     char32_utf8_bin,
		Default:  mustDefault(expression.NewLiteral("", char32_utf8_bin), char32_utf8_bin, true, false),
		Nullable: false,
	}
	char255_ascii_general_ci_not_null_default_empty := &sql.Column{
		Type:     char255_ascii_general_ci,
		Default:  mustDefault(expression.NewLiteral("", char255_ascii_general_ci), char255_ascii_general_ci, true, false),
		Nullable: false,
	}
	enum_N_Y_utf8_general_ci_not_null_default_N := &sql.Column{
		Type:     enum_N_Y_utf8_general_ci,
		Default:  mustDefault(expression.NewLiteral("N", enum_N_Y_utf8_general_ci), enum_N_Y_utf8_general_ci, true, false),
		Nullable: false,
	}

	roleEdgesTblSchema = sql.Schema{
		columnTemplate("FROM_HOST", roleEdgesTblName, true, char255_ascii_general_ci_not_null_default_empty),
		columnTemplate("FROM_USER", roleEdgesTblName, true, char32_utf8_bin_not_null_default_empty),
		columnTemplate("TO_HOST", roleEdgesTblName, true, char255_ascii_general_ci_not_null_default_empty),
		columnTemplate("TO_USER", roleEdgesTblName, true, char32_utf8_bin_not_null_default_empty),
		columnTemplate("WITH_ADMIN_OPTION", roleEdgesTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
	}
}

// These represent the column indexes of the "role_edges" Grant Table.
const (
	roleEdgesTblColIndex_FROM_HOST int = iota
	roleEdgesTblColIndex_FROM_USER
	roleEdgesTblColIndex_TO_HOST
	roleEdgesTblColIndex_TO_USER
	roleEdgesTblColIndex_WITH_ADMIN_OPTION
)
