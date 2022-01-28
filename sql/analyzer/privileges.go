// Copyright 2021 Dolthub, Inc.
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
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/grant_tables"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// checkPrivileges verifies the given statement (node n) by checking that the calling user has the necessary privileges
// to execute it.
func checkPrivileges(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	//TODO: add the remaining statements that interact with the grant tables
	switch n.(type) {
	case *plan.CreateUser, *plan.DropUser, *plan.RenameUser, *plan.CreateRole, *plan.DropRole,
		*plan.Grant, *plan.GrantRole, *plan.GrantProxy, *plan.Revoke, *plan.RevokeRole, *plan.RevokeAll, *plan.RevokeProxy:
		a.Catalog.GrantTables.Enabled = true
	}
	if !a.Catalog.GrantTables.Enabled {
		return n, nil
	}

	client := ctx.Session.Client()
	user := a.Catalog.GrantTables.GetUser(client.User, client.Address, false)
	if user == nil {
		return nil, mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%v'", client.User)
	}

	switch n := n.(type) {
	case *plan.InsertInto:
		if n.IsReplace {
			if !user.PrivilegeSet.Has(grant_tables.PrivilegeType_Insert, grant_tables.PrivilegeType_Delete) {
				return nil, sql.ErrPrivilegeCheckFailed.New("REPLACE", user.UserHostToString("'", `\'`), getTableName(n.Destination))
			}
		} else if !user.PrivilegeSet.Has(grant_tables.PrivilegeType_Insert) {
			return nil, sql.ErrPrivilegeCheckFailed.New("INSERT", user.UserHostToString("'", `\'`), getTableName(n.Destination))
		}
	case *plan.Update:
		if !user.PrivilegeSet.Has(grant_tables.PrivilegeType_Update) {
			return nil, sql.ErrPrivilegeCheckFailed.New("UPDATE", user.UserHostToString("'", `\'`), getTableName(n.Child))
		}
	case *plan.DeleteFrom:
		if !user.PrivilegeSet.Has(grant_tables.PrivilegeType_Delete) {
			return nil, sql.ErrPrivilegeCheckFailed.New("DELETE", user.UserHostToString("'", `\'`), getTableName(n.Child))
		}
	case *plan.Project:
		//TODO: a better way to do this would be to inspect the children of some nodes, such as filter nodes, and
		//recursively inspect their children until we get to a more well-defined node.
		if !user.PrivilegeSet.Has(grant_tables.PrivilegeType_Select) {
			return nil, sql.ErrPrivilegeCheckFailed.New("SELECT", user.UserHostToString("'", `\'`), getTableName(n.Child))
		}
	default:
		//TODO: eventually every node possible will be listed here, therefore the default behavior would error as that
		//would mean a new node is being parsed that doesn't have a specific case here. For now though, just return.
	}
	return n, nil
}
