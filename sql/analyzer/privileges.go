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
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// checkPrivileges verifies the given statement (node n) by checking that the calling user has the necessary privileges
// to execute it.
func checkPrivileges(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	grantTables := a.Catalog.GrantTables
	switch n.(type) {
	case *plan.CreateUser, *plan.DropUser, *plan.RenameUser, *plan.CreateRole, *plan.DropRole,
		*plan.Grant, *plan.GrantRole, *plan.GrantProxy, *plan.Revoke, *plan.RevokeRole, *plan.RevokeAll, *plan.RevokeProxy:
		grantTables.Enabled = true
	}
	if !grantTables.Enabled {
		return n, nil
	}

	client := ctx.Session.Client()
	user := grantTables.GetUser(client.User, client.Address, false)
	if user == nil {
		return nil, mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%v'", ctx.Session.Client().User)
	}
	if isDualTable(getTable(n)) {
		return n, nil
	}
	if !n.CheckPrivileges(ctx, a.Catalog.GrantTables) {
		return nil, sql.ErrPrivilegeCheckFailed.New(user.UserHostToString("'"))
	}
	return n, nil
}
