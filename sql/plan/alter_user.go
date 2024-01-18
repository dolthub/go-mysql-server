// Copyright 2024 Dolthub, Inc.
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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// AlterUser represents the statement ALTER USER.
type AlterUser struct {
	IfExists bool
	User     AuthenticatedUser
	MySQLDb  sql.Database
}

var _ sql.Node = (*AlterUser)(nil)
var _ sql.Databaser = (*AlterUser)(nil)
var _ sql.CollationCoercible = (*AlterUser)(nil)

// Schema implements the interface sql.Node.
func (a *AlterUser) Schema() sql.Schema {
	return types.OkResultSchema
}

// String implements the interface sql.Node.
func (a *AlterUser) String() string {
	ifNotExists := ""
	if a.IfExists {
		ifNotExists = "IfExists: "
	}
	return fmt.Sprintf("AlterUser(%s%s)", ifNotExists, a.User.String(""))
}

// Database implements the interface sql.Databaser.
func (a *AlterUser) Database() sql.Database {
	return a.MySQLDb
}

// WithDatabase implements the interface sql.Databaser.
func (a *AlterUser) WithDatabase(db sql.Database) (sql.Node, error) {
	aa := *a
	aa.MySQLDb = db
	return &aa, nil
}

// Resolved implements the interface sql.Node.
func (a *AlterUser) Resolved() bool {
	_, ok := a.MySQLDb.(sql.UnresolvedDatabase)
	return !ok
}

// IsReadOnly implements the interface sql.Node.
func (a *AlterUser) IsReadOnly() bool {
	return false
}

// Children implements the interface sql.Node.
func (a *AlterUser) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (a *AlterUser) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 0)
	}
	return a, nil
}

// CheckPrivileges implements the interface sql.Node.
func (a *AlterUser) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	// From the MySQL reference on ALTER USER:
	// https://dev.mysql.com/doc/refman/8.0/en/alter-user.html
	// ALTER USER generally requires either the global `CREATE USER` privilege, or the `UPDATE` privilege
	// for the `mysql` system schema.
	if opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(
		sql.PrivilegeCheckSubject{Database: "mysql"}, sql.PrivilegeType_Update)) {
		return true
	} else if opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(
		sql.PrivilegeCheckSubject{}, sql.PrivilegeType_CreateUser)) {
		return true
	}

	// There are several exceptions to the general privilege requirements. Currently, the only relevant one is
	// that any client who connects to the server using a non-anonymous account can change the password for that account.
	authenticatedUser := ctx.Session.Client()
	if a.User.Name == authenticatedUser.User {
		return true
	}

	return false
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (a *AlterUser) CollationCoercibility(_ *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}
