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

package plan

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/grant_tables"
)

// CreateUser represents the statement CREATE USER.
type CreateUser struct {
	IfNotExists     bool
	Users           []AuthenticatedUser
	DefaultRoles    []UserName
	TLSOptions      *TLSOptions
	AccountLimits   *AccountLimits
	PasswordOptions *PasswordOptions
	Locked          bool
	Attribute       string
	GrantTables     sql.Database
}

var _ sql.Node = (*CreateUser)(nil)
var _ sql.Databaser = (*CreateUser)(nil)

// Schema implements the interface sql.Node.
func (n *CreateUser) Schema() sql.Schema {
	return sql.OkResultSchema
}

// String implements the interface sql.Node.
func (n *CreateUser) String() string {
	users := make([]string, len(n.Users))
	for i, user := range n.Users {
		users[i] = user.UserName.StringWithQuote("", "")
	}
	ifNotExists := ""
	if n.IfNotExists {
		ifNotExists = "IfNotExists: "
	}
	return fmt.Sprintf("CreateUser(%s%s)", ifNotExists, strings.Join(users, ", "))
}

// Database implements the interface sql.Databaser.
func (n *CreateUser) Database() sql.Database {
	return n.GrantTables
}

// WithDatabase implements the interface sql.Databaser.
func (n *CreateUser) WithDatabase(db sql.Database) (sql.Node, error) {
	nn := *n
	nn.GrantTables = db
	return &nn, nil
}

// Resolved implements the interface sql.Node.
func (n *CreateUser) Resolved() bool {
	_, ok := n.GrantTables.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the interface sql.Node.
func (n *CreateUser) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *CreateUser) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// RowIter implements the interface sql.Node.
func (n *CreateUser) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	grantTables, ok := n.GrantTables.(*grant_tables.GrantTables)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	userTableData := grantTables.UserTable().Data()
	for _, user := range n.Users {
		userPk := grant_tables.UserPrimaryKey{
			Host: user.UserName.Host,
			User: user.UserName.Name,
		}
		existingRows := userTableData.Get(userPk)
		if len(existingRows) > 0 {
			if n.IfNotExists {
				continue
			}
			return nil, sql.ErrUserCreationFailure.New(user.UserName.StringWithQuote("'", ""))
		}
		//TODO: validate all of the data
		err := userTableData.Put(sql.Row{
			user.UserName.Host,      // 00: Host
			user.UserName.Name,      // 01: User
			"N",                     // 02: Select_priv
			"N",                     // 03: Insert_priv
			"N",                     // 04: Update_priv
			"N",                     // 05: Delete_priv
			"N",                     // 06: Create_priv
			"N",                     // 07: Drop_priv
			"N",                     // 08: Reload_priv
			"N",                     // 09: Shutdown_priv
			"N",                     // 10: Process_priv
			"N",                     // 11: File_priv
			"N",                     // 12: Grant_priv
			"N",                     // 13: References_priv
			"N",                     // 14: Index_priv
			"N",                     // 15: Alter_priv
			"N",                     // 16: Show_db_priv
			"N",                     // 17: Super_priv
			"N",                     // 18: Create_tmp_table_priv
			"N",                     // 19: Lock_tables_priv
			"N",                     // 20: Execute_priv
			"N",                     // 21: Repl_slave_priv
			"N",                     // 22: Repl_client_priv
			"N",                     // 23: Create_view_priv
			"N",                     // 24: Show_view_priv
			"N",                     // 25: Create_routine_priv
			"N",                     // 26: Alter_routine_priv
			"N",                     // 27: Create_user_priv
			"N",                     // 28: Event_priv
			"N",                     // 29: Trigger_priv
			"N",                     // 30: Create_tablespace_priv
			"",                      // 31: ssl_type
			"",                      // 32: ssl_cipher
			"",                      // 33: x509_issuer
			"",                      // 34: x509_subject
			0,                       // 35: max_questions
			0,                       // 36: max_updates
			0,                       // 37: max_connections
			0,                       // 38: max_user_connections
			"caching_sha2_password", // 39: plugin
			nil,                     // 40: authentication_string
			"N",                     // 41: password_expired
			nil,                     // 42: password_last_changed
			nil,                     // 43: password_lifetime
			"N",                     // 44: account_locked
			"N",                     // 45: Create_role_priv
			"N",                     // 46: Drop_role_priv
			nil,                     // 47: Password_reuse_history
			nil,                     // 48: Password_reuse_time
			nil,                     // 49: Password_require_current
			nil,                     // 50: User_attributes
		})
		if err != nil {
			return nil, err
		}
	}
	err := grantTables.Persist(ctx)
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
}
