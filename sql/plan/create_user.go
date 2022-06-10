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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
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
	MySQLDb         sql.Database
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
		users[i] = user.UserName.String("")
	}
	ifNotExists := ""
	if n.IfNotExists {
		ifNotExists = "IfNotExists: "
	}
	return fmt.Sprintf("CreateUser(%s%s)", ifNotExists, strings.Join(users, ", "))
}

// Database implements the interface sql.Databaser.
func (n *CreateUser) Database() sql.Database {
	return n.MySQLDb
}

// WithDatabase implements the interface sql.Databaser.
func (n *CreateUser) WithDatabase(db sql.Database) (sql.Node, error) {
	nn := *n
	nn.MySQLDb = db
	return &nn, nil
}

// Resolved implements the interface sql.Node.
func (n *CreateUser) Resolved() bool {
	_, ok := n.MySQLDb.(sql.UnresolvedDatabase)
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

// CheckPrivileges implements the interface sql.Node.
func (n *CreateUser) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation("", "", "", sql.PrivilegeType_CreateUser))
}

// RowIter implements the interface sql.Node.
func (n *CreateUser) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	mysqlDb, ok := n.MySQLDb.(*mysql_db.MySQLDb)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	// Check if you can even persist in the first place
	if err := mysqlDb.ValidateCanPersist(); err != nil {
		return nil, err
	}
	userTableData := mysqlDb.UserTable().Data()
	for _, user := range n.Users {
		userPk := mysql_db.UserPrimaryKey{
			Host: user.UserName.Host,
			User: user.UserName.Name,
		}
		existingRows := userTableData.Get(userPk)
		if len(existingRows) > 0 {
			if n.IfNotExists {
				continue
			}
			return nil, sql.ErrUserCreationFailure.New(user.UserName.String("'"))
		}

		plugin := "mysql_native_password"
		password := ""
		if user.Auth1 != nil {
			plugin = user.Auth1.Plugin()
			password = user.Auth1.Password()
		}
		// TODO: attributes should probably not be nil, but setting it to &n.Attribute causes unexpected behavior
		//TODO: validate all of the data
		err := userTableData.Put(ctx, &mysql_db.User{
			User:                user.UserName.Name,
			Host:                user.UserName.Host,
			PrivilegeSet:        mysql_db.NewPrivilegeSet(),
			Plugin:              plugin,
			Password:            password,
			PasswordLastChanged: time.Now().UTC(),
			Locked:              false,
			Attributes:          nil,
			IsRole:              false,
		})
		if err != nil {
			return nil, err
		}
	}
	if err := mysqlDb.Persist(ctx); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
}
