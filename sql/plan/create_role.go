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

	"github.com/gabereiser/go-mysql-server/sql/mysql_db"
	"github.com/gabereiser/go-mysql-server/sql/types"

	"github.com/gabereiser/go-mysql-server/sql"
)

// CreateRole represents the statement CREATE ROLE.
type CreateRole struct {
	IfNotExists bool
	Roles       []UserName
	MySQLDb     sql.Database
}

// NewCreateRole returns a new CreateRole node.
func NewCreateRole(ifNotExists bool, roles []UserName) *CreateRole {
	return &CreateRole{
		IfNotExists: ifNotExists,
		Roles:       roles,
		MySQLDb:     sql.UnresolvedDatabase("mysql"),
	}
}

var _ sql.Node = (*CreateRole)(nil)

// Schema implements the interface sql.Node.
func (n *CreateRole) Schema() sql.Schema {
	return types.OkResultSchema
}

// String implements the interface sql.Node.
func (n *CreateRole) String() string {
	roles := make([]string, len(n.Roles))
	for i, role := range n.Roles {
		roles[i] = role.String("")
	}
	ifNotExists := ""
	if n.IfNotExists {
		ifNotExists = "IfNotExists: "
	}
	return fmt.Sprintf("CreateRole(%s%s)", ifNotExists, strings.Join(roles, ", "))
}

// Database implements the interface sql.Databaser.
func (n *CreateRole) Database() sql.Database {
	return n.MySQLDb
}

// WithDatabase implements the interface sql.Databaser.
func (n *CreateRole) WithDatabase(db sql.Database) (sql.Node, error) {
	nn := *n
	nn.MySQLDb = db
	return &nn, nil
}

// Resolved implements the interface sql.Node.
func (n *CreateRole) Resolved() bool {
	_, ok := n.MySQLDb.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the interface sql.Node.
func (n *CreateRole) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *CreateRole) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 0)
	}
	return n, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *CreateRole) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	// Both CREATE ROLE and CREATE USER are valid privileges, so we use an OR
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation("", "", "", sql.PrivilegeType_CreateRole)) ||
		opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation("", "", "", sql.PrivilegeType_CreateUser))
}

// RowIter implements the interface sql.Node.
func (n *CreateRole) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	mysqlDb, ok := n.MySQLDb.(*mysql_db.MySQLDb)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}

	userTableData := mysqlDb.UserTable().Data()
	for _, role := range n.Roles {
		userPk := mysql_db.UserPrimaryKey{
			Host: role.Host,
			User: role.Name,
		}
		if role.AnyHost {
			userPk.Host = "%"
		}
		existingRows := userTableData.Get(userPk)
		if len(existingRows) > 0 {
			if n.IfNotExists {
				continue
			}
			return nil, sql.ErrRoleCreationFailure.New(role.String("'"))
		}

		//TODO: When password expiration is implemented, make sure that roles have an expired password on creation
		err := userTableData.Put(ctx, &mysql_db.User{
			User:                userPk.User,
			Host:                userPk.Host,
			PrivilegeSet:        mysql_db.NewPrivilegeSet(),
			Plugin:              "mysql_native_password",
			Password:            "",
			PasswordLastChanged: time.Now().UTC(),
			Locked:              true,
			Attributes:          nil,
			IsRole:              true,
		})
		if err != nil {
			return nil, err
		}
	}
	if err := mysqlDb.Persist(ctx); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{types.NewOkResult(0)}), nil
}
