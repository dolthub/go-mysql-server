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

package rowexec

import (
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func (b *BaseBuilder) buildFlushPrivileges(ctx *sql.Context, n *plan.FlushPrivileges, row sql.Row) (sql.RowIter, error) {
	gts, ok := n.MysqlDb.(*mysql_db.MySQLDb)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	err := gts.Persist(ctx)
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.Row{types.NewOkResult(0)}), nil
}

func (b *BaseBuilder) buildDropUser(ctx *sql.Context, n *plan.DropUser, row sql.Row) (sql.RowIter, error) {
	mysqlDb, ok := n.MySQLDb.(*mysql_db.MySQLDb)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	userTableData := mysqlDb.UserTable().Data()
	roleEdgesData := mysqlDb.RoleEdgesTable().Data()
	for _, user := range n.Users {
		existingUser := mysqlDb.GetUser(user.Name, user.Host, false)
		if existingUser == nil {
			if n.IfExists {
				continue
			}
			return nil, sql.ErrUserDeletionFailure.New(user.String("'"))
		}

		//TODO: if a user is mentioned in the "mandatory_roles" (users and roles are interchangeable) system variable then they cannot be dropped
		err := userTableData.Remove(ctx, mysql_db.UserPrimaryKey{
			Host: existingUser.Host,
			User: existingUser.User,
		}, nil)
		if err != nil {
			return nil, err
		}
		err = roleEdgesData.Remove(ctx, mysql_db.RoleEdgesFromKey{
			FromHost: existingUser.Host,
			FromUser: existingUser.User,
		}, nil)
		if err != nil {
			return nil, err
		}
		err = roleEdgesData.Remove(ctx, mysql_db.RoleEdgesToKey{
			ToHost: existingUser.Host,
			ToUser: existingUser.User,
		}, nil)
		if err != nil {
			return nil, err
		}
	}
	if err := mysqlDb.Persist(ctx); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{types.NewOkResult(0)}), nil
}

func (b *BaseBuilder) buildRevokeRole(ctx *sql.Context, n *plan.RevokeRole, row sql.Row) (sql.RowIter, error) {
	mysqlDb, ok := n.MySQLDb.(*mysql_db.MySQLDb)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	roleEdgesData := mysqlDb.RoleEdgesTable().Data()
	for _, targetUser := range n.TargetUsers {
		user := mysqlDb.GetUser(targetUser.Name, targetUser.Host, false)
		if user == nil {
			return nil, sql.ErrGrantRevokeRoleDoesNotExist.New(targetUser.String("`"))
		}
		for _, targetRole := range n.Roles {
			role := mysqlDb.GetUser(targetRole.Name, targetRole.Host, true)
			if role == nil {
				return nil, sql.ErrGrantRevokeRoleDoesNotExist.New(targetRole.String("`"))
			}
			//TODO: if a role is mentioned in the "mandatory_roles" system variable then they cannot be revoked
			err := roleEdgesData.Remove(ctx, mysql_db.RoleEdgesPrimaryKey{
				FromHost: role.Host,
				FromUser: role.User,
				ToHost:   user.Host,
				ToUser:   user.User,
			}, nil)
			if err != nil {
				return nil, err
			}
		}
	}
	if err := mysqlDb.Persist(ctx); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{types.NewOkResult(0)}), nil
}

func (b *BaseBuilder) buildDropRole(ctx *sql.Context, n *plan.DropRole, row sql.Row) (sql.RowIter, error) {
	mysqlDb, ok := n.MySQLDb.(*mysql_db.MySQLDb)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	userTableData := mysqlDb.UserTable().Data()
	roleEdgesData := mysqlDb.RoleEdgesTable().Data()
	for _, role := range n.Roles {
		userPk := mysql_db.UserPrimaryKey{
			Host: role.Host,
			User: role.Name,
		}
		if role.AnyHost {
			userPk.Host = "%"
		}
		existingRows := userTableData.Get(userPk)
		if len(existingRows) == 0 {
			if n.IfExists {
				continue
			}
			return nil, sql.ErrRoleDeletionFailure.New(role.String("'"))
		}
		existingUser := existingRows[0].(*mysql_db.User)

		//TODO: if a role is mentioned in the "mandatory_roles" system variable then they cannot be dropped
		err := userTableData.Remove(ctx, userPk, nil)
		if err != nil {
			return nil, err
		}
		err = roleEdgesData.Remove(ctx, mysql_db.RoleEdgesFromKey{
			FromHost: existingUser.Host,
			FromUser: existingUser.User,
		}, nil)
		if err != nil {
			return nil, err
		}
		err = roleEdgesData.Remove(ctx, mysql_db.RoleEdgesToKey{
			ToHost: existingUser.Host,
			ToUser: existingUser.User,
		}, nil)
		if err != nil {
			return nil, err
		}
	}
	if err := mysqlDb.Persist(ctx); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{types.NewOkResult(0)}), nil
}

func (b *BaseBuilder) buildRevokeProxy(ctx *sql.Context, n *plan.RevokeProxy, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("%T has no execution iterator", n)
}

func (b *BaseBuilder) buildGrantRole(ctx *sql.Context, n *plan.GrantRole, row sql.Row) (sql.RowIter, error) {
	mysqlDb, ok := n.MySQLDb.(*mysql_db.MySQLDb)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	roleEdgesData := mysqlDb.RoleEdgesTable().Data()
	for _, targetUser := range n.TargetUsers {
		user := mysqlDb.GetUser(targetUser.Name, targetUser.Host, false)
		if user == nil {
			return nil, sql.ErrGrantRevokeRoleDoesNotExist.New(targetUser.String("`"))
		}
		for _, targetRole := range n.Roles {
			role := mysqlDb.GetUser(targetRole.Name, targetRole.Host, true)
			if role == nil {
				return nil, sql.ErrGrantRevokeRoleDoesNotExist.New(targetRole.String("`"))
			}
			err := roleEdgesData.Put(ctx, &mysql_db.RoleEdge{
				FromHost:        role.Host,
				FromUser:        role.User,
				ToHost:          user.Host,
				ToUser:          user.User,
				WithAdminOption: n.WithAdminOption,
			})
			if err != nil {
				return nil, err
			}
		}
	}
	if err := mysqlDb.Persist(ctx); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{types.NewOkResult(0)}), nil
}

func (b *BaseBuilder) buildGrantProxy(ctx *sql.Context, n *plan.GrantProxy, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("%T has no execution iterator", n)
}

func (b *BaseBuilder) buildRenameUser(ctx *sql.Context, n *plan.RenameUser, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("not yet implemented")
}

func (b *BaseBuilder) buildRevoke(ctx *sql.Context, n *plan.Revoke, row sql.Row) (sql.RowIter, error) {
	mysqlDb, ok := n.MySQLDb.(*mysql_db.MySQLDb)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	if n.PrivilegeLevel.Database == "*" && n.PrivilegeLevel.TableRoutine == "*" {
		if n.ObjectType != plan.ObjectType_Any {
			return nil, sql.ErrGrantRevokeIllegalPrivilege.New()
		}
		for _, revokeUser := range n.Users {
			user := mysqlDb.GetUser(revokeUser.Name, revokeUser.Host, false)
			if user == nil {
				return nil, sql.ErrGrantUserDoesNotExist.New()
			}
			if err := n.HandleGlobalPrivileges(user); err != nil {
				return nil, err
			}
		}
	} else if n.PrivilegeLevel.Database != "*" && n.PrivilegeLevel.TableRoutine == "*" {
		database := n.PrivilegeLevel.Database
		if database == "" {
			database = ctx.GetCurrentDatabase()
			if database == "" {
				return nil, sql.ErrNoDatabaseSelected.New()
			}
		}
		if n.ObjectType != plan.ObjectType_Any {
			return nil, sql.ErrGrantRevokeIllegalPrivilege.New()
		}
		for _, revokeUser := range n.Users {
			user := mysqlDb.GetUser(revokeUser.Name, revokeUser.Host, false)
			if user == nil {
				return nil, sql.ErrGrantUserDoesNotExist.New()
			}
			if err := n.HandleDatabasePrivileges(user, database); err != nil {
				return nil, err
			}
		}
	} else {
		database := n.PrivilegeLevel.Database
		if database == "" {
			database = ctx.GetCurrentDatabase()
			if database == "" {
				return nil, sql.ErrNoDatabaseSelected.New()
			}
		}
		if n.ObjectType != plan.ObjectType_Any {
			//TODO: implement object types
			return nil, fmt.Errorf("GRANT has not yet implemented object types")
		}
		for _, grantUser := range n.Users {
			user := mysqlDb.GetUser(grantUser.Name, grantUser.Host, false)
			if user == nil {
				return nil, sql.ErrGrantUserDoesNotExist.New()
			}
			if err := n.HandleTablePrivileges(user, database, n.PrivilegeLevel.TableRoutine); err != nil {
				return nil, err
			}
		}
	}
	if err := mysqlDb.Persist(ctx); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{types.NewOkResult(0)}), nil
}

func (b *BaseBuilder) buildRevokeAll(ctx *sql.Context, n *plan.RevokeAll, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("not yet implemented")
}

func (b *BaseBuilder) buildGrant(ctx *sql.Context, n *plan.Grant, row sql.Row) (sql.RowIter, error) {
	mysqlDb, ok := n.MySQLDb.(*mysql_db.MySQLDb)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	if n.PrivilegeLevel.Database == "*" && n.PrivilegeLevel.TableRoutine == "*" {
		if n.ObjectType != plan.ObjectType_Any {
			return nil, sql.ErrGrantRevokeIllegalPrivilege.New()
		}
		if n.As != nil {
			return nil, fmt.Errorf("GRANT has not yet implemented user assumption")
		}
		for _, grantUser := range n.Users {
			user := mysqlDb.GetUser(grantUser.Name, grantUser.Host, false)
			if user == nil {
				return nil, sql.ErrGrantUserDoesNotExist.New()
			}
			if err := n.HandleGlobalPrivileges(user); err != nil {
				return nil, err
			}
			if n.WithGrantOption {
				user.PrivilegeSet.AddGlobalStatic(sql.PrivilegeType_GrantOption)
			}
		}
	} else if n.PrivilegeLevel.Database != "*" && n.PrivilegeLevel.TableRoutine == "*" {
		database := n.PrivilegeLevel.Database
		if database == "" {
			database = ctx.GetCurrentDatabase()
			if database == "" {
				return nil, sql.ErrNoDatabaseSelected.New()
			}
		}
		if n.ObjectType != plan.ObjectType_Any {
			return nil, sql.ErrGrantRevokeIllegalPrivilege.New()
		}
		if n.As != nil {
			return nil, fmt.Errorf("GRANT has not yet implemented user assumption")
		}
		for _, grantUser := range n.Users {
			user := mysqlDb.GetUser(grantUser.Name, grantUser.Host, false)
			if user == nil {
				return nil, sql.ErrGrantUserDoesNotExist.New()
			}
			if err := n.HandleDatabasePrivileges(user, database); err != nil {
				return nil, err
			}
			if n.WithGrantOption {
				user.PrivilegeSet.AddDatabase(database, sql.PrivilegeType_GrantOption)
			}
		}
	} else {
		database := n.PrivilegeLevel.Database
		if database == "" {
			database = ctx.GetCurrentDatabase()
			if database == "" {
				return nil, sql.ErrNoDatabaseSelected.New()
			}
		}
		if n.ObjectType != plan.ObjectType_Any {
			//TODO: implement object types
			return nil, fmt.Errorf("GRANT has not yet implemented object types")
		}
		if n.As != nil {
			return nil, fmt.Errorf("GRANT has not yet implemented user assumption")
		}
		for _, grantUser := range n.Users {
			user := mysqlDb.GetUser(grantUser.Name, grantUser.Host, false)
			if user == nil {
				return nil, sql.ErrGrantUserDoesNotExist.New()
			}
			if err := n.HandleTablePrivileges(user, database, n.PrivilegeLevel.TableRoutine); err != nil {
				return nil, err
			}
			if n.WithGrantOption {
				user.PrivilegeSet.AddTable(database, n.PrivilegeLevel.TableRoutine, sql.PrivilegeType_GrantOption)
			}
		}
	}
	if err := mysqlDb.Persist(ctx); err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.Row{types.NewOkResult(0)}), nil
}

func (b *BaseBuilder) buildCreateRole(ctx *sql.Context, n *plan.CreateRole, row sql.Row) (sql.RowIter, error) {
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
