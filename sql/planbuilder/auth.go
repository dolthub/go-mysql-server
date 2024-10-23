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

package planbuilder

import (
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

// TODO: doc
var Authorization = func(b *Builder, auth ast.AuthInformation) {
	// TODO: expose necessary stuff from Builder to public
	ctx := b.ctx
	// TODO: database loading shouldn't be done for every call, this needs to be cached for each Parse call in some way
	db, err := b.cat.Database(ctx, "mysql")
	if err != nil {
		b.handleErr(err)
	}
	mysqlDb, ok := db.(*mysql_db.MySQLDb)
	if !ok {
		b.handleErr(fmt.Errorf("FOR TESTING: could not load the `mysql` database")) // TODO: Check if this is likely
	}
	if !mysqlDb.Enabled() {
		return
	}
	// TODO: cache that the user exists
	client := ctx.Session.Client()
	user := func() *mysql_db.User {
		rd := mysqlDb.Reader()
		defer rd.Close()
		return mysqlDb.GetUser(rd, client.User, client.Address, false)
	}()
	if user == nil {
		b.handleErr(mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%s'", client.User))
	}
	// TODO: cache for the call
	privSet := mysqlDb.UserActivePrivilegeSet(ctx)

	var privilegeTypes []sql.PrivilegeType
	switch auth.AuthType {
	case ast.AuthType_IGNORE:
		// This means that authorization is being handled elsewhere (such as a child or parent), and should be ignored here
		return
	case ast.AuthType_DELETE:
		privilegeTypes = []sql.PrivilegeType{sql.PrivilegeType_Delete}
	case ast.AuthType_INSERT:
		privilegeTypes = []sql.PrivilegeType{sql.PrivilegeType_Insert}
	case ast.AuthType_REPLACE:
		privilegeTypes = []sql.PrivilegeType{sql.PrivilegeType_Insert, sql.PrivilegeType_Delete}
	case ast.AuthType_SELECT:
		privilegeTypes = []sql.PrivilegeType{sql.PrivilegeType_Select}
	case ast.AuthType_UPDATE:
		privilegeTypes = []sql.PrivilegeType{sql.PrivilegeType_Update}
	default:
		b.handleErr(fmt.Errorf("FOR TESTING: default case hit for AuthType"))
	}

	hasPrivileges := true
	switch auth.TargetType {
	case ast.AuthTargetType_SingleTableIdentifier:
		dbName := auth.TargetNames[0]
		tableName := auth.TargetNames[1]
		if strings.EqualFold(dbName, "information_schema") {
			return
		}
		authCheckDatabaseTableNames(b, privSet, user.User, dbName, tableName)
		subject := sql.PrivilegeCheckSubject{
			Database: authDatabaseName(ctx, dbName),
			Table:    tableName,
		}
		hasPrivileges = mysqlDb.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(subject, privilegeTypes...))
	case ast.AuthTargetType_MultipleTableIdentifiers:
		for i := 0; i < len(auth.TargetNames) && hasPrivileges; i += 2 {
			dbName := auth.TargetNames[i]
			tableName := auth.TargetNames[i+1]
			if strings.EqualFold(dbName, "information_schema") {
				continue
			}
			authCheckDatabaseTableNames(b, privSet, user.User, dbName, tableName)
			subject := sql.PrivilegeCheckSubject{
				Database: authDatabaseName(ctx, dbName),
				Table:    tableName,
			}
			hasPrivileges = hasPrivileges && mysqlDb.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(subject, privilegeTypes...))
		}
	default:
		b.handleErr(fmt.Errorf("FOR TESTING: default case hit for TargetType"))
	}

	if !hasPrivileges {
		b.handleErr(sql.ErrPrivilegeCheckFailed.New(user.UserHostToString("'")))
	}
}

// authDatabaseName uses the current database from the context if a database is not specified, otherwise it returns the
// given database name.
func authDatabaseName(ctx *sql.Context, dbName string) string {
	if len(dbName) == 0 {
		return ctx.GetCurrentDatabase()
	}
	return dbName
}

// authCheckDatabaseTableNames errors if the user does not have access to the database or table in any capacity,
// regardless of the command.
func authCheckDatabaseTableNames(b *Builder, privSet mysql_db.PrivilegeSet, userName string, dbName string, tableName string) {
	dbSet := privSet.Database(dbName)
	// If there are no usable privileges for this database then the table is inaccessible.
	if privSet.Count() == 0 && !dbSet.HasPrivileges() {
		b.handleErr(sql.ErrDatabaseAccessDeniedForUser.New(userName, dbName))
	}
	tblSet := dbSet.Table(tableName)
	// If the user has no global static privileges, database-level privileges, or table-relevant privileges then the
	// table is not accessible.
	if privSet.Count() == 0 && dbSet.Count() == 0 && !tblSet.HasPrivileges() {
		b.handleErr(sql.ErrTableAccessDeniedForUser.New(userName, tableName))
	}
}
