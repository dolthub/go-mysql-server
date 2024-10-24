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

package planbuilder

import (
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

// DefaultAuthorizationHandler returns the default AuthorizationHandler that is used by go-mysql-server. It is built
// with the assumption of being paired with an AST generated directly by the Vitess SQL parser.
func DefaultAuthorizationHandler() AuthorizationHandler {
	return defaultAuthorizationHandler{}
}

// defaultAuthorizationHandler handles authorization for ASTs that were generated directly by the Vitess SQL parser.
type defaultAuthorizationHandler struct{}

var _ AuthorizationHandler = defaultAuthorizationHandler{}

// NewQueryHandler implements the AuthorizationHandler interface.
func (handler defaultAuthorizationHandler) NewQueryHandler(ctx *sql.Context, cat sql.Catalog) (QueryAuthorizationHandler, error) {
	db, err := cat.Database(ctx, "mysql")
	if err != nil {
		return nil, err
	}
	mysqlDb, ok := db.(*mysql_db.MySQLDb)
	if !ok {
		return nil, fmt.Errorf("could not load the `mysql` database") // TODO: Check if this is likely
	}
	var user *mysql_db.User
	var privSet mysql_db.PrivilegeSet
	enabled := mysqlDb.Enabled()
	if enabled {
		client := ctx.Session.Client()
		user = func() *mysql_db.User {
			rd := mysqlDb.Reader()
			defer rd.Close()
			return mysqlDb.GetUser(rd, client.User, client.Address, false)
		}()
		if user == nil {
			return nil, mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%s'", client.User)
		}
		privSet = mysqlDb.UserActivePrivilegeSet(ctx)
	}
	return &defaultQueryAuthorizationHandler{
		ctx:     ctx,
		enabled: enabled,
		db:      mysqlDb,
		user:    user,
		privSet: privSet,
	}, nil
}

// defaultQueryAuthorizationHandler is the specific query handler for defaultAuthorizationHandler.
type defaultQueryAuthorizationHandler struct {
	ctx     *sql.Context
	enabled bool
	db      *mysql_db.MySQLDb
	user    *mysql_db.User
	privSet mysql_db.PrivilegeSet
}

var _ QueryAuthorizationHandler = (*defaultQueryAuthorizationHandler)(nil)

// Handle implements the QueryAuthorizationHandler interface.
func (h *defaultQueryAuthorizationHandler) Handle(auth ast.AuthInformation) error {
	if !h.enabled {
		return nil
	}

	var privilegeTypes []sql.PrivilegeType
	switch auth.AuthType {
	case ast.AuthType_IGNORE:
		// This means that authorization is being handled elsewhere (such as a child or parent), and should be ignored here
		return nil
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
		return fmt.Errorf("FOR TESTING: default case hit for AuthType")
	}

	hasPrivileges := true
	switch auth.TargetType {
	case ast.AuthTargetType_SingleTableIdentifier:
		dbName := auth.TargetNames[0]
		tableName := auth.TargetNames[1]
		if strings.EqualFold(dbName, "information_schema") {
			return nil
		}
		if err := h.authCheckDatabaseTableNames(h.privSet, h.user.User, dbName, tableName); err != nil {
			return err
		}
		subject := sql.PrivilegeCheckSubject{
			Database: h.authDatabaseName(h.ctx, dbName),
			Table:    tableName,
		}
		hasPrivileges = h.db.UserHasPrivileges(h.ctx, sql.NewPrivilegedOperation(subject, privilegeTypes...))
	case ast.AuthTargetType_MultipleTableIdentifiers:
		for i := 0; i < len(auth.TargetNames) && hasPrivileges; i += 2 {
			dbName := auth.TargetNames[i]
			tableName := auth.TargetNames[i+1]
			if strings.EqualFold(dbName, "information_schema") {
				continue
			}
			if err := h.authCheckDatabaseTableNames(h.privSet, h.user.User, dbName, tableName); err != nil {
				return err
			}
			subject := sql.PrivilegeCheckSubject{
				Database: h.authDatabaseName(h.ctx, dbName),
				Table:    tableName,
			}
			hasPrivileges = h.db.UserHasPrivileges(h.ctx, sql.NewPrivilegedOperation(subject, privilegeTypes...))
		}
	default:
		return fmt.Errorf("FOR TESTING: default case hit for TargetType")
	}

	if !hasPrivileges {
		return sql.ErrPrivilegeCheckFailed.New(h.user.UserHostToString("'"))
	}
	return nil
}

// authDatabaseName uses the current database from the context if a database is not specified, otherwise it returns the
// given database name.
func (h *defaultQueryAuthorizationHandler) authDatabaseName(ctx *sql.Context, dbName string) string {
	if len(dbName) == 0 {
		return ctx.GetCurrentDatabase()
	}
	return dbName
}

// authCheckDatabaseTableNames errors if the user does not have access to the database or table in any capacity,
// regardless of the command.
func (h *defaultQueryAuthorizationHandler) authCheckDatabaseTableNames(privSet mysql_db.PrivilegeSet, userName string, dbName string, tableName string) error {
	dbSet := privSet.Database(dbName)
	// If there are no usable privileges for this database then the table is inaccessible.
	if privSet.Count() == 0 && !dbSet.HasPrivileges() {
		return sql.ErrDatabaseAccessDeniedForUser.New(userName, dbName)
	}
	tblSet := dbSet.Table(tableName)
	// If the user has no global static privileges, database-level privileges, or table-relevant privileges then the
	// table is not accessible.
	if privSet.Count() == 0 && dbSet.Count() == 0 && !tblSet.HasPrivileges() {
		return sql.ErrTableAccessDeniedForUser.New(userName, tableName)
	}
	return nil
}
