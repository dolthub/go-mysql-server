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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// PrivilegedDatabaseProvider is a wrapper around a normal sql.DatabaseProvider that takes a context's client's
// privileges into consideration when returning a sql.Database. In addition, any returned databases are wrapped with
// PrivilegedDatabase.
type PrivilegedDatabaseProvider struct {
	grantTables *MySQLDb
	provider    sql.DatabaseProvider
	authHandler sql.AuthorizationHandler
}

var _ sql.DatabaseProvider = PrivilegedDatabaseProvider{}

// NewPrivilegedDatabaseProvider returns a new PrivilegedDatabaseProvider. As a sql.DatabaseProvider may be added to an
// analyzer when Grant Tables are disabled (and Grant Tables may be enabled or disabled at any time), a new
// PrivilegedDatabaseProvider is returned whenever the sql.DatabaseProvider is needed (as long as Grant Tables are
// enabled) rather than wrapping a sql.DatabaseProvider when it is provided to the analyzer.
func NewPrivilegedDatabaseProvider(grantTables *MySQLDb, p sql.DatabaseProvider, authHandler sql.AuthorizationHandler) sql.DatabaseProvider {
	return PrivilegedDatabaseProvider{
		grantTables: grantTables,
		provider:    p,
		authHandler: authHandler,
	}
}

// Database implements the interface sql.DatabaseProvider.
func (pdp PrivilegedDatabaseProvider) Database(ctx *sql.Context, name string) (sql.Database, error) {
	if strings.ToLower(name) == "mysql" {
		return pdp.grantTables, nil
	}

	db, providerErr := pdp.provider.Database(ctx, name)
	if sql.ErrDatabaseNotFound.Is(providerErr) {
		// continue to priv check below, which will deny access or return not found as appropriate, before returning this
		// original not found error
	} else if providerErr != nil {
		return nil, providerErr
	}

	checkName := name
	if adb, ok := db.(sql.AliasedDatabase); ok {
		checkName = adb.AliasedName()
	}

	privSet := pdp.grantTables.UserActivePrivilegeSet(ctx)
	// If the user has no global static privileges or database-relevant privileges then the database is not accessible.
	if privSet.Count() == 0 && !privSet.Database(checkName).HasPrivileges() {
		return nil, sql.ErrDatabaseAccessDeniedForUser.New(pdp.usernameFromCtx(ctx), checkName)
	}

	if providerErr != nil {
		return nil, providerErr
	}

	return db, nil
}

// HasDatabase implements the interface sql.DatabaseProvider.
func (pdp PrivilegedDatabaseProvider) HasDatabase(ctx *sql.Context, name string) bool {
	if strings.EqualFold(name, "mysql") {
		return true
	}

	db, err := pdp.provider.Database(ctx, name)
	if sql.ErrDatabaseNotFound.Is(err) {
		// continue to check below, which will deny access or return not found as appropriate
	} else if err != nil {
		return false
	}

	if adb, ok := db.(sql.AliasedDatabase); ok {
		name = adb.AliasedName()
	}

	privSet := pdp.grantTables.UserActivePrivilegeSet(ctx)
	// If the user has no global static privileges or database-relevant privileges then the database is not accessible.
	if privSet.Count() == 0 && !privSet.Database(name).HasPrivileges() {
		return false
	}

	return pdp.provider.HasDatabase(ctx, name)
}

// AllDatabases implements the interface sql.DatabaseProvider.
func (pdp PrivilegedDatabaseProvider) AllDatabases(ctx *sql.Context) []sql.Database {
	privilegeSet := pdp.grantTables.UserActivePrivilegeSet(ctx)
	privilegeSetCount := privilegeSet.Count()

	var databasesWithAccess []sql.Database
	allDatabases := pdp.provider.AllDatabases(ctx)
	for _, db := range allDatabases {
		// If the user has any global static privileges or database-relevant privileges then the database is accessible
		checkName := db.Name()

		if adb, ok := db.(sql.AliasedDatabase); ok {
			checkName = adb.AliasedName()
		}

		if privilegeSetCount > 0 || privilegeSet.Database(checkName).HasPrivileges() {
			databasesWithAccess = append(databasesWithAccess, db)
		}
	}
	return databasesWithAccess
}

// usernameFromCtx returns the username from the context, properly formatted for returned errors.
func (pdp PrivilegedDatabaseProvider) usernameFromCtx(ctx *sql.Context) string {
	client := ctx.Session.Client()
	return User{User: client.User, Host: client.Address}.UserHostToString("'")
}
