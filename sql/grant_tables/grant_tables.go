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

package grant_tables

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"net"
	"strings"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
)

// GrantTables are the collection of tables that are used with any user or privilege-related operations.
// https://dev.mysql.com/doc/refman/8.0/en/grant-tables.html
type GrantTables struct {
	Enabled bool

	user        *grantTable
	role_edges  *grantTable
	db          *grantTableShim
	tables_priv *grantTableShim
	//TODO: add the rest of these tables
	//global_grants    *grantTable
	//columns_priv     *grantTable
	//procs_priv       *grantTable
	//proxies_priv     *grantTable
	//default_roles    *grantTable
	//password_history *grantTable
}

var _ sql.Database = (*GrantTables)(nil)
var _ mysql.AuthServer = (*GrantTables)(nil)

// CreateEmptyGrantTables returns a collection of Grant Tables that do not contain any data.
func CreateEmptyGrantTables() *GrantTables {
	// original tables
	grantTables := &GrantTables{
		user:       newGrantTable(userTblName, userTblSchema, &User{}, UserPrimaryKey{}, UserSecondaryKey{}),
		role_edges: newGrantTable(roleEdgesTblName, roleEdgesTblSchema, &RoleEdge{}, RoleEdgesPrimaryKey{}, RoleEdgesFromKey{}, RoleEdgesToKey{}),
	}

	// shims
	grantTables.db = newGrantTableShim(dbTblName, dbTblSchema, grantTables.user, DbConverter{})
	grantTables.tables_priv = newGrantTableShim(tablesPrivTblName, tablesPrivTblSchema, grantTables.user, TablesPrivConverter{})

	return grantTables
}

// AddRootAccount adds the root account to the list of accounts.
func (g *GrantTables) AddRootAccount() {
	g.Enabled = true
	addSuperUser(g.user, "root", "localhost", "")
}

// AddSuperUser adds the given username and password to the list of accounts. This is a temporary function, which is
// meant to replace the "auth.New..." functions while the remaining functions are added.
func (g *GrantTables) AddSuperUser(username string, password string) {
	//TODO: remove this function and the called function
	g.Enabled = true
	if len(password) > 0 {
		hash := sha1.New()
		hash.Write([]byte(password))
		s1 := hash.Sum(nil)
		hash.Reset()
		hash.Write(s1)
		s2 := hash.Sum(nil)
		password = "*" + strings.ToUpper(hex.EncodeToString(s2))
	}
	addSuperUser(g.user, username, "%", password)
}

// GetUser returns a user matching the given user and host if it exists. Due to the slight difference between users and
// roles, roleSearch changes whether the search matches against user or role rules.
func (g *GrantTables) GetUser(user string, host string, roleSearch bool) *User {
	//TODO: determine what the localhost is on the machine, then handle the conversion between ip and localhost
	// For now, this just does another check for localhost if the host is 127.0.0.1
	//TODO: match on anonymous users, which have an empty username (different for roles)
	var userEntry *User
	userEntries := g.user.data.Get(UserPrimaryKey{
		Host: host,
		User: user,
	})
	if len(userEntries) == 1 {
		userEntry = userEntries[0].(*User)
	} else {
		userEntries = g.user.data.Get(UserSecondaryKey{
			User: user,
		})
		for _, readUserEntry := range userEntries {
			readUserEntry := readUserEntry.(*User)
			//TODO: use the most specific match first, using "%" only if there isn't a more specific match
			if host == readUserEntry.Host || (host == "127.0.0.1" && readUserEntry.Host == "localhost") ||
				(readUserEntry.Host == "%" && (!roleSearch || host == "")) {
				userEntry = readUserEntry
				break
			}
		}
	}
	return userEntry
}

// UserActivePrivilegeSet fetches the User, and returns their entire active privilege set. This takes into account the
// active roles, which are set in the context, therefore the user is also pulled from the context.
func (g *GrantTables) UserActivePrivilegeSet(ctx *sql.Context) PrivilegeSet {
	client := ctx.Session.Client()
	user := g.GetUser(client.User, client.Address, false)
	if user == nil {
		return NewPrivilegeSet()
	}
	privSet := user.PrivilegeSet.Copy()
	roleEdgeEntries := g.role_edges.data.Get(RoleEdgesToKey{
		ToHost: user.Host,
		ToUser: user.User,
	})
	//TODO: filter the active roles using the context, rather than using every granted roles
	//TODO: System variable "activate_all_roles_on_login", if set, will set all roles as active upon logging in
	for _, roleEdgeEntry := range roleEdgeEntries {
		roleEdge := roleEdgeEntry.(*RoleEdge)
		role := g.GetUser(roleEdge.FromUser, roleEdge.FromHost, true)
		if role != nil {
			privSet.UnionWith(role.PrivilegeSet)
		}
	}
	return privSet
}

// UserHasPrivileges fetches the User, and returns whether they have the desired privileges necessary to perform the
// privileged operation. This takes into account the active roles, which are set in the context, therefore the user is
// also pulled from the context.
func (g *GrantTables) UserHasPrivileges(ctx *sql.Context, operations ...sql.PrivilegedOperation) bool {
	privSet := g.UserActivePrivilegeSet(ctx)
	for _, operation := range operations {
		for _, operationPriv := range operation.Privileges {
			if privSet.Has(operationPriv) {
				//TODO: Handle partial revokes
				continue
			}
			database := operation.Database
			if database == "" {
				database = ctx.GetCurrentDatabase()
			}
			dbSet := privSet.Database(database)
			if dbSet.Has(operationPriv) {
				continue
			}
			tblSet := dbSet.Table(operation.Table)
			if tblSet.Has(operationPriv) {
				continue
			}
			colSet := tblSet.Column(operation.Column)
			if !colSet.Has(operationPriv) {
				return false
			}
		}
	}
	return true
}

// Name implements the interface sql.Database.
func (g *GrantTables) Name() string {
	return "mysql"
}

// GetTableInsensitive implements the interface sql.Database.
func (g *GrantTables) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	switch strings.ToLower(tblName) {
	case userTblName:
		return g.user, true, nil
	case roleEdgesTblName:
		return g.role_edges, true, nil
	case dbTblName:
		return g.db, true, nil
	case tablesPrivTblName:
		return g.tables_priv, true, nil
	default:
		return nil, false, nil
	}
}

// GetTableNames implements the interface sql.Database.
func (g *GrantTables) GetTableNames(ctx *sql.Context) ([]string, error) {
	return []string{userTblName, dbTblName, tablesPrivTblName, roleEdgesTblName}, nil
}

// AuthMethod implements the interface mysql.AuthServer.
func (g *GrantTables) AuthMethod(user string) (string, error) {
	//TODO: this should pass in the host as well to correctly determine which auth method to use
	return "mysql_native_password", nil
}

// Salt implements the interface mysql.AuthServer.
func (g *GrantTables) Salt() ([]byte, error) {
	return mysql.NewSalt()
}

// ValidateHash implements the interface mysql.AuthServer. This is called when the method used is "mysql_native_password".
func (g *GrantTables) ValidateHash(salt []byte, user string, authResponse []byte, addr net.Addr) (mysql.Getter, error) {
	if !g.Enabled {
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			return nil, err
		}
		return MysqlConnectionUser{User: user, Host: host}, nil
	}

	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return nil, err
	}

	userEntry := g.GetUser(user, host, false)
	if userEntry == nil || userEntry.Locked {
		return nil, mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	if len(userEntry.Password) > 0 {
		if !validateMysqlNativePassword(authResponse, salt, userEntry.Password) {
			return nil, mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%v'", user)
		}
	} else if len(authResponse) > 0 { // password is nil or empty, therefore no password is set
		// a password was given and the account has no password set, therefore access is denied
		return nil, mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	return MysqlConnectionUser{User: userEntry.User, Host: userEntry.Host}, nil
}

// Negotiate implements the interface mysql.AuthServer. This is called when the method used is not "mysql_native_password".
func (g *GrantTables) Negotiate(c *mysql.Conn, user string, addr net.Addr) (mysql.Getter, error) {
	if !g.Enabled {
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			return nil, err
		}
		return MysqlConnectionUser{User: user, Host: host}, nil
	}
	return nil, fmt.Errorf(`the only user login interface currently supported is "mysql_native_password"`)
}

// Persist passes along all changes to the integrator.
func (g *GrantTables) Persist(ctx *sql.Context) error {
	//TODO: add the UserPersist interface, using this as a stand-in so I won't forget to put it where it needs to go
	return nil
}

// UserTable returns the "user" table.
func (g *GrantTables) UserTable() *grantTable {
	return g.user
}

// RoleEdgesTable returns the "role_edges" table.
func (g *GrantTables) RoleEdgesTable() *grantTable {
	return g.role_edges
}

// columnTemplate takes in a column as a template, and returns a new column with a different name based on the given
// template.
func columnTemplate(name string, source string, isPk bool, template *sql.Column) *sql.Column {
	newCol := *template
	if newCol.Default != nil {
		newCol.Default = &(*newCol.Default)
	}
	newCol.Name = name
	newCol.Source = source
	newCol.PrimaryKey = isPk
	return &newCol
}

// validateMysqlNativePassword was taken directly from vitess and validates the password hash for "mysql_native_password".
func validateMysqlNativePassword(authResponse, salt []byte, mysqlNativePassword string) bool {
	// SERVER: recv(authResponse)
	// 		   hash_stage1=xor(authResponse, sha1(salt,hash))
	// 		   candidate_hash2=sha1(hash_stage1)
	// 		   check(candidate_hash2==hash)
	if len(authResponse) == 0 || len(mysqlNativePassword) == 0 {
		return false
	}
	if mysqlNativePassword[0] == '*' {
		mysqlNativePassword = mysqlNativePassword[1:]
	}

	hash, err := hex.DecodeString(mysqlNativePassword)
	if err != nil {
		return false
	}

	// scramble = SHA1(salt+hash)
	crypt := sha1.New()
	crypt.Write(salt)
	crypt.Write(hash)
	scramble := crypt.Sum(nil)

	// token = scramble XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= authResponse[i]
	}
	stage1Hash := scramble
	crypt.Reset()
	crypt.Write(stage1Hash)
	candidateHash2 := crypt.Sum(nil)

	return bytes.Equal(candidateHash2, hash)
}

// mustDefault enforces that no error occurred when constructing the column default value.
func mustDefault(expr sql.Expression, outType sql.Type, representsLiteral bool, mayReturnNil bool) *sql.ColumnDefaultValue {
	colDef, err := sql.NewColumnDefaultValue(expr, outType, representsLiteral, mayReturnNil)
	if err != nil {
		panic(err)
	}
	return colDef
}

type dummyPartition struct{}

var _ sql.Partition = dummyPartition{}

// Key implements the interface sql.Partition.
func (d dummyPartition) Key() []byte {
	return nil
}
