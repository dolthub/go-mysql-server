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
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql/mysql_db/serial"
	"github.com/dolthub/vitess/go/mysql"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

// MySQLDataJSON is used to marshal/unmarshal data to/from JSON.
type MySQLDataJSON struct {
	Users []*User
	Roles []*RoleEdge
	// TODO: other tables in mysql db
}

// PrivilegePersistCallback represents the callback that will be called when the Grant Tables have been updated and need to be
// persisted.
type PrivilegePersistCallback func(ctx *sql.Context, users []*User, roleConnections []*RoleEdge) error
type DataPersistCallback func(ctx *sql.Context, mysqlDb *MySQLDataJSON) error

// MySQLDb are the collection of tables that are in the MySQL database
type MySQLDb struct {
	Enabled bool

	user        *mysqlTable
	role_edges  *mysqlTable
	db          *mysqlTableShim
	tables_priv *mysqlTableShim
	//TODO: add the rest of these tables
	//global_grants    *mysqlTable
	//columns_priv     *mysqlTable
	//procs_priv       *mysqlTable
	//proxies_priv     *mysqlTable
	//default_roles    *mysqlTable
	//password_history *mysqlTable

	privilegePersistFunc PrivilegePersistCallback
	dataPersistFunc      DataPersistCallback
}

var _ sql.Database = (*MySQLDb)(nil)
var _ mysql.AuthServer = (*MySQLDb)(nil)

// CreateEmptyMySQLDb returns a collection of MySQL Tables that do not contain any data.
func CreateEmptyMySQLDb() *MySQLDb {
	// original tables
	mysqlDb := &MySQLDb{
		user:       newMySQLTable(userTblName, userTblSchema, &User{}, UserPrimaryKey{}, UserSecondaryKey{}),
		role_edges: newMySQLTable(roleEdgesTblName, roleEdgesTblSchema, &RoleEdge{}, RoleEdgesPrimaryKey{}, RoleEdgesFromKey{}, RoleEdgesToKey{}),
	}

	// mysqlTable shims
	mysqlDb.db = newMySQLTableShim(dbTblName, dbTblSchema, mysqlDb.user, DbConverter{})
	mysqlDb.tables_priv = newMySQLTableShim(tablesPrivTblName, tablesPrivTblSchema, mysqlDb.user, TablesPrivConverter{})

	return mysqlDb
}

// LoadPrivilegeData adds the given data to the MySQL Tables. It does not remove any current data, but will overwrite any
// pre-existing data.
func (t *MySQLDb) LoadPrivilegeData(ctx *sql.Context, users []*User, roleConnections []*RoleEdge) error {
	// TODO: this is bad do something else
	// if there are already entries in user or role_edges table, clear them
	//if t.user.data.Count() > 0 {
	//	t.user.data.Clear()
	//	t.role_edges.data.Clear()
	//}

	t.Enabled = true
	for _, user := range users {
		if user == nil {
			continue
		}
		if err := t.user.data.Put(ctx, user); err != nil {
			return err
		}
	}
	for _, role := range roleConnections {
		if role == nil {
			continue
		}
		if err := t.role_edges.data.Put(ctx, role); err != nil {
			return err
		}
	}
	return nil
}

// LoadPrivilegeData adds the given data to the MySQL Tables. It does not remove any current data, but will overwrite any
// pre-existing data.
func (t *MySQLDb) LoadMySQLData(ctx *sql.Context, data *serial.MySQLDb) error {
	// Do nothing if data file didn't exist
	if data == nil {
		return nil
	}

	t.Enabled = true

	// TODO: do we want schema to be stored too?
	// Fill in user table
	for i := 0; i < data.UserLength(); i++ {
		serialUser := new(serial.User)
		if !data.User(serialUser, i) {
			continue
		}
		user := &User{
			User:                string(serialUser.User()),
			Host:                string(serialUser.Host()),
			PrivilegeSet:        PrivilegeSet{},
			Plugin:              string(serialUser.Plugin()),
			Password:            string(serialUser.Password()),
			PasswordLastChanged: time.Unix(int64(serialUser.PasswordLastChanged()), 0),
			Locked:              serialUser.Locked(),
			Attributes:          nil,
		}
		if err := t.user.data.Put(ctx, user); err != nil {
			return err
		}
	}

	// Fill in Roles table
	for i := 0; i < data.RoleEdgesLength(); i++ {
		serialRoleEdge := new(serial.RoleEdge)
		if !data.RoleEdges(serialRoleEdge, i) {
			continue
		}
		role := &RoleEdge{
			FromHost: string(serialRoleEdge.FromHost()),
			FromUser: string(serialRoleEdge.FromUser()),
			ToHost:   string(serialRoleEdge.ToHost()),
			ToUser:   string(serialRoleEdge.ToUser()),
		}
		if err := t.role_edges.data.Put(ctx, role); err != nil {
			return err
		}
	}

	// TODO: fill in other tables when they exist
	return nil
}

// SetPersistCallbacks sets the callback to be used when the Grant Tables have been updated and need to be persisted.
func (t *MySQLDb) SetPersistCallbacks(privilegePersistFunc PrivilegePersistCallback, dataPersistFunc DataPersistCallback) {
	t.privilegePersistFunc = privilegePersistFunc
	t.dataPersistFunc = dataPersistFunc
}

// AddRootAccount adds the root account to the list of accounts.
func (t *MySQLDb) AddRootAccount() {
	t.Enabled = true
	addSuperUser(t.user, "root", "localhost", "")
}

// AddSuperUser adds the given username and password to the list of accounts. This is a temporary function, which is
// meant to replace the "auth.New..." functions while the remaining functions are added.
func (t *MySQLDb) AddSuperUser(username string, password string) {
	//TODO: remove this function and the called function
	t.Enabled = true
	if len(password) > 0 {
		hash := sha1.New()
		hash.Write([]byte(password))
		s1 := hash.Sum(nil)
		hash.Reset()
		hash.Write(s1)
		s2 := hash.Sum(nil)
		password = "*" + strings.ToUpper(hex.EncodeToString(s2))
	}
	addSuperUser(t.user, username, "%", password)
}

// GetUser returns a user matching the given user and host if it exists. Due to the slight difference between users and
// roles, roleSearch changes whether the search matches against user or role rules.
func (t *MySQLDb) GetUser(user string, host string, roleSearch bool) *User {
	//TODO: determine what the localhost is on the machine, then handle the conversion between ip and localhost
	// For now, this just does another check for localhost if the host is 127.0.0.1
	//TODO: match on anonymous users, which have an empty username (different for roles)
	var userEntry *User
	userEntries := t.user.data.Get(UserPrimaryKey{
		Host: host,
		User: user,
	})
	if len(userEntries) == 1 {
		userEntry = userEntries[0].(*User)
	} else {
		userEntries = t.user.data.Get(UserSecondaryKey{
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
func (t *MySQLDb) UserActivePrivilegeSet(ctx *sql.Context) PrivilegeSet {
	client := ctx.Session.Client()
	user := t.GetUser(client.User, client.Address, false)
	if user == nil {
		return NewPrivilegeSet()
	}
	privSet := user.PrivilegeSet.Copy()
	roleEdgeEntries := t.role_edges.data.Get(RoleEdgesToKey{
		ToHost: user.Host,
		ToUser: user.User,
	})
	//TODO: filter the active roles using the context, rather than using every granted roles
	//TODO: System variable "activate_all_roles_on_login", if set, will set all roles as active upon logging in
	for _, roleEdgeEntry := range roleEdgeEntries {
		roleEdge := roleEdgeEntry.(*RoleEdge)
		role := t.GetUser(roleEdge.FromUser, roleEdge.FromHost, true)
		if role != nil {
			privSet.UnionWith(role.PrivilegeSet)
		}
	}
	return privSet
}

// UserHasPrivileges fetches the User, and returns whether they have the desired privileges necessary to perform the
// privileged operation. This takes into account the active roles, which are set in the context, therefore the user is
// also pulled from the context.
func (t *MySQLDb) UserHasPrivileges(ctx *sql.Context, operations ...sql.PrivilegedOperation) bool {
	privSet := t.UserActivePrivilegeSet(ctx)
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
func (t *MySQLDb) Name() string {
	return "mysql"
}

// GetTableInsensitive implements the interface sql.Database.
func (t *MySQLDb) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	switch strings.ToLower(tblName) {
	case userTblName:
		return t.user, true, nil
	case roleEdgesTblName:
		return t.role_edges, true, nil
	case dbTblName:
		return t.db, true, nil
	case tablesPrivTblName:
		return t.tables_priv, true, nil
	default:
		return nil, false, nil
	}
}

// GetTableNames implements the interface sql.Database.
func (t *MySQLDb) GetTableNames(ctx *sql.Context) ([]string, error) {
	return []string{userTblName, dbTblName, tablesPrivTblName, roleEdgesTblName}, nil
}

// AuthMethod implements the interface mysql.AuthServer.
func (t *MySQLDb) AuthMethod(user string) (string, error) {
	//TODO: this should pass in the host as well to correctly determine which auth method to use
	return "mysql_native_password", nil
}

// Salt implements the interface mysql.AuthServer.
func (t *MySQLDb) Salt() ([]byte, error) {
	return mysql.NewSalt()
}

// ValidateHash implements the interface mysql.AuthServer. This is called when the method used is "mysql_native_password".
func (t *MySQLDb) ValidateHash(salt []byte, user string, authResponse []byte, addr net.Addr) (mysql.Getter, error) {
	if !t.Enabled {
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

	userEntry := t.GetUser(user, host, false)
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
func (t *MySQLDb) Negotiate(c *mysql.Conn, user string, addr net.Addr) (mysql.Getter, error) {
	if !t.Enabled {
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			return nil, err
		}
		return MysqlConnectionUser{User: user, Host: host}, nil
	}
	return nil, fmt.Errorf(`the only user login interface currently supported is "mysql_native_password"`)
}

// Persist passes along all changes to the integrator.
func (t *MySQLDb) Persist(ctx *sql.Context) error {
	// TODO: just persist to both for now

	// Do nothing if both persist functions are nil
	if t.dataPersistFunc == nil && t.privilegePersistFunc == nil {
		return nil
	}

	// Extract all user entries from table, and sort
	userEntries := t.user.data.ToSlice(ctx)
	users := make([]*User, len(userEntries))
	for i, userEntry := range userEntries {
		users[i] = userEntry.(*User)
	}
	sort.Slice(users, func(i, j int) bool {
		if users[i].Host == users[j].Host {
			return users[i].User < users[j].User
		}
		return users[i].Host < users[j].Host
	})

	// Extract all role entries from table, and sort
	roleEntries := t.role_edges.data.ToSlice(ctx)
	roles := make([]*RoleEdge, len(roleEntries))
	for i, roleEntry := range roleEntries {
		roles[i] = roleEntry.(*RoleEdge)
	}
	sort.Slice(roles, func(i, j int) bool {
		if roles[i].FromHost == roles[j].FromHost {
			if roles[i].FromUser == roles[j].FromUser {
				if roles[i].ToHost == roles[j].ToHost {
					return roles[i].ToUser < roles[j].ToUser
				}
				return roles[i].ToHost < roles[j].ToHost
			}
			return roles[i].FromUser < roles[j].FromUser
		}
		return roles[i].FromHost < roles[j].FromHost
	})

	// Persist to privilege file
	var err error
	if t.privilegePersistFunc != nil {
		err = t.privilegePersistFunc(ctx, users, roles)
	}

	// Error from persisting to privilege file
	if err != nil {
		return err
	}

	// Persist to mysql.db file
	if t.dataPersistFunc == nil {
		return nil
	}

	// TODO: Extract all other table entries

	// Convert to json
	data := &MySQLDataJSON{
		Users: users,
		Roles: roles,
	}

	// Persist
	err = t.dataPersistFunc(ctx, data)

	return err
}

// UserTable returns the "user" table.
func (t *MySQLDb) UserTable() *mysqlTable {
	return t.user
}

// RoleEdgesTable returns the "role_edges" table.
func (t *MySQLDb) RoleEdgesTable() *mysqlTable {
	return t.role_edges
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
