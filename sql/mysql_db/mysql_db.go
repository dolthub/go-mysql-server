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
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"

	"github.com/dolthub/vitess/go/mysql"
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db/serial"
)

// MySQLDbPersistence is used to determine the behavior of how certain tables in MySQLDb will be persisted.
type MySQLDbPersistence interface {
	Persist(ctx *sql.Context, data []byte) error
}

// NoopPersister is used when nothing in mysql db should be persisted
type NoopPersister struct{}

var _ MySQLDbPersistence = &NoopPersister{}

// Persist implements the MySQLDbPersistence interface
func (p *NoopPersister) Persist(ctx *sql.Context, data []byte) error {
	return nil
}

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

	persister MySQLDbPersistence

	cache *privilegeCache
}

var _ sql.Database = (*MySQLDb)(nil)
var _ mysql.AuthServer = (*MySQLDb)(nil)

// CreateEmptyMySQLDb returns a collection of MySQL Tables that do not contain any data.
func CreateEmptyMySQLDb() *MySQLDb {
	// original tables
	mysqlDb := &MySQLDb{}
	mysqlDb.user = newMySQLTable(
		userTblName,
		userTblSchema,
		mysqlDb,
		&User{},
		UserPrimaryKey{},
		UserSecondaryKey{},
	)
	mysqlDb.role_edges = newMySQLTable(
		roleEdgesTblName,
		roleEdgesTblSchema,
		mysqlDb,
		&RoleEdge{},
		RoleEdgesPrimaryKey{},
		RoleEdgesFromKey{},
		RoleEdgesToKey{},
	)

	// mysqlTable shims
	mysqlDb.db = newMySQLTableShim(dbTblName, dbTblSchema, mysqlDb.user, DbConverter{})
	mysqlDb.tables_priv = newMySQLTableShim(tablesPrivTblName, tablesPrivTblSchema, mysqlDb.user, TablesPrivConverter{})
	mysqlDb.cache = newPrivilegeCache()

	return mysqlDb
}

// LoadPrivilegeData adds the given data to the MySQL Tables. It does not remove any current data, but will overwrite any
// pre-existing data.
func (db *MySQLDb) LoadPrivilegeData(ctx *sql.Context, users []*User, roleConnections []*RoleEdge) error {
	db.Enabled = true
	for _, user := range users {
		if user == nil {
			continue
		}
		if err := db.user.data.Put(ctx, user); err != nil {
			return err
		}
	}
	for _, role := range roleConnections {
		if role == nil {
			continue
		}
		if err := db.role_edges.data.Put(ctx, role); err != nil {
			return err
		}
	}

	db.clearCache()

	return nil
}

// LoadData adds the given data to the MySQL Tables. It does not remove any current data, but will overwrite any
// pre-existing data.
func (db *MySQLDb) LoadData(ctx *sql.Context, buf []byte) (err error) {
	// Do nothing if data file doesn't exist or is empty
	if buf == nil || len(buf) == 0 {
		return nil
	}

	type privDataJson struct {
		Users []*User
		Roles []*RoleEdge
	}

	// if it's a json file, read it; will be rewritten as flatbuffer later
	data := &privDataJson{}
	if err := json.Unmarshal(buf, data); err == nil {
		return db.LoadPrivilegeData(ctx, data.Users, data.Roles)
	}

	// Indicate that mysql db exists
	db.Enabled = true

	// Recover from panics
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("ill formatted privileges file")
		}
	}()

	// Deserialize the flatbuffer
	serialMySQLDb := serial.GetRootAsMySQLDb(buf, 0)

	// Fill in user table
	for i := 0; i < serialMySQLDb.UserLength(); i++ {
		serialUser := new(serial.User)
		if !serialMySQLDb.User(serialUser, i) {
			continue
		}
		user := LoadUser(serialUser)
		if err := db.user.data.Put(ctx, user); err != nil {
			return err
		}
	}

	// Fill in Roles table
	for i := 0; i < serialMySQLDb.RoleEdgesLength(); i++ {
		serialRoleEdge := new(serial.RoleEdge)
		if !serialMySQLDb.RoleEdges(serialRoleEdge, i) {
			continue
		}
		role := LoadRoleEdge(serialRoleEdge)
		if err := db.role_edges.data.Put(ctx, role); err != nil {
			return err
		}
	}

	db.clearCache()

	// TODO: fill in other tables when they exist
	return
}

// SetPersister sets the custom persister to be used when the MySQL Db tables have been updated and need to be persisted.
func (db *MySQLDb) SetPersister(persister MySQLDbPersistence) {
	db.persister = persister
}

// AddRootAccount adds the root account to the list of accounts.
func (db *MySQLDb) AddRootAccount() {
	db.Enabled = true
	addSuperUser(db.user, "root", "localhost", "")
	db.clearCache()
}

// AddSuperUser adds the given username and password to the list of accounts. This is a temporary function, which is
// meant to replace the "auth.New..." functions while the remaining functions are added.
func (db *MySQLDb) AddSuperUser(username string, password string) {
	//TODO: remove this function and the called function
	db.Enabled = true
	if len(password) > 0 {
		hash := sha1.New()
		hash.Write([]byte(password))
		s1 := hash.Sum(nil)
		hash.Reset()
		hash.Write(s1)
		s2 := hash.Sum(nil)
		password = "*" + strings.ToUpper(hex.EncodeToString(s2))
	}
	addSuperUser(db.user, username, "localhost", password)
	db.clearCache()
}

// GetUser returns a user matching the given user and host if it exists. Due to the slight difference between users and
// roles, roleSearch changes whether the search matches against user or role rules.
func (db *MySQLDb) GetUser(user string, host string, roleSearch bool) *User {
	//TODO: determine what the localhost is on the machine, then handle the conversion between ip and localhost
	// For now, this just does another check for localhost if the host is 127.0.0.1
	//TODO: match on anonymous users, which have an empty username (different for roles)
	var userEntry *User
	userEntries := db.user.data.Get(UserPrimaryKey{
		Host: host,
		User: user,
	})
	if len(userEntries) == 1 {
		userEntry = userEntries[0].(*User)
	} else {
		userEntries = db.user.data.Get(UserSecondaryKey{
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
func (db *MySQLDb) UserActivePrivilegeSet(ctx *sql.Context) PrivilegeSet {
	client := ctx.Session.Client()
	user := db.GetUser(client.User, client.Address, false)
	if user == nil {
		return NewPrivilegeSet()
	}

	if priv, ok := db.cache.userPrivileges(user); ok {
		return priv
	}

	privSet := user.PrivilegeSet.Copy()
	roleEdgeEntries := db.role_edges.data.Get(RoleEdgesToKey{
		ToHost: user.Host,
		ToUser: user.User,
	})
	//TODO: filter the active roles using the context, rather than using every granted roles
	//TODO: System variable "activate_all_roles_on_login", if set, will set all roles as active upon logging in
	for _, roleEdgeEntry := range roleEdgeEntries {
		roleEdge := roleEdgeEntry.(*RoleEdge)
		role := db.GetUser(roleEdge.FromUser, roleEdge.FromHost, true)
		if role != nil {
			privSet.UnionWith(role.PrivilegeSet)
		}
	}

	// This is technically a race -- two clients could cache at the same time. But this shouldn't matter, as they will
	// eventually get the same data after the cache is cleared on a write, and MySQL doesn't even guarantee immediate
	// effect for grant statements.
	db.cache.cacheUserPrivileges(user, privSet)
	return privSet
}

// UserHasPrivileges fetches the User, and returns whether they have the desired privileges necessary to perform the
// privileged operation. This takes into account the active roles, which are set in the context, therefore the user is
// also pulled from the context.
func (db *MySQLDb) UserHasPrivileges(ctx *sql.Context, operations ...sql.PrivilegedOperation) bool {
	privSet := db.UserActivePrivilegeSet(ctx)
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
func (db *MySQLDb) Name() string {
	return "mysql"
}

// GetTableInsensitive implements the interface sql.Database.
func (db *MySQLDb) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	switch strings.ToLower(tblName) {
	case userTblName:
		return db.user, true, nil
	case roleEdgesTblName:
		return db.role_edges, true, nil
	case dbTblName:
		return db.db, true, nil
	case tablesPrivTblName:
		return db.tables_priv, true, nil
	default:
		return nil, false, nil
	}
}

// GetTableNames implements the interface sql.Database.
func (db *MySQLDb) GetTableNames(ctx *sql.Context) ([]string, error) {
	return []string{
		userTblName,
		dbTblName,
		tablesPrivTblName,
		roleEdgesTblName,
	}, nil
}

// AuthMethod implements the interface mysql.AuthServer.
func (db *MySQLDb) AuthMethod(user string) (string, error) {
	//TODO: this should pass in the host as well to correctly determine which auth method to use
	return "mysql_native_password", nil
}

// Salt implements the interface mysql.AuthServer.
func (db *MySQLDb) Salt() ([]byte, error) {
	return mysql.NewSalt()
}

// ValidateHash implements the interface mysql.AuthServer. This is called when the method used is "mysql_native_password".
func (db *MySQLDb) ValidateHash(salt []byte, user string, authResponse []byte, addr net.Addr) (mysql.Getter, error) {
	var host string
	var err error
	if addr.Network() == "unix" {
		host = "localhost"
	} else {
		host, _, err = net.SplitHostPort(addr.String())
		if err != nil {
			return nil, err
		}
	}

	if !db.Enabled {
		return MysqlConnectionUser{User: user, Host: host}, nil
	}

	userEntry := db.GetUser(user, host, false)
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
func (db *MySQLDb) Negotiate(c *mysql.Conn, user string, addr net.Addr) (mysql.Getter, error) {
	if !db.Enabled {
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			return nil, err
		}
		return MysqlConnectionUser{User: user, Host: host}, nil
	}
	return nil, fmt.Errorf(`the only user login interface currently supported is "mysql_native_password"`)
}

// Persist passes along all changes to the integrator.
func (db *MySQLDb) Persist(ctx *sql.Context) error {
	defer db.clearCache()

	// Extract all user entries from table, and sort
	userEntries := db.user.data.ToSlice(ctx)
	users := make([]*User, 0)
	for _, userEntry := range userEntries {
		user := userEntry.(*User)
		if user.IsSuperUser {
			continue
		}
		users = append(users, user)
	}
	sort.Slice(users, func(i, j int) bool {
		if users[i].Host == users[j].Host {
			return users[i].User < users[j].User
		}
		return users[i].Host < users[j].Host
	})

	// Extract all role entries from table, and sort
	roleEntries := db.role_edges.data.ToSlice(ctx)
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

	// TODO: serialize other tables when the exist

	// Create flatbuffer
	b := flatbuffers.NewBuilder(0)
	user := serializeUser(b, users)
	roleEdge := serializeRoleEdge(b, roles)

	// Write MySQL DB
	serial.MySQLDbStart(b)
	serial.MySQLDbAddUser(b, user)
	serial.MySQLDbAddRoleEdges(b, roleEdge)
	mysqlDbOffset := serial.MySQLDbEnd(b)

	// Finish writing
	b.Finish(mysqlDbOffset)

	// Persist data
	return db.persister.Persist(ctx, b.FinishedBytes())
}

// UserTable returns the "user" table.
func (db *MySQLDb) UserTable() *mysqlTable {
	return db.user
}

// RoleEdgesTable returns the "role_edges" table.
func (db *MySQLDb) RoleEdgesTable() *mysqlTable {
	return db.role_edges
}

func (db *MySQLDb) clearCache() {
	if db == nil { // nil in the case of some tests
		return
	}
	db.cache.clear()
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

type privilegeCache struct {
	mu       sync.Mutex
	userPriv map[string]PrivilegeSet
}

func newPrivilegeCache() *privilegeCache {
	return &privilegeCache{
		userPriv: make(map[string]PrivilegeSet),
	}
}

func userKey(user *User) string {
	return fmt.Sprintf("%s@%s", user.User, user.Host)
}

func (pc *privilegeCache) userPrivileges(user *User) (PrivilegeSet, bool) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	privs, ok := pc.userPriv[userKey(user)]
	return privs, ok
}

// cacheUserPrivileges Caches the user privileges given. Needs external locking.
func (pc *privilegeCache) cacheUserPrivileges(user *User, privs PrivilegeSet) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	pc.userPriv[userKey(user)] = privs
}

func (pc *privilegeCache) clear() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	pc.userPriv = make(map[string]PrivilegeSet)
}
