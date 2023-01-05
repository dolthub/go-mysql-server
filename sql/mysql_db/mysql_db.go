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

type PlaintextAuthPlugin interface {
	Authenticate(db *MySQLDb, user string, userEntry *User, pass string) (bool, error)
}

// MySQLDb are the collection of tables that are in the MySQL database
type MySQLDb struct {
	Enabled bool

	user              *mysqlTable
	role_edges        *mysqlTable
	slave_master_info *mysqlTable

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
	plugins   map[string]PlaintextAuthPlugin

	updateCounter uint64
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
	mysqlDb.slave_master_info = newMySQLTable(
		replicaSourceInfoTblName,
		replicaSourceInfoTblSchema,
		mysqlDb,
		&ReplicaSourceInfo{},
		ReplicaSourceInfoPrimaryKey{},
	)

	// mysqlTable shims
	mysqlDb.db = newMySQLTableShim(dbTblName, dbTblSchema, mysqlDb.user, DbConverter{})
	mysqlDb.tables_priv = newMySQLTableShim(tablesPrivTblName, tablesPrivTblSchema, mysqlDb.user, TablesPrivConverter{})

	// Start the counter at 1, all new sessions will start at zero so this forces an update for any new session
	mysqlDb.updateCounter = 1

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

	db.updateCounter++

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
		Users              []*User
		Roles              []*RoleEdge
		ReplicaSourceInfos []*ReplicaSourceInfo
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

	// Fill in the ReplicaSourceInfo table
	for i := 0; i < serialMySQLDb.ReplicaSourceInfoLength(); i++ {
		serialReplicaSourceInfo := new(serial.ReplicaSourceInfo)
		if !serialMySQLDb.ReplicaSourceInfo(serialReplicaSourceInfo, i) {
			continue
		}
		replicaSourceInfo := LoadReplicaSourceInfo(serialReplicaSourceInfo)
		if err := db.slave_master_info.data.Put(ctx, replicaSourceInfo); err != nil {
			return err
		}
	}

	db.updateCounter++

	// TODO: fill in other tables when they exist
	return
}

// SetPersister sets the custom persister to be used when the MySQL Db tables have been updated and need to be persisted.
func (db *MySQLDb) SetPersister(persister MySQLDbPersistence) {
	db.persister = persister
}

func (db *MySQLDb) SetPlugins(plugins map[string]PlaintextAuthPlugin) {
	db.plugins = plugins
}

func (db *MySQLDb) VerifyPlugin(plugin string) error {
	_, ok := db.plugins[plugin]
	if ok {
		return nil
	}
	return fmt.Errorf(`must provide authentication plugin for unsupported authentication format`)
}

// AddRootAccount adds the root account to the list of accounts.
func (db *MySQLDb) AddRootAccount() {
	db.Enabled = true
	addSuperUser(db.user, "root", "localhost", "")
	db.updateCounter++
}

// AddSuperUser adds the given username and password to the list of accounts. This is a temporary function, which is
// meant to replace the "auth.New..." functions while the remaining functions are added.
func (db *MySQLDb) AddSuperUser(username string, host string, password string) {
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
	addSuperUser(db.user, username, host, password)
	db.updateCounter++
}

// GetUser returns a user matching the given user and host if it exists. Due to the slight difference between users and
// roles, roleSearch changes whether the search matches against user or role rules.
func (db *MySQLDb) GetUser(user string, host string, roleSearch bool) *User {
	//TODO: Determine what the localhost is on the machine, then handle the conversion between IP and localhost.
	// For now, this just treats localhost and 127.0.0.1 as the same.
	//TODO: Determine how to match anonymous roles (roles with an empty user string), which differs from users
	//TODO: Treat '%' as a proper wildcard for hostnames, allowing for regex-like matches.
	// Hostnames representing an IP address that have a wildcard have additional restrictions on what may match
	//TODO: Match non-existent users to the most relevant anonymous user if multiple exist (''@'localhost' vs ''@'%')
	// It appears that ''@'localhost' can use the privileges set on ''@'%', which seems to be unique behavior.
	// For example, 'abc'@'localhost' CANNOT use any privileges set on 'abc'@'%'.
	// Unknown if this is special for ''@'%', or applies to any matching anonymous user.
	//TODO: Hostnames representing IPs can use masks, such as 'abc'@'54.244.85.0/255.255.255.0'
	//TODO: Allow for CIDR notation in hostnames
	//TODO: Which user do we choose when multiple host names match (e.g. host name with most characters matched, etc.)
	userEntries := db.user.data.Get(UserPrimaryKey{
		Host: host,
		User: user,
	})

	if len(userEntries) == 1 {
		return userEntries[0].(*User)
	}

	// First we check for matches on the same user, then we try the anonymous user
	for _, targetUser := range []string{user, ""} {
		userEntries = db.user.data.Get(UserSecondaryKey{
			User: targetUser,
		})
		for _, readUserEntry := range userEntries {
			readUserEntry := readUserEntry.(*User)
			//TODO: use the most specific match first, using "%" only if there isn't a more specific match
			if host == readUserEntry.Host ||
				(host == "127.0.0.1" && readUserEntry.Host == "localhost") ||
				(host == "localhost" && readUserEntry.Host == "127.0.0.1") ||
				(readUserEntry.Host == "%" && (!roleSearch || host == "")) {
				return readUserEntry
			}
		}
	}
	return nil
}

// UserActivePrivilegeSet fetches the User, and returns their entire active privilege set. This takes into account the
// active roles, which are set in the context, therefore the user is also pulled from the context.
func (db *MySQLDb) UserActivePrivilegeSet(ctx *sql.Context) PrivilegeSet {
	if privSet, counter := ctx.Session.GetPrivilegeSet(); db.updateCounter == counter {
		// If the counters are equal, we can guarantee that the privilege set exists and is valid
		return privSet.(PrivilegeSet)
	}

	client := ctx.Session.Client()
	user := db.GetUser(client.User, client.Address, false)
	if user == nil {
		return NewPrivilegeSet()
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

	ctx.Session.SetPrivilegeSet(privSet, db.updateCounter)
	return privSet
}

// UserHasPrivileges fetches the User, and returns whether they have the desired privileges necessary to perform the
// privileged operation. This takes into account the active roles, which are set in the context, therefore the user is
// also pulled from the context.
func (db *MySQLDb) UserHasPrivileges(ctx *sql.Context, operations ...sql.PrivilegedOperation) bool {
	if !db.Enabled {
		return true
	}
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
func (db *MySQLDb) GetTableInsensitive(_ *sql.Context, tblName string) (sql.Table, bool, error) {
	switch strings.ToLower(tblName) {
	case userTblName:
		return db.user, true, nil
	case roleEdgesTblName:
		return db.role_edges, true, nil
	case dbTblName:
		return db.db, true, nil
	case tablesPrivTblName:
		return db.tables_priv, true, nil
	case replicaSourceInfoTblName:
		return db.slave_master_info, true, nil
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
		replicaSourceInfoTblName,
	}, nil
}

// AuthMethod implements the interface mysql.AuthServer.
func (db *MySQLDb) AuthMethod(user, addr string) (string, error) {
	if !db.Enabled {
		return "mysql_native_password", nil
	}
	var host string
	// TODO : need to check for network type instead of addr string if it's unix socket network,
	//  macOS passes empty addr, but ubuntu returns "@" as addr for `localhost`
	if addr == "@" || addr == "" {
		host = "localhost"
	} else {
		splitHost, _, err := net.SplitHostPort(addr)
		if err != nil {
			return "", err
		}
		host = splitHost
	}

	u := db.GetUser(user, host, false)
	if u == nil {
		return "", mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "User not found '%v'", user)
	}
	if _, ok := db.plugins[u.Plugin]; ok {
		return "mysql_clear_password", nil
	}
	return u.Plugin, nil
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

	connUser := MysqlConnectionUser{User: user, Host: host}
	if !db.Enabled {
		return connUser, nil
	}
	userEntry := db.GetUser(user, host, false)

	if userEntry.Plugin != "" {
		authplugin, ok := db.plugins[userEntry.Plugin]
		if !ok {
			return nil, mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%v'; auth plugin %s not registered with server", user, userEntry.Plugin)
		}
		pass, err := mysql.AuthServerReadPacketString(c)
		if err != nil {
			return nil, err
		}
		authed, err := authplugin.Authenticate(db, user, userEntry, pass)
		if err != nil {
			return nil, mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%v': %v", user, err)
		}
		if !authed {
			return nil, mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%v'", user)
		}
		return connUser, nil
	}
	return nil, fmt.Errorf(`the only user login interface currently supported is "mysql_native_password"`)
}

// Persist passes along all changes to the integrator.
func (db *MySQLDb) Persist(ctx *sql.Context) error {
	db.updateCounter++
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

	// Extract all replica source info entries from table, and sort
	replicaSourceInfoEntries := db.slave_master_info.data.ToSlice(ctx)
	replicaSourceInfos := make([]*ReplicaSourceInfo, len(replicaSourceInfoEntries))
	for i, replicaSourceInfoEntry := range replicaSourceInfoEntries {
		replicaSourceInfos[i] = replicaSourceInfoEntry.(*ReplicaSourceInfo)
	}
	sort.Slice(replicaSourceInfos, func(i, j int) bool {
		if replicaSourceInfos[i].Host == replicaSourceInfos[j].Host {
			if replicaSourceInfos[i].Port == replicaSourceInfos[j].Port {
				return replicaSourceInfos[i].User < replicaSourceInfos[j].User
			}
			return replicaSourceInfos[i].Port < replicaSourceInfos[j].Port
		}
		return replicaSourceInfos[i].Host < replicaSourceInfos[j].Host
	})

	// TODO: serialize other tables when they exist

	// Create flatbuffer
	b := flatbuffers.NewBuilder(0)
	user := serializeUser(b, users)
	roleEdge := serializeRoleEdge(b, roles)
	replicaSourceInfo := serializeReplicaSourceInfo(b, replicaSourceInfos)

	// Write MySQL DB
	serial.MySQLDbStart(b)
	serial.MySQLDbAddUser(b, user)
	serial.MySQLDbAddRoleEdges(b, roleEdge)
	serial.MySQLDbAddReplicaSourceInfo(b, replicaSourceInfo)
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

// ReplicaSourceInfoTable returns the "slave_master_info" table.
func (db *MySQLDb) ReplicaSourceInfoTable() *mysqlTable {
	return db.slave_master_info
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
	colDef, err := sql.NewColumnDefaultValue(expr, outType, representsLiteral, !representsLiteral, mayReturnNil)
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
