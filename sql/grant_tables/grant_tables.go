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

	user *grantTable
	//TODO: add the rest of these tables
	//db               *grantTable
	//global_grants    *grantTable
	//tables_priv      *grantTable
	//columns_priv     *grantTable
	//procs_priv       *grantTable
	//proxies_priv     *grantTable
	//default_roles    *grantTable
	//role_edges       *grantTable
	//password_history *grantTable
}

var _ sql.Database = (*GrantTables)(nil)
var _ mysql.AuthServer = (*GrantTables)(nil)

// CreateEmptyGrantTables returns a collection of Grant Tables that do not contain any data.
func CreateEmptyGrantTables() *GrantTables {
	grantTables := &GrantTables{
		user: newGrantTable(userTblName, userTblSchema, &User{}, UserPrimaryKey{}, UserSecondaryKey{}),
	}
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
			if host == readUserEntry.Host || (host == "127.0.0.1" && readUserEntry.Host == "localhost") || (readUserEntry.Host == "%" && !roleSearch) {
				userEntry = readUserEntry
				break
			}
		}
	}
	return userEntry
}

// Name implements the interface sql.Database.
func (g *GrantTables) Name() string {
	return "mysql"
}

// GetTableInsensitive implements the interface sql.Database.
func (g *GrantTables) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	switch strings.ToLower(tblName) {
	case "user":
		return g.user, true, nil
	default:
		return nil, false, nil
	}
}

// GetTableNames implements the interface sql.Database.
func (g *GrantTables) GetTableNames(ctx *sql.Context) ([]string, error) {
	return []string{"user"}, nil
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
	if userEntry == nil {
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

// UserTable returns the user table.
func (g *GrantTables) UserTable() *grantTable {
	return g.user
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
