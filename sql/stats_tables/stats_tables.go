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

package stats_tables

import (
	"fmt"
	"net"
	"sort"
	"strings"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
)

type StatsTables struct {
	Enabled bool
}

var _ sql.Database = (*StatsTables)(nil)
var _ mysql.AuthServer = (*StatsTables)(nil)

// CreateEmptyStatsTables returns a collection of Statistics Tables that do not contain any data.
func CreateEmptyStatsTables() *StatsTables {
	// original tables
	statsTables := &StatsTables{}

	// TODO: shims

	return statsTables
}

// UserHasPrivileges is always true. Anyone can generate statistics.
// TODO: who is actually allowed to generate stats.
func (g *StatsTables) UserHasPrivileges(ctx *sql.Context, operations ...sql.PrivilegedOperation) bool {
	return true
}

// Name implements the interface sql.Database.
// TODO: making special hidden database; bad idea?
func (g *StatsTables) Name() string {
	return "dolt_statistics"
}

// GetTableInsensitive implements the interface sql.Database.
func (g *StatsTables) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
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
func (g *StatsTables) GetTableNames(ctx *sql.Context) ([]string, error) {
	return []string{userTblName, dbTblName, tablesPrivTblName, roleEdgesTblName}, nil
}

// AuthMethod implements the interface mysql.AuthServer.
func (g *StatsTables) AuthMethod(user string) (string, error) {
	//TODO: this should pass in the host as well to correctly determine which auth method to use
	return "mysql_native_password", nil
}

// Salt implements the interface mysql.AuthServer.
func (g *StatsTables) Salt() ([]byte, error) {
	return mysql.NewSalt()
}

// ValidateHash implements the interface mysql.AuthServer. This is called when the method used is "mysql_native_password".
func (g *StatsTables) ValidateHash(salt []byte, user string, authResponse []byte, addr net.Addr) (mysql.Getter, error) {
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
func (g *StatsTables) Negotiate(c *mysql.Conn, user string, addr net.Addr) (mysql.Getter, error) {
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
func (g *StatsTables) Persist(ctx *sql.Context) error {
	persistFunc := g.persistFunc
	if persistFunc == nil {
		return nil
	}
	userEntries := g.user.data.ToSlice(ctx)
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

	roleEntries := g.role_edges.data.ToSlice(ctx)
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
	return persistFunc(ctx, users, roles)
}

// UserTable returns the "user" table.
func (g *StatsTables) UserTable() *grantTable {
	return g.user
}

// RoleEdgesTable returns the "role_edges" table.
func (g *StatsTables) RoleEdgesTable() *grantTable {
	return g.role_edges
}

type dummyPartition struct{}

var _ sql.Partition = dummyPartition{}

// Key implements the interface sql.Partition.
func (d dummyPartition) Key() []byte {
	return nil
}
